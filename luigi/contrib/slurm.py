
import os
import subprocess
import time
import sys
import logging
import random
import traceback
import tempfile

import luigi

logger = logging.getLogger('luigi-interface')

class SlurmMixin(object):
    '''Mixin to support running Task on a SLURM cluster '''
    
    n_cpu = luigi.IntParameter(default=1, significant=False)
    mem = luigi.IntParameter(default=1000, significant=False)
    partition = luigi.Parameter(default='tgac-medium', significant=False)
    job_name_format = luigi.Parameter(
        significant=False, default=None, description="A string that can be "
        "formatted with class variables to name the job with qsub.")
    job_name = luigi.Parameter(
        significant=False, default=None,
        description="Explicit job name given via qsub.")
    run_locally = luigi.BoolParameter(
        significant=False,
        description="run locally instead of on the cluster")
    rm_tmp = luigi.BoolParameter(default=False, significant=False)

    def _init_local(self):
        # Set up temp folder in shared directory (trim to max filename length)
        base_tmp_dir = tempfile.gettempdir()
        random_id = '%016x' % random.getrandbits(64)
        folder_name = self.task_id + '-' + random_id
        self.tmp_dir = os.path.join(base_tmp_dir, folder_name)
        max_filename_length = os.fstatvfs(0).f_namemax
        self.tmp_dir = self.tmp_dir[:max_filename_length]
        logger.info("Tmp dir: %s", self.tmp_dir)
        os.makedirs(self.tmp_dir)
    
    def _srun(self, launch):
        #return ". lmod-6.1; ml gcc/4.9.1 openmpi/1.10.2; salloc -N 1 -n {n_cpu} --mem {mem} -p {partition} -J {job_name}  mpirun  -np {n_cpu} {launch} > {outfile} 2> {errfile}"
        return "salloc -N 1 -n {n_cpu} --mem {mem} -p {partition} -J {job_name} srun -n 1 -c {n_cpu} -o {outfile} -e {errfile} {launch}".format(n_cpu=self.n_cpu,
         mem=self.mem, partition=self.partition, job_name=self.job_name, launch=launch, outfile=self.outfile, errfile=self.errfile )


class SlurmExecutableTask(luigi.Task, SlurmMixin):

    """Base class for executing a job on SLURM

    Override ``work_script()`` to return a script file as a string to run

    Parameters:

    - n_cpu: Number of CPUs to allocate for the Task. 
    - mem: Amount of memory to require MB
    - partition: slurm partition to submit to
    - shared_tmp_dir: Shared drive accessible from all nodes in the cluster.
          Task classes and dependencies are pickled to a temporary folder on
          this drive. The default is ``/tgac/scratch/``, the NFS share location setup
          by StarCluster
    - job_name_format: String that can be passed in to customize the job name
        string passed to qsub; e.g. "Task123_{task_family}_{n_cpu}...".
    - job_name: Exact job name to pass to qsub.
    - run_locally: Run locally instead of on the cluster.

    """

    def __init__(self, *args, **kwargs):
        super(SlurmExecutableTask, self).__init__(*args, **kwargs)
        if self.job_name:
            # use explicitly provided job name
            pass
        elif self.job_name_format:
            # define the job name with the provided format
            self.job_name = self.job_name_format.format(
                task_family=self.task_family, **self.__dict__)
        else:
            # default to the task family
            self.job_name = self.task_family

    def _fetch_task_failures(self):
        ret = ''
        try:
            with open(self.errfile, 'r') as err:
                ret +="\nSLURM err:" + err.read() 
        except FileNotFoundError:
            ret +="\nSLURM err: None"
        try: 
            with open(self.outfile, 'r') as out:
                ret +="\nSLURM out:" + out.read()
        except FileNotFoundError:
            ret +="\nSLURM out: None"
        
        return ret
        
    def run(self):
        self._init_local()            
        self.launcher = os.path.join(self.tmp_dir, "launch.sh")
        
        with open(self.launcher, 'w') as l:
            l.write(self.work_script())
            
        if self.run_locally:
            output = subprocess.check_output(self.launcher , shell=True, stderr=subprocess.PIPE)
        
        else:
            self.outfile = os.path.join(self.tmp_dir, 'job.out')
            self.errfile = os.path.join(self.tmp_dir, 'job.err')
            
            submit_cmd = self._srun(self.launcher) 
            logger.debug("SLURM: " + submit_cmd )
            
            output = subprocess.check_output(submit_cmd, shell=True, stderr=subprocess.PIPE)
            
            if (self.tmp_dir and os.path.exists(self.tmp_dir) and self.rm_tmp):
                logger.info('Removing temporary directory %s' % self.tmp_dir)
                subprocess.call(["rm", "-rf", self.tmp_dir])
                
    def on_failure(self, exception):
        slurm_err = self._fetch_task_failures()
        logger.info(slurm_err)
        super_retval = super().on_failure(exception)
        if super_retval is not None:
            return slurm_err + "\n" + super_retval
        else:
            return slurm_err

    def on_success(self):
        slurm_err = self._fetch_task_failures()
        logger.info(slurm_err)
        super_retval = super().on_success()
        if super_retval is not None:
            return slurm_err + "\n" + super_retval
        else:
            return slurm_err
    def work_script(self):
        """Override this an make it return the shell script to run"""
        pass
