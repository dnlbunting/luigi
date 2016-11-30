
import luigi
from luigi import Target, LocalTarget
from luigi.util import task_wraps, inherits, requires
from luigi.task import getpaths
import six,os,math
import copy

def indextarget(struct, idx):
    """
    Maps all Targets in a structured output to an indexed temporary file
    """
    if isinstance(struct, Target):
        base, *ext = struct.path.split('.', maxsplit=1)
        if len(ext) > 0:
            return LocalTarget(base + "_" + str(idx) + "." + ext[0])
        else :
            return LocalTarget(base + "_" + str(idx))
    else:
        raise NotImplemented()

class ScatterGather():
    '''Decorator to transparently add Scatter-Gather parallelism to a Luigi task
    :param scatterTask must inherit and implement a run() method which maps
           a single input() file to an array of output() files
    :param scatterTask must inherit and implement a run() method which maps
           an array of input() files to a single output() file
    :param N the number of parts to scatter into
    
    Example
    =======
    
    class scatter(luigi.Task):
        def run(self):
            with self.input().open() as fin:
                inp = fin.readlines()
            perfile = math.ceil(len(inp)/len(self.output()))        
            for i,out in enumerate(self.output()):
                with out.open('w') as fout:
                    fout.writelines(inp[i*perfile:(i+1)*perfile])
                

    class gather(luigi.Task):
        def run(self):
            with self.output().open('w') as fout:
                for i in self.input():
                    with i.open('r') as fin:
                        fout.write(fin.read())
                    
                    
    @ScatterGather(scatter, gather, 10) 
    class ToBeScattered(luigi.Task):
        def run(self):
            with self.input().open('r') as fin:
                with self.output().open('w') as fout:
                    for l in fin:
                        fout.write("Done! " + l)
    
    '''
    def __init__(self, scatterTask, gatherTask, N):
        self.scatterTask = scatterTask
        self.gatherTask = gatherTask
        self.N = N
        

    def metaProgScatter(self, scattertask):
        Scatter = type(scattertask.__name__, (scattertask,), {})
        
        Scatter = inherits(self.workTask)(Scatter)
        Scatter.requires = self.workTask.requires
        Scatter.output = lambda cls_self : [indextarget(self.workTask.input(cls_self), i) for i in range(self.N)]
        Scatter.__hash__ = lambda cls_self : hash(cls_self.task_id+str(cls_self.input())+str(cls_self.output()))
        return Scatter
        
    def metaProgWork(self, worktask):
        Work = type(worktask.__name__, (worktask,), {})
        
        Work.SG_index = luigi.IntParameter()
        Work.requires = lambda cls_self : cls_self.clone(self.Scatter)
        Work.input = lambda cls_self : self.workTask.input(cls_self)[cls_self.SG_index]
        Work.output = lambda cls_self : indextarget(self.workTask.output(cls_self), cls_self.SG_index)
        return Work

    def metaProgGather(self, gathertask):
        Gather = type(gathertask.__name__, (gathertask,), {})
        
        Gather = inherits(self.workTask)(Gather)
        Gather.SG_index = None
        Gather.requires = lambda cls_self : [cls_self.clone(self.Work, SG_index=i) for i  in range(self.N)]
        Gather.output = self.workTask.output
        Gather.__hash__ = lambda cls_self : hash(cls_self.task_id+str(cls_self.input())+str(cls_self.output())) y
        
        return Gather
        
    def __call__(self, workTask):
        
        self.workTask = workTask
        
        self.Scatter = self.metaProgScatter(self.scatterTask)
        self.Work = self.metaProgWork(self.workTask)
        self.Gather = self.metaProgGather(self.gatherTask)
        
        return self.Gather
        
