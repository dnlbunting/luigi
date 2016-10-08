from luigi.file import LocalTarget
import os
import tempfile
import random

class TemporaryTarget(LocalTarget):
    
    def __init__(self, task, temp_id):
        
        path = os.path.join(tempfile.gettempdir(), task.task_id, task.task_id+"."+str(temp_id))
        self.path = path
        self.format = format
        
        