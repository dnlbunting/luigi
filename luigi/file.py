# -*- coding: utf-8 -*-
#
# Copyright 2012-2016 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
luigi.file has moved to :py:mod:`luigi.local_target`
"""
# Delete this file any time after 7 Feb 2017
import tempfile
import warnings
from luigi.local_target import *  # NOQA
warnings.warn("luigi.file module has been moved to luigi.local_target",
              DeprecationWarning)

class TemporaryFile(LocalTarget):
    '''Anonymous temporary file that is accessible from within a single Task. After the Task ends the file is deleted.'''
    def __init__(self):
        path = os.path.join(tempfile.gettempdir(), 'luigi-tmp-%09d' % random.randint(0, 999999999))
        self.path = path
        super().__init__(path)

    def __del__(self):
        if self.exists():
            self.remove()
