# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#


import datetime
import time

class Timer(object):
    """ Profiles a section of code using an execution context (with statement).
    If teamcity is installed will return a test with a specified name.
    """

    def __init__(self, name=None, tc_report=True):
        """ Set up name for the profile run, important for teamcity"""
        # For our purposes flowid and name are always the smae
        self.flowid = name
        self.name = name
        self.tc_report = tc_report

    def __enter__(self):
        """ Begin profiling, return the context object for further analysis"""
        self.start = datetime.datetime.now()
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        """ End profiling, print run time, if teamcity is installed return a test """
        self.end = datetime.datetime.now()
        self.end_time = time.time()

        self.run_time = time.strftime('%H:%M:%S', time.gmtime(self.end_time - self.start_time))

        self.diff = self.end - self.start
        self.seconds = self.end_time - self.start_time

        print self.run_time
        # If teamcity messages is installed, use it
        try:
            import teamcity.messages as tc
            if self.name is not None and self.flowid is not None and self.tc_report:
                tsvc = tc.TeamcityServiceMessages()
                # If there's a better way of doing this I don't know it
                with tsvc.test(self.name, testDuration=self.diff, flowId=self.flowid):
                    pass

        except ImportError, e:
            if e.message != "No module named teamcity.messages":
                raise


