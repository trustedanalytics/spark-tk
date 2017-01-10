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

""" Library to support scoring in TAP for the ATK service """
import subprocess as sp
import requests
import time
import signal
import os
import config
from ConfigParser import SafeConfigParser


class scorer(object):

    def __init__(self, model_path, port_id, host=config.scoring_engine_host):
        """Set up the server location, port and model file"""
        self.hdfs_path = model_path
        self.name = host.split('.')[0]
        self.host = host
        # set port
        port_config = SafeConfigParser()
        filepath = os.path.abspath(os.path.join(
            config.root, "regression-tests", "sparktkregtests", "lib", "port.ini"))
        port_config.read(filepath)
        self.port = port_config.get('port', port_id)
        self.scoring_process = None

    def __enter__(self):
        """Activate the Server"""
        # change current working directory to point at scoring_engine dir
        run_path = os.path.abspath(os.path.join(config.root, "scoring", "scoring_engine"))

        # keep track of cwd for future
        test_dir = os.getcwd()
        os.chdir(run_path)

        # make a new process group
        self.scoring_process = sp.Popen(
            ["./bin/model-scoring.sh", "-Dtrustedanalytics.scoring-engine.archive-mar=%s" % self.hdfs_path,
             "-Dtrustedanalytics.scoring.port=%s" % self.port],
            preexec_fn=os.setsid)

        # restore cwd
        os.chdir(test_dir)

        # wait for server to start
        time.sleep(20)
        return self

    def __exit__(self, *args):
        """Teardown the server"""
        # Get the process group to kill all of the suprocesses
        pgrp = os.getpgid(self.scoring_process.pid)
        os.killpg(pgrp, signal.SIGKILL)

    def score(self, data_val):
        """score the json set data_val"""

        # Magic headers to make the server respond appropriately
        # Ask the head of scoring why these
        headers = {'Content-type': 'application/json',
                   'Accept': 'application/json,text/plain'}

        scoring_host = self.host + ":" + self.port
        submit_string = 'http://'+scoring_host+'/v2/score'
        response = requests.post(
            submit_string, json={"records": data_val}, headers=headers)
        return response

    def revise(self, model_path):
        """revises the scoring engine model"""
        scoring_host = self.host + ":" + self.port
        submit_string = 'http://'+scoring_host+'/v2/revise'
        response = requests.post(
            submit_string, json={"model-path":model_path, "force":"true"})
        return response
