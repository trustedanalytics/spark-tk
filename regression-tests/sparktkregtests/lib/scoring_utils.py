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

    def __init__(self, model_path, port_id, host=config.scoring_engine_host,
                 pipeline=False, pipeline_filename=None):
        """Set up the server location, port and model file"""
        self.hdfs_path = model_path
        self.name = host.split('.')[0]
        self.host = host
        self.pipeline = pipeline
        self.pipeline_filename = pipeline_filename
        port_config = SafeConfigParser()
        filepath = os.path.abspath(os.path.join(
            config.root, "regression-tests",
            "sparktkregtests", "lib", "port.ini"))
        port_config.read(filepath)
        self.port = port_config.get(
            'port', ('.'.join(port_id.split('.')[-2:])))

    def __enter__(self):
        """Activate the Server"""
        if self.pipeline:
            self.start_pipeline_server(self.pipeline_filename)
            time.sleep(30)
        # change current working directory to point at scoring_engine dir
        run_path = os.path.abspath(
            os.path.join(config.root, "scoring", "scoring_engine"))

        # keep track of cwd for future
        test_dir = os.getcwd()
        os.chdir(run_path)

        path = self.hdfs_path
        # make a new process group
        self.scoring_process = sp.Popen(
            ["./bin/model-scoring.sh",
             "-Dtrustedanalytics.scoring-engine.archive-mar=%s" % path,
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
        pgrp_engine = os.getpgid(self.scoring_process.pid)
        os.killpg(pgrp_engine, signal.SIGKILL)
        if self.pipeline:
            pgrp_pipeline = os.getpgid(self.pipeline_process.pid)
            os.killpg(pgrp_pipeline, signal.SIGKILL)

    def score(self, data_val):
        """score the json set data_val"""

        # Magic headers to make the server respond appropriately
        # Ask the head of scoring why these
        headers = {'Content-type': 'application/json',
                   'Accept': 'application/json,text/plain'}

        scoring_host = self.host + ":" + self.port
        if self.pipeline:
            submit_string = 'http://localhost:45000/v1/score'
            munged_data = ','.join(map(str, data_val[0].values()))
            response = requests.post(
                submit_string, json={"message": munged_data}, headers=headers)
        else:
            submit_string = 'http://'+scoring_host+'/v2/score'
            response = requests.post(
                submit_string, json={"records": data_val}, headers=headers)

        return response

    def revise(self, model_path):
        """revises the scoring engine model"""
        scoring_host = self.host + ":" + self.port
        submit_string = 'http://'+scoring_host+'/v2/revise'
        response = requests.post(
            submit_string, json={"model-path": model_path, "force": "true"})
        return response

    def start_pipeline_server(self, pipeline_filename):
        # host = "0.0.0.0"
        port = "45000"
        scoring_pipeline_location = os.path.join(
            config.root, "scoring_pipelines",
            "scoring_pipelines", "scoringExecutor.py")
        self.pipeline_process = sp.Popen(
            ["python2.7", scoring_pipeline_location, pipeline_filename, port],
            preexec_fn=os.setsid)
