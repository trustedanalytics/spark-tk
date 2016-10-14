""" Library to support scoring in TAP for the ATK service """
import subprocess as sp
import requests
import time
import os
import config

class scorer(object):

    def __init__(self, model_path, host=config.scoring_engine_host):
        """Set up the server location, port and model file"""

        self.hdfs_path = model_path
        self.name = host.split('.')[0]
        self.host = host
        self.scoring_process = None

    def __enter__(self):
        """Activate the Server"""
        run_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "run_scoring_model.sh")
        self.scoring_process = sp.Popen([run_path, self.hdfs_path])

        # wait for server to start
        time.sleep(10)
        return self

    def __exit__(self, *args):
        """Teardown the server"""
        self.scoring_process.kill()

    def score(self, data_val):
        """score the json set data_val"""

        # Magic headers to make the server respond appropriately
        # Ask the head of scoring why these
        headers = {'Content-type': 'application/json',
                   'Accept': 'application/json,text/plain'}

        scoring_host = self.host
        submit_string = 'http://'+scoring_host+'/v2/score'
        response = requests.post(submit_string, json={"records":data_val}, headers=headers)
        return response
