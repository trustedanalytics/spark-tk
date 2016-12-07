import os
import subprocess as sp
import sys

#set sparktk home 
print "Running tests..."

run_process = sp.Popen(["/bin/bash", "/home/vcap/jupyter/sparktk_test/regression-tests/automation/run_tests_jupyter.sh"], stdout=sp.PIPE, stderr=sp.PIPE)

stdout, stderr = run_process.communicate()

print "STDOUT:\n", stdout
print "STDERR:\n", stderr
print "END OF TESTS"

