import os
import subprocess as sp


#install packages
dir_path=os.path.dirname(os.path.realpath(__file__))
os.chdir(dir_path)
install_process = sp.Popen(["/bin/bash", "/home/vcap/jupyter/sparktk_test/regression-tests/automation/install_source_jupyter.sh"], stdout=sp.PIPE, stderr=sp.PIPE)
install_out, install_err = install_process.communicate()
print install_out, install_err

