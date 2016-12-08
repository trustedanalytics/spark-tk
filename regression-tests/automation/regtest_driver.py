import subprocess as sp                                                                                                                         
unpack_process = sp.Popen(["/bin/bash", "/home/vcap/jupyter/sparktk_test/install_reg_test.sh"], stdout=sp.PIPE, stderr=sp.PIPE)
out, err = unpack_process.communicate()
print out, err
print "Unpacked regression-tests package"
