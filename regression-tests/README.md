# spark-tk regression tests


# Setup and run regression tests out of source code

To run the regression tests you need to perform the following tasks. This
assumes you wish to use the source code and not a package such as pip.

In order to make the establishment of the correct value for the environment
variable SPARKTK\_HOME a shell file in utils has been provided named home.bash.
Simply source this file in order to set up the correct environment variable,
in bash this would be `. $PWD/utils/home.sh`. This can be added to 
your runcom or rc file for your shell, for bash this would be the .bashrc
file. If you add this to your rc file, the line will be
`. <path to spark-tk repo>/regression-tests/utils/home.bash` rather than
`$PWD`

1. Follow the instructions in the parent folders README to setup spark-tk
2. Install the datasets. To do this run `./utils/install_datasets.sh`
3. Enter the testcases folder with `cd testcases`
4. To run all tests run the command `nosetests .`
5. To run an individual test suite or file, enter the directory with this file
and run `python2.7 <filename>`, for example
`cd models;python2.7 confusion_matrix_test.py`
6. To run an individual test enter the relevant directory with the file that
contains the testcase, and run
`python2.7 -m unittest <filename>.<classname>.<testcasename>`, for example
`cd models;python2.7 -m unittest confusion_matrix_test.ConfusionMatrix.test_confusion_matrix`


