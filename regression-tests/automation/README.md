

# Automation for regression suite

There are 6 files for the regression suite of the spark-tk libraries.

They are:

- install_source.sh - installs the spark-tk libraries along with the scoring engine, graph frames and any other software needed
- install_qa.sh - installs anything needed specifically by the QA suite, or QA tools
- install_datasets.sh - installs the datasets the regression suite runs on
- run_tests.sh - runs the regression suite
- cleanup.sh - deletes any files created by the regression suite, reaps any results
- run_coverage.sh - runs the test coverage. Currently needed because of failings in the automation suite, may be removed in the future

The tests use the `requirements.txt` files to list any required python code.

# To Run

Move the scoring engine package and the spark-tk package to the top level of this
repository. Run the scripts in order (replacing `run_tests.sh` with `run_coverge.sh` if you
wish to see the code coverage).

Code coverage is in the `pytest` folder if run (an HTML report will be generated)

