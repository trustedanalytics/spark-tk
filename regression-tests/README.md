# spark-tk regression tests


# Setup and run regression tests out of source code

1. First build the source code, this requires maven to be installed. Run
   `mvn install` at the top level of this repo.
2. Run `./run_regression_tests.sh` in this folder
NOTE: THIS WILL DELETE ALL EXISTING DATASETS AND RE-ADD THE CONTENTS OF DATASETS


# Developers
NOTE: THIS WILL DELETE ALL EXISTING DATASETS AND RE-ADD THE CONTENTS OF DATASETS

There are two environment variables that need to be set; `SPARKTK_HOME` and
`PYTHONPATH`. It is recommended to set them in your shell rc file (.bashrc for
most users).

`SPARKTK_HOME` needs to be set to `$PWD/../core/target` from this directory
`PYTHONPATH` needs to be set to `$PWD/sparktkregtests` from this directory
# Developers

There are two environment variables that need to be set; `SPARKTK_HOME` and
`PYTHONPATH`. It is recommended to set them in your shell rc file (.bashrc for
most users).

You also need to download the latest graphframes library, and add it to your `PYTHONPATH`

`SPARKTK_HOME` needs to be set to `<PATH TO SPARK-TK>/sparktk-core/target`

`PYTHONPATH` needs to be set with both pyspark and the spark-tk regression suite libraries
to `<PATH TO SPARK-TK>/regression-tests:/opt/cloudera/parcels/CDH/lib/spark/python/:<path to graphframes>:PYTHONPATH`

In addition you need to make sure your datasets are up to date, to do this you
run the `install_datasets.sh` file out of the automation folder.
NOTE: THIS WILL DELETE ALL EXISTING DATASETS AND RE-ADD THE CONTENTS OF DATASETS
