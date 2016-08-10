# spark-tk regression tests


# Setup and run regression tests out of source code

1. First build the source code, this requires maven to be installed. Run
   `mvn install` at the top level of this repo.
2. Run `./run_tests.sh` in this folder


# Developers

There are two environment variables that need to be set; `SPARKTK_HOME` and
`PYTHONPATH`. I would recommend setting them in your shell rc file (.bashrc for
most users).

`SPARKTK_HOME` needs to be set to `$PWD/../core/target` from this directory
`PYTHONPATH` needs to be set to `$PWD/sparktkregtests` from this directory
