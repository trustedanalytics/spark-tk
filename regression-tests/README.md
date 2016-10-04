# spark-tk regression tests

# Setup and run regression tests out of source code

Prerequisites:
- The maven build tool must be installed and configured - https://maven.apache.org/install.html
- Python 2.7 must be installed - https://www.python.org/downloads/
- The Python packages numpy, pytest, scipy, pandas and statsmodels must be installed - `pip2.7 install numpy scipy pandas statsmodels pytest`
- A Cloudera Distribution of Hadoop (CDH) installation must be set up - http://www.cloudera.com/documentation/cdh/5-1-x/CDH5-Installation-Guide/CDH5-Installation-Guide.html

Note: the following assumes development is being performed on a controller node of the CDH cluster (i.e. access to YARN and HDFS is available)


1. First build and install the source code at the top level of this git repository.

   ```shell
   mvn install -DskipTests -Dlicense.skip=true
   ```
   
2. Set the environment variables necessary for sparktk to execute. These environment variables tell sparktk where to find
   pyspark, and the relevant jar files. The python path must be set to find the sparktk python library, the sparktk regression suite library, CDH's python installation
   and CDH's pyspark installation. It is suggested you add these to your .bashrc file. To set the variables run 
   
   ```
    export SPARKTK_HOME=<PATH TO GIT REPOSITORY>/sparktk-core/target
    export PYTHONPATH=<PATH TO GIT REPOSITORY>/regression-tests:<PATH TO GIT REPOSITORY>/python:/opt/cloudera/parcels/CDH/lib/spark/python/pyspark/:/opt/cloudera/parcels/CDH/lib/spark/python/:$PYTHONPATH
    ```
    
3. Install the datasets into HDFS by running the `install_datasets.sh` script in the `regression-tests/automation folder`. This cleans and creates a folder for regression tests
   to place data, and then puts the datasets the regression suite uses into HDFS at this location. This command requires sudo permissions to HDFS. To run this use the following command in this folder
   
   ```
   ./automation/install_datasets.sh
   ```
   
    NOTE: THIS WILL DELETE ALL EXISTING DATASETS AND RE-ADD THE CONTENTS OF DATASETS FOLDER
4. (Optional) Install the GraphFrames library to use the graph functionality. This is only necessary if you want to run the graph regressions, or leverage the graph functionality.
   To do this you download the GraphFrames library and add the python library to your python path (again, we suggest you extend the python path in your bashrc). To do this
   
   ```
   wget -nv --no-check-certificate http://dl.bintray.com/spark-packages/maven/graphframes/graphframes/0.1.0-spark1.6/graphframes-0.1.0-spark1.6.jar -O graphframes.zip
    unzip graphframes.zip
    export PYTHONPATH=$PWD/graphframes/:$PYTHONPATH
    ```
5. To run the regression tests you can either enter find the tests under the `sparktkregtests/testcases` folders, or you can run the following command to run all regression tests.

   ```
   py.test .
   ```

