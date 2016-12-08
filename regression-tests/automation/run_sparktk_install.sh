#!/bin/bash

#Script to upload sparktk-core and regression tests packages to jupyter instance

JUPYTER_SERVICE_INSTANCE=sparktk-qa-test
JUPYTER_PLAN=free

dashboard=$(cf service $JUPYTER_SERVICE_INSTANCE | grep Dashboard|sed -e 's/.*:\/\///g')
JUPYTER_NOTEBOOK_URL=$JUPYTER_SERVICE_INSTANCE-$(awk -F/ -v delim="." '{print $NF""substr($1, index($0, delim))}' <<< $dashboard)

install_driver_path="sparktk_test/regression-tests/automation"
curl http://$JUPYTER_NOTEBOOK_URL/spark-submit -d "driver-path=$install_driver_path/install_driver.py"


sleep 2
report=$(curl http://$JUPYTER_NOTEBOOK_URL/status -d "app-path=$install_driver_path")
status=$(awk -F, -v delim=" " '{print substr($1, index($1, delim)) }' <<< $report|tr -d "\"")

while [ $status != "completed" ]
do
    echo " Installtion Status: $status"
    sleep 2
    report=$(curl http://$JUPYTER_NOTEBOOK_URL/status -d "app-path=$install_driver_path")
    status=$(awk -F, -v delim=" " '{print substr($1, index($1, delim)) }' <<< $report|tr -d "\"")
done

curl http://$JUPYTER_NOTEBOOK_URL/logs -d "app-path=$install_driver_path" -d "offset=1" -d "n=100" >> INSTALL.log

#delete old instance 
#cf delete-service -f $JUPYTER_SERVICE_INSTANCE

