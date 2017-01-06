#!/bin/bash
#
#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#


#Script to upload sparktk-core and regression tests packages to jupyter instance

JUPYTER_SERVICE_INSTANCE=sparktk-qa-test
JUPYTER_PLAN=free

dashboard=$(cf service $JUPYTER_SERVICE_INSTANCE | grep Dashboard|sed -e 's/.*:\/\///g')
JUPYTER_NOTEBOOK_URL=$JUPYTER_SERVICE_INSTANCE-$(awk -F/ -v delim="." '{print $NF""substr($1, index($0, delim))}' <<< $dashboard)

test_driver_path="sparktk_test/regression-tests/automation"
curl http://$JUPYTER_NOTEBOOK_URL/spark-submit -d "driver-path=$test_driver_path/test_driver.py"


sleep 2
report=$(curl http://$JUPYTER_NOTEBOOK_URL/status -d "app-path=$test_driver_path")
status=$(awk -F, -v delim=" " '{print substr($1, index($1, delim)) }' <<< $report|tr -d "\"")

echo $status, "completed"

while [ $status != "completed" ]
do
    echo "Current Status:$status "
    sleep 3
    report=$(curl http://$JUPYTER_NOTEBOOK_URL/status -d "app-path=$test_driver_path")
    status=$(awk -F, -v delim=" " '{print substr($1, index($1, delim)) }' <<< $report|tr -d "\"")
done

curl http://$JUPYTER_NOTEBOOK_URL/logs -d "app-path=$test_driver_path" -d "offset=1" -d "n=1000" >> SPARKTK_QA_TEST.log

cat SPARKTK_QA_TEST.log

#delete old instance 
#cf delete-service -f $JUPYTER_SERVICE_INSTANCE

