#!/bin/sh

#Script to upload sparktk-core and regression tests packages to jupyter instance

JUPYTER_SERVICE_INSTANCE=sparktk-qa-test
JUPYTER_PLAN=free
sparkcorepackage=$(find `pwd` -name "sparktk-core*.zip")
regtestpackage=$(find `pwd` -name "sparktk-*.tar.gz")
regtestscript=$(find `pwd` -name "install_reg_test.sh")
regtestdriver=$(find `pwd` -name "regtest_driver.py")

echo $sparkcorepackage
#create jupyter instance
cf create-service jupyter $JUPYTER_PLAN $JUPYTER_SERVICE_INSTANCE

dashboard=$(cf service $JUPYTER_SERVICE_INSTANCE | grep Dashboard|sed -e 's/.*:\/\///g')
JUPYTER_NOTEBOOK_URL=$JUPYTER_SERVICE_INSTANCE-$(awk -F/ -v delim="." '{print $NF""substr($1, index($0, delim))}' <<< $dashboard)

#upload pacakges to Jupyter instance
out=$(curl http://$JUPYTER_NOTEBOOK_URL/upload -F "filearg=@$sparkcorepackage" -F "filearg=@$regtestpackage" -F "filearg=@$regtestscript" -F "filearg=@$regtestdriver")
uploads_folder=$(echo $out |sed 's/.*\(uploads\/[0-9]*\).*/\1/')
curl http://$JUPYTER_NOTEBOOK_URL/rename -d "app-path=$uploads_folder" -d "dst-path=sparktk_test"

#unpack regression tests package
test_driver_path="sparktk_test"
curl http://$JUPYTER_NOTEBOOK_URL/spark-submit -d "driver-path=$test_driver_path/regtest_driver.py"


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

curl http://$JUPYTER_NOTEBOOK_URL/logs -d "app-path=$test_driver_path" -d "offset=1" -d "n=100" >> SPARKTK_REG_INSTALL.log

cat SPARKTK_REG_INSTALL.log



#delete old instance 
#cf delete-service -f $JUPYTER_SERVICE_INSTANCE

