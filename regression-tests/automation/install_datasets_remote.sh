#!/bin/sh
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


tar -cvzf files.tar.gz /qa_data/*
CDH_MASTER="atk-qa-nokrb-manager"
echo $CDH_MASTER
FILE_LOCATION="/user/vcap"
scp files.tar.gz $CDH_MASTER:files.tar.gz
ssh -q -tt $CDH_MASTER "tar -xzf files.tar.gz"
ssh -q -tt $CDH_MASTER "chmod a+rx qa_data"
ssh -q -tt $CDH_MASTER "chmod a+rx ."
ssh -q -tt $CDH_MASTER "cd qa_data; chmod a+r *;"
ssh -q -tt $CDH_MASTER "sudo -u hdfs hdfs dfs -rm -r -skipTrash $FILE_LOCATION/qa_data"
ssh -q -tt $CDH_MASTER "sudo -u hdfs hdfs dfs -mkdir $FILE_LOCATION/qa_data"
ssh -q -tt $CDH_MASTER "sudo -u hdfs hdfs dfs -chown -R vcap:supergroup $FILE_LOCATION"
ssh -q -tt $CDH_MASTER "sudo -u hdfs hdfs dfs -put -p -f qa_data/* $FILE_LOCATION/qa_data/"

