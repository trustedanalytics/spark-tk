# vim: set encoding=utf-8

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


"""Different configs for the different scales of tests"""
import config

sparktkconf_dict = {'spark.driver.maxPermSize': '512m',
                    'spark.ui.enabled': 'false',
                    'spark.driver.maxResultSize': '1g',
                    'spark.driver.memory': '2g',
                    'spark.dynamicAllocation.enabled': 'true',
                    'spark.dynamicAllocation.maxExecutors': '16',
                    'spark.dynamicAllocation.minExecutors': '1',
                    'spark.executor.cores': '2',
                    'spark.executor.memory': '2g',
                    'spark.shuffle.io.preferDirectBufs': 'true',
                    'spark.shuffle.service.enabled': 'true',
                    'spark.yarn.am.waitTime': '1000000',
                    'spark.yarn.executor.memoryOverhead': '384',
                    'spark.eventLog.enabled': 'false',
                    'spark.sql.shuffle.partitions': '16'}


def get_spark_conf():
    if config.test_size == "test":
        return sparktkconf_dict
    elif config.test_size == "performance":
        return sparktkconf_dict
    else:
        return sparktkconf_dict
