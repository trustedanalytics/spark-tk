
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
