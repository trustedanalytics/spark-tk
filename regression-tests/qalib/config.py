##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
""" Global Config file for testcases, used heavily by automation"""
import os

import trustedanalytics as ta


# global configuration information for the test suites

root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
fs_root = os.getenv("ATK_FS_ROOT", "/user/atkuser/")
export_location = os.getenv("ATK_EXPORT", ".")
hdfs_namenode = os.getenv("CDH_MASTER", "localhost")
atk_server_uri = os.getenv("ATK_SERVER_URI", ta.server.uri)
atk_server_port = os.getenv("ATK_SERVER_PORT", ta.server.port)
data_location = os.getenv("ATK_DATA_HOME", "qa_data")
keep_results = os.getenv("KEEP_ATK_RESULTS", 0)
put_files = os.getenv("ENABLE_HDFS_PUT", 0)
performance_location = os.getenv("ATK_PERFORMANCE_DATA_HOME", "performance")
qa_suffix = os.getenv("QA_NAMESPACE_SUFFIX", "qaowned")
credentials_file = os.getenv("ATK_CREDENTIALS", "")
gc_timeout_secs = os.getenv("GC_TIMEOUT", "0")
perf_size = os.getenv("ATK_PERFORMANCE_SIZE", "small")
frame_profile = False if os.getenv("ATK_FRAME_PROFILE", "0") == "0" else True
performance_config = os.getenv(
    "ATK_PERFORMANCE_CONFIG", root+"/resources/performance_config.ini")
coverage = False if os.getenv("ATK_API_COVERAGE", "0") == "0" else True
on_tap = False if os.getenv("ATK_TAP", "0") == "0" else True
atk_logger = False if os.getenv("ATK_LOGGER", "0") == "0" else True
scoring_engine_host = os.getenv("ATK_SCORING_ENGINE_HOST", "127.0.0.1:9100")
