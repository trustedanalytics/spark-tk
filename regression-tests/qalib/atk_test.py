##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014, 2015 Intel Corporation All Rights Reserved.
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


"""
ATK specific test functionality.

Abstracts across all test cases
Handles connect, building hardened prefixes, cleanup in the general case
"""

import httplib
import trustedanalytics as ia
import unittest

import common_utils
import config


class ATKTestCase(unittest.TestCase):
    _multiprocess_shared_ = False

    @classmethod
    def setUpClass(cls):
        """Connect to the trustedanalytics server."""
        cls.class_prefix = cls.__name__
        if config.coverage:
            ia.loggers.set_api()

        if config.atk_logger:
            ia.loggers.set_api()
            httplib.HTTPConnection.debuglevel = 1

        if not ia.api_status.is_installed:
            ia.server.uri = config.atk_server_uri

            try:
                ia.server.ping()
            except IOError:
                print "Failed to ping server"
                raise

            try:
                ia.connect(config.credentials_file)
            except:
                print "connect failed"
                raise

            if not ia.api_status.is_installed:
                raise RuntimeError("Failed to install API (Connect)")

    def setUp(self):
        """Create a unique prefix for this test."""
        # Grab the class and test name
        self.prefix = "_".join(self.id().split('.')[-2:])+"__"

    def tearDown(self):
        """Drop everything built with this system.

           If the environment variable KEEP_ATK_RESULTS is set to
           non 0 nothing will be cleaned up.
        """
        # if we are to teardown the results, do so
        if config.keep_results == 0:
            common_utils.drop_all_prefix(self.prefix)

    @classmethod
    def tearDownClass(cls):
        """Disconnect from the trustedanalytics server."""
        # TODO: remove this block once ATK is able to consistently Start and 
        #  Release YARN Job Contexts
        try:
            # ia.release()
            pass
        except Exception, err:
            print "Failed to release YARN Job Context: %s" % err
        # finally:
        #     pass
