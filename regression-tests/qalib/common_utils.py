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
    Utilities for generic qa manipulation
"""

import datetime
import time
import trustedanalytics as ia
import uuid

import config


def datestamp():
    """
    This function generates a "_" separated string of current timestamp
    :return: An "_" separated string of timestamp.
    """
    return datetime.datetime.now().strftime("%m_%d_%H_%M_")


def get_a_name(prefix, harden=True):
    """
    This function generates a name to be used for naming graphs or frames.
    Appends the current timestamp to the prefix provided to the function.
    :param prefix: The string from which graph/frame name should start.
    :return: Name of graph/frame generated using the prefix.
    """
    if harden:
        name = prefix + datestamp() + uuid.uuid1().hex + config.qa_suffix
    else:
        name = prefix + datestamp()

    # ATK presently doesn't allow names over 127 characters in length
    if len(name) > 127:
        print "Warning: Name Length is over 127"

    print "The name generated is ", name
    return name


class Timer(object):
    """ Profiles a section of code using an execution context (with statement).
        If teamcity is installed will return a test with a specified name.
    """

    def __init__(self, name=None, tc_report=True):
        """ Set up name for the profile run, important for teamcity"""
        # For our purposes flowid and name are always the smae
        self.flowid = name
        self.name = name
        self.tc_report = tc_report

    def __enter__(self):
        """ Begin profiling, return the context object for further analysis"""
        self.start = datetime.datetime.now()
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        """ End profiling, print run time, if teamcity is installed return a
            test
        """
        self.end = datetime.datetime.now()
        self.end_time = time.time()
        self.run_time = time.strftime(
            '%H:%M:%S', time.gmtime(self.end_time - self.start_time))
        self.diff = self.end - self.start
        self.seconds = self.end_time - self.start_time
        print self.run_time
        # If teamcity messages is installed, use it
        try:
            import teamcity.messages as tc
            if self.name is not None and self.flowid is not None and self.tc_report:
                tsvc = tc.TeamcityServiceMessages()
                # If there's a better way of doing this I don't know it
                with tsvc.test(self.name,
                               testDuration=self.diff, flowId=self.flowid):
                    pass

        except ImportError, e:
            if e.message != "No module named teamcity.messages":
                raise

# NTS: whenever a testcase is saving data, create a folder and remember to delete the folder here for cleanup

#def drop_all_prefix(prefix):
    """Drop all frames, models and graphs with a given prefix"""
    #ia.drop_models(
    #    [obj for obj in ia.get_model_names()if obj.startswith(prefix)])
    #ia.drop_graphs(
    #    [obj for obj in ia.get_graph_names() if obj.startswith(prefix)])
    #ia.drop_frames(
    #    [obj for obj in ia.get_frame_names() if obj.startswith(prefix)])


#def drop_all_suffix(suffix):
    """Drop all frames_models and graphs with a given suffix"""
    #ia.drop_models(
    #    [obj for obj in ia.get_model_names()if obj.endswith(suffix)])
    #ia.drop_graphs(
    #    [obj for obj in ia.get_graph_names() if obj.endswith(suffix)])
    #ia.drop_frames(
    #    [obj for obj in ia.get_frame_names() if obj.endswith(suffix)])


#def flatten(tree):
#    """
#    Remove all the list and tuple nesting from a sequence.  For instance:
#    ["Start", [1, 2], [2, 7], [3, 9],
#    {"A":1, "B":2}, [5, 7, [["Mid", 8], 6]], "End"]
#        is returned as
#    ['Start', 1, 2, 2, 7, 3, 9, {'A': 1, 'B': 2}, 5, 7, 'Mid', 8, 6, 'End']
#    other sequences and structures
#    (e.g. dictionaries and strings) are unaffected.
#    :param tree: sequence to be flattened
#    :return: flattened sequence
#    """
#    result = []
#    if isinstance(tree, (list, tuple)):
#        for elem in tree:
#            result.extend(flatten(elem))
#    else:
#        result.append(tree)
#    return result
