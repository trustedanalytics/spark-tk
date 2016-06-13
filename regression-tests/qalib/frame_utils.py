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
"""A set of utilities used to control the frames in Intel analytics"""

import trustedanalytics as ia
import common_utils
import math
import numpy

import config
import common_utils as profiler


def build_frame(input_path, schema=None, prefix="NoName", skip_header_lines=0,
                delimit=',', file_format="csv", location=config.data_location):
    """
    Creates a data frame
    :param input_path : string
        Name of data input file.
        File must be in the hadoop file system.
        Relative paths are interpreted relative to
        the org.trunstedanalytics.atk.engine.fs.root configuration.
        Absolute paths (beginning with hdfs://..., for example) are also
        supported.
    :param schema : [(string, type)] or string (optional)
        schema description of the fields for a given line.
        It is a list of tuples which describe each field,
        (field name, field type), where the field name is a string,
        and field type is a supported type, (See data_types from
        the iatypes module).  Unicode characters should not be used
        in the column name. This must be passed in if csv
        is set for file_format
        If xml is set for file format, this is the node to split on
    :param delimit : string (optional)
        string indicator of the delimiter for the fields
    :param  skip_header_lines : int (optional)
        indicates numbers of lines to skip before parsing records.
    :param file_format: string (optional)
        Format of the file to be parsed. Currently csv, line, xml,
            json or list.
        Defaults to csv
    :return: Frame
        Frame that is built
    :raises: RuntimeException
        If csv is set and no schema given or if xml is set and no node name
        given this will be raised
    """
    if file_format != "list":
        print "create frame from file %s with schema %s" % (input_path, schema)
    # Input path is the content, not a string in the case of 
    # list input

    if file_format == "csv":
        if schema is None:
            raise RuntimeError("no schema defined")
        placed_path = location + "/" + input_path

        read_file = ia.CsvFile(placed_path, schema,
                               skip_header_lines=skip_header_lines,
                               delimiter=delimit)
    elif file_format == "json":
        placed_path = location + "/" + input_path
        read_file = ia.JsonFile(placed_path)

    elif file_format == "line":
        placed_path = location + "/" + input_path
        read_file = ia.LineFile(placed_path)

    elif file_format == "xml":
        if schema is None:
            raise RuntimeError("no node defined")
        placed_path = location + "/" + input_path

        read_file = ia.XmlFile(placed_path, schema)

    elif file_format == "list":
        if schema is None:
            raise RuntimeError("no schema defined")
        
        read_file = ia.UploadRows(input_path, schema)

    hardened_name = common_utils.get_a_name(prefix)

    print "Building Data Frame"
    with profiler.Timer("profile."+prefix, config.frame_profile):
        frame = ia.Frame(read_file, name=hardened_name)

    return frame


def compare_frames(frame1, frame2, cols, max_rows=10000, almost_equal=False, digits=16):
    """ Compares a given number of rows of 2 frames,
        :param frame1: frame
            first frame
        :param frame2: frame
            second frame
        :param cols: list tuple(str,str)
            a list of tuples containing the frame1 column to compare against the frame2 column
        :param max_rows: int (optional)
             Maximum number of rows to use
        :param almost_equal: bool (optional)
            If true will assume the elements are floats, and will only use
            :digits: number of digits
        :param digits: int
            number of digits to compare in a floating point number

        :returns: bool
            whether or not the frames are the same

        Note that the columns to be compared in :frame_1_info: and
        :frame_2_info: are based on position, not name. So the first column
        in :frame_1_info: is compared to the first column in :frame_2_info:,
        even if they have different names.
    """

    print cols
    frame1.sort(map(lambda (x,y): x, cols))
    frame2.sort(map(lambda (x,y): y, cols))
    

    # Get all rows (up to the limit stipulated)
    frame1_data = frame1.download(max_rows, columns=map(lambda (x,y): x, cols))
    frame2_data = frame2.download(max_rows, columns=map(lambda (x,y): y, cols))

    for (l,r) in cols:
        left = frame1_data[l].copy()
        right = frame2_data[r].copy()
        left.sort()
        right.sort()
        for i in range(left.count()):
            li = left[i]
            ri = right[i]
            if almost_equal and (type(li) is numpy.float64 or type(li) is numpy.float32):
                if (numpy.isnan(li) and not numpy.isnan(ri)) or (not numpy.isnan(li) and numpy.isnan(ri)):
                    print "nan not equal", li, ri
                    return False
                elif not numpy.isnan(li) and not numpy.isnan(ri) and (round(li, digits) != round(ri, digits)):
                    print "float not equal", round(li, digits), round(ri, digits)
                    return False
            else:
                if li != ri:
                    print "Not equivalent", li, ri
                    return False

    return True
