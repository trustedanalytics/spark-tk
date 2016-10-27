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

from setup import tc, rm, get_sandbox_path
from sparktk import dtypes
import numpy as np
import dicom as pydicom


def test_import_dcm_py_to_scala_and_scala_to_py_count(tc):
    """
    Create dicom object and perform python to scala and scala to python ops
    """
    dicom_path = "../datasets/dicom_uncompressed"
    dicom = tc.dicom.import_dcm(dicom_path)

    # Dicom object contains two frames. Metadata frame and Pixeldata frame.
    # Check for frame type. By default the frames should in scala.
    assert(dicom.metadata._is_scala == True)
    assert(dicom.pixeldata._is_scala == True)
    assert(dicom.metadata._is_python == False)
    assert(dicom.pixeldata._is_python == False)

    # Each frame should contain 3 rows
    assert(dicom.metadata.count(), 3)
    assert(dicom.pixeldata.count(), 3)

    # Convert both frames to python
    dicom.metadata._python
    dicom.pixeldata._python
    assert(dicom.metadata._is_scala == False)
    assert(dicom.pixeldata._is_scala == False)
    assert(dicom.metadata._is_python == True)
    assert(dicom.pixeldata._is_python == True)

    # Each frame should contain 3 rows
    assert(dicom.metadata.count(), 3)
    assert(dicom.pixeldata.count(), 3)


def test_check_mr_metadata_and_pixeldata_content(tc):
    """
    Check metadata and pixeldata content before and after conversions
    Pixeldata compared with pydicom pixel_array
    """
    dicom_path = "../datasets/dicom_uncompressed/mr1.dcm"
    dicom = tc.dicom.import_dcm(dicom_path)

    # using pydicom
    pydc_pixeldata = pydicom.read_file(dicom_path).pixel_array

    scala_metadata = dicom.metadata.take(1)[0][1]
    scala_pixeldata = dicom.pixeldata.take(1)[0][1]

    # Compare pydicom pixeldata vs our pixelddata (In scala space)
    assert((pydc_pixeldata == scala_pixeldata).all() == True)

    # Convert frames to python
    dicom.metadata._python
    dicom.pixeldata._python

    py_metadata = dicom.metadata.take(1)[0][1]
    py_pixeldata = dicom.pixeldata.take(1)[0][1]

    assert(scala_metadata == py_metadata)

    # Compare pydicom pixeldata vs our pixelddata (In python space)
    assert((pydc_pixeldata == py_pixeldata).all() == True)

    # Convert back frames to scala
    dicom.metadata._scala
    dicom.pixeldata._scala

    scala_pixeldata = dicom.pixeldata.take(1)[0][1]
    assert((pydc_pixeldata == scala_pixeldata).all() == True)

    # Convert frames to python
    dicom.metadata._python
    dicom.pixeldata._python

    py_pixeldata = dicom.pixeldata.take(1)[0][1]
    assert((pydc_pixeldata == py_pixeldata).all() == True)


def test_matrix_using_import_dcm_and_export_dcm(tc):
    dicom_path = "../datasets/dicom_uncompressed/mr1.dcm"
    dicom = tc.dicom.import_dcm(dicom_path)

    # using pydicom
    pydc_pixeldata = pydicom.read_file(dicom_path).pixel_array

    # using our dicom module
    import_pixeldata = dicom.pixeldata.take(1)[0][1]

    # Compare with pydicom pixeldata
    assert((pydc_pixeldata == import_pixeldata).all() == True)

    save_path = "sandbox/dicom_export_test"
    dicom.export_to_dcm(save_path)

    export_dicom = tc.dicom.import_dcm(save_path)
    export_pixeldata = export_dicom.pixeldata.take(1)[0][1]

    # Test pixeldata, loading from saved location
    assert((pydc_pixeldata == export_pixeldata).all() == True)


def test_matrix_using_frame_create_with_list(tc):
    data = [[1, [[1,2,3,5],[2,3,5,6],[4,6,7,3],[8,9,2,4]]]]
    schema = [('id', int),('pixeldata', dtypes.matrix)]
    frame = tc.frame.create(data, schema)

    np_ndarray = np.array([[1,2,3,5],[2,3,5,6],[4,6,7,3],[8,9,2,4]])
    py_matrix = frame.take(1)[0][1]

    assert((np_ndarray == py_matrix).all() == True)

    frame._scala

    scala_matrix=frame.take(1)[0][1]
    assert((np_ndarray == scala_matrix).all() == True)

    frame.add_columns(lambda row: row[1]*2, ('new_col', dtypes.matrix))
    new_np_ndarray = np_ndarray *2

    scala_new_matrix = frame.take(1)[0][2]
    assert((new_np_ndarray == scala_new_matrix).all() == True)

    frame._python
    py_new_matrix = frame.take(1)[0][2] /2

    assert((np_ndarray == py_new_matrix).all() == True)


def test_matrix_using_frame_create_with_ndarray(tc):
    data = [[1, np.array([[1,2,3,5],[2,3,5,6],[4,6,7,3],[8,9,2,4]])]]
    schema = [('id', int),('pixeldata', dtypes.matrix)]
    frame = tc.frame.create(data, schema)

    assert(frame._is_python == True)
    assert(frame.count(), 1)

    frame._scala
    assert(frame._is_scala == True)
    assert(frame.count(), 1)

    frame._python
    assert(frame._is_python == True)
    assert(frame.count(), 1)


















