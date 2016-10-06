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

def drop_rows_by_tags(self, tags_values_dict):
    """
    Drop the rows based on dictionary of {"tag":"value"} from column holding xml string

    Ex: tags_values_dict -> {"00080018":"1.3.12.2.1107.5.2.5.11090.5.0.5823667428974336", "00080070":"SIEMENS", "00080020":"20040305"}

    Parameters
    ----------

    :param tags_values_dict: (dict(str, str)) dictionary of tags and values from xml string in metadata


    Examples
    --------

        >>> dicom_path = "../datasets/dicom_uncompressed"

        >>> dicom = tc.dicom.import_dcm(dicom_path)

        <skip>
        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata
        =======================================
        [0]   0  <?xml version="1.0" encodin...
        [1]   1  <?xml version="1.0" encodin...
        [2]   2  <?xml version="1.0" encodin...
        </skip>

        #Part of xml string looks as below
        <?xml version="1.0" encoding="UTF-8"?>
            <NativeDicomModel xml:space="preserve">
                <DicomAttribute keyword="FileMetaInformationVersion" tag="00020001" vr="OB"><InlineBinary>AAE=</InlineBinary></DicomAttribute>
                <DicomAttribute keyword="MediaStorageSOPClassUID" tag="00020002" vr="UI"><Value number="1">1.2.840.10008.5.1.4.1.1.4</Value></DicomAttribute>
                <DicomAttribute keyword="MediaStorageSOPInstanceUID" tag="00020003" vr="UI"><Value number="1">1.3.12.2.1107.5.2.5.11090.5.0.5823667428974336</Value></DicomAttribute>
                ...

        >>> tags_values_dict = {"00080018":"1.3.12.2.1107.5.2.5.11090.5.0.5823667428974336", "00080070":"SIEMENS", "00080020":"20040305"}
        >>> dicom.drop_rows_by_tags(tags_values_dict)
        >>> dicom.metadata.count()
        2


        <skip>
        #After drop_rows
        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata
        =======================================
        [0]   1  <?xml version="1.0" encodin...
        [1]   2  <?xml version="1.0" encodin...

        >>> dicom.pixeldata.inspect(truncate=30)
        [#]  id  imagematrix
        =========================================
        [1]   1  [[  0.   1.   0. ...,   0.   0.   1.]
        [  1.   9.  10. ...,   2.   4.   6.]
        [  0.  12.  11. ...,   4.   4.   7.]
        ...,
        [  0.   4.   2. ...,   3.   5.   5.]
        [  0.   8.   5. ...,   7.   8.   8.]
        [  0.  10.  10. ...,   8.   8.   8.]]
        [2]   2  [[ 0.  0.  0. ...,  0.  0.  0.]
        [ 0.  2.  2. ...,  6.  5.  5.]
        [ 0.  7.  8. ...,  4.  4.  5.]
        ...,
        [ 0.  4.  1. ...,  4.  5.  6.]
        [ 0.  4.  5. ...,  6.  6.  5.]
        [ 1.  6.  8. ...,  4.  5.  4.]]
        </skip>

    """

    if not isinstance(tags_values_dict, dict):
        raise TypeError("tags_values_dict should be a type of dict, but found type as %" % type(tags_values_dict))

    #Always scala dicom is invoked, as python joins are expensive compared to serailizations.
    def f(scala_dicom):
        scala_dicom.dropRowsByTags(self._tc.jutils.convert.to_scala_map(tags_values_dict))

    self._call_scala(f)
