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


def drop_rows_by_keywords(self, keywords_values_dict):
    """
    Drop the rows based on dictionary of {"keyword":"value"}(applying 'and' operation on dictionary) from column holding xml string.

    Ex: keywords_values_dict -> {"SOPInstanceUID":"1.3.6.1.4.1.14519.5.2.1.7308.2101.234736319276602547946349519685", "Manufacturer":"SIEMENS", "StudyDate":"20030315"}

    Parameters
    ----------

    :param keywords_values_dict: (dict(str, str)) dictionary of keywords and values from xml string in metadata


    Examples
    --------

        >>> dicom_path = "../datasets/dicom_uncompressed"

        >>> dicom = tc.dicom.import_dcm(dicom_path)

        >>> dicom.metadata.count()
        3

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
                <DicomAttribute keyword="MediaStorageSOPInstanceUID" tag="00020003" vr="UI"><Value number="1">1.3.6.1.4.1.14519.5.2.1.7308.2101.234736319276602547946349519685</Value></DicomAttribute>
                ...

        >>> keywords_values_dict = {"SOPInstanceUID":"1.3.6.1.4.1.14519.5.2.1.7308.2101.234736319276602547946349519685", "Manufacturer":"SIEMENS", "StudyDate":"20030315"}
        >>> dicom.drop_rows_by_keywords(keywords_values_dict)
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
        ===========================================================
        [0]   1  [[   0.    0.    0. ...,    0.    0.    0.]
        [   0.   70.   85. ...,  215.  288.  337.]
        [   0.   63.   72. ...,  228.  269.  317.]
        ...,
        [   0.   42.   40. ...,  966.  919.  871.]
        [   0.   42.   33. ...,  988.  887.  860.]
        [   0.   46.   38. ...,  983.  876.  885.]]
        [1]   2  [[    0.     0.     0. ...,     0.     0.     0.]
        [    0.   111.   117. ...,   159.   148.   135.]
        [    0.   116.   111. ...,   152.   138.   139.]
        ...,
        [    0.    49.    18. ...,  1057.   965.   853.]
        [    0.    42.    20. ...,  1046.   973.   891.]
        [    0.    48.    26. ...,  1041.   969.   930.]]
        </skip>

    """

    if not isinstance(keywords_values_dict, dict):
        raise TypeError("keywords_values_dict should be a type of dict, but found type as %" % type(keywords_values_dict))

    #Always scala dicom is invoked, as python joins are expensive compared to serailizations.
    def f(scala_dicom):
        scala_dicom.dropRowsByKeywords(self._tc.jutils.convert.to_scala_map(keywords_values_dict))

    self._call_scala(f)