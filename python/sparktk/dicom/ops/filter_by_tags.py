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


def filter_by_tags(self, tags_values_dict):
    """
    Filter the rows based on dictionary of {"tag":"value"} from column holding xml string

    Ex: tags_values_dict -> {"00080018":"1.3.6.1.4.1.14519.5.2.1.7308.2101.234736319276602547946349519685", "00080070":"SIEMENS", "00080020":"20030315"}

    Parameters
    ----------

    :param tags_values_dict: (dict(str, str)) dictionary of tags and values from xml string in metadata


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

        >>> tags_values_dict = {"00080018":"1.3.6.1.4.1.14519.5.2.1.7308.2101.234736319276602547946349519685", "00080070":"SIEMENS", "00080020":"20030315"}
        >>> dicom.filter_by_tags(tags_values_dict)
        >>> dicom.metadata.count()
        1

        <skip>
        #After filter
        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata
        =======================================
        [0]   0  <?xml version="1.0" encodin...

        >>> dicom.pixeldata.inspect(truncate=30)
        [#]  id  imagematrix
        =====================================================
        [0]   0  [[   0.    0.    0. ...,    0.    0.    0.]
        [   0.  125.  103. ...,  120.  213.  319.]
        [   0.  117.   94. ...,  135.  223.  325.]
        ...,
        [   0.   62.   21. ...,  896.  886.  854.]
        [   0.   63.   23. ...,  941.  872.  897.]
        [   0.   60.   30. ...,  951.  822.  906.]]
        </skip>

    """

    if not isinstance(tags_values_dict, dict):
        raise TypeError("tags_values_dict should be a type of dict, but found type as %" % type(tags_values_dict))

    #Always scala dicom is invoked, as python joins are expensive compared to serailizations.
    def f(scala_dicom):
        scala_dicom.filterByTags(self._tc.jutils.convert.to_scala_map(tags_values_dict))

    self._call_scala(f)