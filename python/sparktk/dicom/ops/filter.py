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


def filter(self, predicate):

    """
    Filter the rows of dicom metadata and pixeldata based on given predicate

    Parameters
    ----------

    :param predicate: predicate to apply on filter


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

        >>> import xml.etree.ElementTree as ET

        #sample custom filter function
        >>> def filter_meta(tag_name, tag_value):
        ...    def _filter_meta(row):
        ...        root = ET.fromstring(row["metadata"])
        ...        for attribute in root.findall('DicomAttribute'):
        ...            keyword = attribute.get('keyword')
        ...            if attribute.get('keyword') is not None:
        ...                if attribute.find('Value') is not None:
        ...                    value = attribute.find('Value').text
        ...                    if keyword == tag_name and value == tag_value:
        ...                        return True
        ...    return _filter_meta

        >>> tag_name = "SOPInstanceUID"

        >>> tag_value = "1.3.6.1.4.1.14519.5.2.1.7308.2101.234736319276602547946349519685"

        >>> dicom.filter(filter_meta(tag_name, tag_value))
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

    self.metadata.filter(predicate)
    filtered_id_frame = self.metadata.copy(columns = "id")
    self._pixeldata = filtered_id_frame.join_inner(self.pixeldata, "id")
