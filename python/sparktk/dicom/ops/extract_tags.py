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

def extract_tags(self, tags):
    """
    Extracts value for each tag from column holding xml string and adds column for each tag to assign value.
    For missing tag, the value is None

    Ex: tags -> ["00020001", "00020002"]

    Parameters
    ----------

    :param tags: (str or list(str)) List of tags from xml string of metadata column


    Examples
    --------

        <skip>
        >>> dicom_path = "../datasets/dicom_uncompressed"

        >>> dicom = tc.dicom.import_dcm(dicom_path)

        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata
        =======================================
        [0]   0  <?xml version="1.0" encodin...
        [1]   1  <?xml version="1.0" encodin...
        [2]   2  <?xml version="1.0" encodin...

        #Part of xml string looks as below
        <?xml version="1.0" encoding="UTF-8"?>
            <NativeDicomModel xml:space="preserve">
                <DicomAttribute keyword="FileMetaInformationVersion" tag="00020001" vr="OB"><InlineBinary>AAE=</InlineBinary></DicomAttribute>
                <DicomAttribute keyword="MediaStorageSOPClassUID" tag="00020002" vr="UI"><Value number="1">1.2.840.10008.5.1.4.1.1.4</Value></DicomAttribute>
                <DicomAttribute keyword="MediaStorageSOPInstanceUID" tag="00020003" vr="UI"><Value number="1">1.3.12.2.1107.5.2.5.11090.5.0.5823667428974336</Value></DicomAttribute>
                ...

        #Extract value for each tag from column holding xml string
        >>> dicom.extract_tags(["00080018", "00080070", "00080030"])

        >>> dicom.metadata.inspect(truncate=20)
        [#]  id  metadata              00080018              00080070  00080030
        ============================================================================
        [0]   0  <?xml version="1....  1.3.12.2.1107.5.2...  SIEMENS   085922.859000
        [1]   1  <?xml version="1....  1.3.12.2.1107.5.2...  SIEMENS   085922.859000
        [2]   2  <?xml version="1....  1.3.12.2.1107.5.2...  SIEMENS   085922.859000
        </skip>

    """

    if isinstance(tags, basestring):
        tags = [tags]

    if not isinstance(tags, list):
        raise TypeError("tags type should be either str or list but found type as %s" % type(tags))

    if self._metadata._is_scala:
        def f(scala_dicom):
            scala_dicom.extractTags(self._tc.jutils.convert.to_scala_vector_string(tags))
        results = self._call_scala(f)
        return results

    #If metadata is python frame, run below udf
    import xml.etree.ElementTree as ET

    def extractor(dtags):
        def _extractor(row):
            root = ET.fromstring(row["metadata"])
            values=[None]*len(dtags)
            for attribute in root.findall('DicomAttribute'):
                dtag = attribute.get('tag')
                if dtag in dtags:
                    if attribute.find('Value') is not None:
                        values[dtags.index(dtag)]=attribute.find('Value').text
            return values
        return _extractor

    cols_type = [(tag, str) for tag in tags]
    self._metadata.add_columns(extractor(tags), cols_type)