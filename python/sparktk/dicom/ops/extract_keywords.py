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

def extract_keywords(self, keywords):
    """

    Extracts value for each keyword from column holding xml string and adds column for each keyword to assign value
    For missing keyword, the value is None

    Ex: keywords -> ["PatientID"]

    Parameters
    ----------

    :param keywords: (str or list(str)) List of keywords from metadata xml string


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

        #Extract values for given keywords and add as new columns in metadata frame
        >>> dicom.extract_keywords(["SOPInstanceUID", "Manufacturer", "StudyDate"])

        >>> dicom.metadata.inspect(truncate=20)
        [#]  id  metadata              SOPInstanceUID        Manufacturer  StudyDate
        ============================================================================
        [0]   0  <?xml version="1....  1.3.12.2.1107.5.2...  SIEMENS       20040305
        [1]   1  <?xml version="1....  1.3.12.2.1107.5.2...  SIEMENS       20040305
        [2]   2  <?xml version="1....  1.3.12.2.1107.5.2...  SIEMENS       20040305
        </skip>

    """

    if isinstance(keywords, basestring):
        keywords = [keywords]

    if not isinstance(keywords, list):
        raise TypeError("keywords type should be either str or list but found type as %s" % type(keywords))

    if self._metadata._is_scala:
        def f(scala_dicom):
            scala_dicom.extractKeywords(self._tc.jutils.convert.to_scala_vector_string(keywords))
        results = self._call_scala(f)
        return results

    # If metadata is python frame, run below udf
    import xml.etree.ElementTree as ET

    def extractor(dkeywords):
        def _extractor(row):
            root = ET.fromstring(row["metadata"])
            values=[None]*len(dkeywords)
            for attribute in root.findall('DicomAttribute'):
                dkeyword = attribute.get('keyword')
                if dkeyword in dkeywords:
                    if attribute.find('Value') is not None:
                        values[dkeywords.index(dkeyword)]=attribute.find('Value').text
            return values
        return _extractor

    cols_type = [(key, str) for key in keywords]
    self._metadata.add_columns(extractor(keywords), cols_type)



