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


def export_to_dcm(self, path):
    """
    export_to_dcm creates .dcm image from dicom object with (metadata, imagedata) and saves to given path

    Parameters
    ----------

    :param path: (str) local/hdfs path


    Examples
    --------

        >>> dicom_path = "../datasets/dicom_uncompressed"

        >>> dicom = tc.dicom.import_dcm(dicom_path)

        >>> dicom.metadata.count()
        3

        >>> dicom.pixeldata.count()
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

        #pixeldata property is sparktk frame
        >>> pixeldata = dicom.pixeldata.take(1)

        #dispaly
        <skip>
        >>> pixeldata
        [[0L, array([[   0.,    0.,    0., ...,    0.,    0.,    0.],
        [   0.,  125.,  103., ...,  120.,  213.,  319.],
        [   0.,  117.,   94., ...,  135.,  223.,  325.],
        ...,
        [   0.,   62.,   21., ...,  896.,  886.,  854.],
        [   0.,   63.,   23., ...,  941.,  872.,  897.],
        [   0.,   60.,   30., ...,  951.,  822.,  906.]])]]
        </skip>

        >>> dicom.export_to_dcm("sandbox/dicom_export")

        >>> loaded_dicom = tc.dicom.import_dcm("sandbox/dicom_export")

        >>> loaded_dicom.metadata.count()
        3

        >>> loaded_dicom.pixeldata.count()
        3

    """

    if not isinstance(path, basestring):
        raise TypeError("path must be a type of string, but found type as " % type(path))

    def f(scala_dicom):
        scala_dicom.exportToDcm(path)

    self._call_scala(f)