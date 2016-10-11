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

import logging
logger = logging.getLogger('sparktk')

# import constructors for the API's sake (not actually dependencies of the Dicom class)
from sparktk.dicom.constructors.import_dcm import import_dcm


class Dicom(object):
    """
    sparktk Dicom

    Represents a collection of DICOM data objects. Reference: [https://en.wikipedia.org/wiki/DICOM](https://en.wikipedia.org/wiki/DICOM)

    The metadata property is a sparktk frame which defines the metadata of the collection of DICOM objects.
    Its schema has a column named "id" which holds a unique integer ID for the record and another column which
    holds a string of XML comprised of the metadata.  Users can run XQuery or invoke canned column extraction/filter
    operations on this frame.

    The pixeldata property is a sparktk frame which defines the pixeldata of the collection of DICOM objects.
    Its schema has a column named "id" which holds a unique integer ID for the record and another column which
    holds a matrix(internally it is a numpy.ndarray) comprised of the pixeldata.  Users can run numpy supported transformations on it.

    dcm4che-3.x dependencies are used to support various operations on dicom images. It is available as java library
    Reference: [https://github.com/dcm4che/dcm4che](https://github.com/dcm4che/dcm4che)

    Note: Currently sparktk Dicom supports only uncompressed dicom images

    Load a set of uncompressed sample .dcm files from path (integration-tests/datasets/dicom_uncompressed)
    and create a dicom object. The below examples helps you to understand how to access dicom object properties.

    Examples
    --------

        #Path can be local/hdfs to dcm file(s)
        >>> dicom_path = "../datasets/dicom_uncompressed"

        #use import_dcm available inside dicom module to create a dicom object from given dicom_path
        >>> dicom = tc.dicom.import_dcm(dicom_path)

        #Type of dicom object created
        >>> type(dicom)
        <class 'sparktk.dicom.dicom.Dicom'>

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

        #Display
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

        #Access ndarray
        >>> image_ndarray= pixeldata[0][1]

        >>> type(image_ndarray)
        <type 'numpy.ndarray'>

        #Dimesions of the image matrix stored
        >>> image_ndarray.shape
        (320, 320)

        <skip>
        #Use python matplot lib package to verify image visually
        >>> import pylab
        >>> pylab.imshow(image_ndarray, cmap=pylab.cm.bone)
        >>> pylab.show()

        #Save method persists the dicom object to disk
        >>> dicom.save("sandbox/dicom_data")

        #loads the saved dicom object
        >>> load_dicom = tc.load("sandbox/dicom_data")

        #Re-check whether we loaded back the dicom object or not
        >>> type(load_dicom)
        <class 'sparktk.dicom.dicom.Dicom'>

        #Again access pixeldata and perform same operations as above
        >>> load_pixeldata = load_dicom.pixeldata.take(1)

        #Order may differ when you load back dicom object

        >>> load_pixeldata
        [[0L, array([[   0.,    0.,    0., ...,    0.,    0.,    0.],
        [   0.,  125.,  103., ...,  120.,  213.,  319.],
        [   0.,  117.,   94., ...,  135.,  223.,  325.],
        ...,
        [   0.,   62.,   21., ...,  896.,  886.,  854.],
        [   0.,   63.,   23., ...,  941.,  872.,  897.],
        [   0.,   60.,   30., ...,  951.,  822.,  906.]])]]


        >>> load_image_ndarray= load_pixeldata[0][1]

        >>> type(load_image_ndarray)
        <type 'numpy.ndarray'>

        >>> load_image_ndarray.shape
        (320, 320)

        #Inspect metadata property to see dicom metadata xml content

        >>> load_dicom.metadata.inspect(truncate=30)
        [#]  id  metadata
        =======================================
        [0]   0  <?xml version="1.0" encodin...
        [1]   1  <?xml version="1.0" encodin...
        [2]   2  <?xml version="1.0" encodin...
        </skip>

        #Using to built-in xml libraries to run xquery on metadata
        >>> import xml.etree.ElementTree as ET

        #Performing add_columns operation.
        #Add xml tag as column in dicom metadata frame
        #Here we add SOPInstanceUID as column to metadaframe

        #sample function to apply on row - add_columns
        >>> def extractor(tag_name):
        ...    def _extractor(row):
        ...        root = ET.fromstring(row["metadata"])
        ...        for attribute in root.findall('DicomAttribute'):
        ...            keyword = attribute.get('keyword')
        ...            value = None
        ...            if attribute.find('Value') is not None:
        ...                value = attribute.find('Value').text
        ...            if keyword == tag_name:
        ...                return value
        ...    return _extractor

        >>> tag_name = "SOPInstanceUID"

        >>> dicom.metadata.add_columns(extractor(tag_name), (tag_name, str))

        >>> dicom.metadata.count()
        3

        <skip>
        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata                        SOPInstanceUID
        =======================================================================
        [0]   0  <?xml version="1.0" encodin...  1.3.6.1.4.1.14519.5.2.1.730...
        [1]   1  <?xml version="1.0" encodin...  1.3.6.1.4.1.14519.5.2.1.730...
        [2]   2  <?xml version="1.0" encodin...  1.3.6.1.4.1.14519.5.2.1.730...
        </skip>

    """

    def __init__(self, tc, scala_dicom):
        self._tc = tc
        from sparktk.frame.frame import Frame
        self._metadata = Frame(self._tc, scala_dicom.metadata())
        self._pixeldata = Frame(self._tc, scala_dicom.pixeldata())

    def __repr__(self):
        #TODO Python friendly repr
        #Write a string summary
        return self._get_new_scala().toString()

    @property
    def metadata(self):
        return self._metadata

    @property
    def pixeldata(self):
        return self._pixeldata

    @staticmethod
    def _from_scala(tc, scala_dicom):
        """creates a python dicom for the given scala dicom"""
        return Dicom(tc, scala_dicom)

    #Creating new scala dicom to handle mutability issue.
    # When import_dcm is invoked, it returns scala dicom object(scala metadata frame and pixeldata frame).
    # When user performs add_columns or any operation which turns scala frame to python frame, the link is lost
    # To avoid such issues, we create new dicom object using (metadata and pixeldata frames) when accessing scala method
    def _get_new_scala(self):
        return self._tc.sc._jvm.org.trustedanalytics.sparktk.dicom.Dicom(self._metadata._scala, self._pixeldata._scala)

    #method to call passed function with new scala dicom
    def _call_scala(self, func):
        from sparktk.frame.frame import Frame
        scala_dicom = self._get_new_scala()
        results = func(scala_dicom)
        self._metadata = Frame(self._tc, scala_dicom.metadata())
        self._pixeldata = Frame(self._tc, scala_dicom.pixeldata())
        return results

    # Dicom Operations
    from sparktk.dicom.ops.drop_rows import drop_rows
    from sparktk.dicom.ops.drop_rows_by_keywords import drop_rows_by_keywords
    from sparktk.dicom.ops.drop_rows_by_tags import drop_rows_by_tags
    from sparktk.dicom.ops.extract_keywords import extract_keywords
    from sparktk.dicom.ops.extract_tags import extract_tags
    from sparktk.dicom.ops.export_to_dcm import export_to_dcm
    from sparktk.dicom.ops.filter import filter
    from sparktk.dicom.ops.filter_by_keywords import filter_by_keywords
    from sparktk.dicom.ops.filter_by_tags import filter_by_tags
    from sparktk.dicom.ops.save import save

