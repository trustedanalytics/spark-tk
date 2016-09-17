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

        #pixeldata property is sparktk frame
        >>> pixeldata = dicom.pixeldata.take(1)

        #dispaly
        <skip>
        >>> pixeldata
        TakeResult(data=[[0L, array([[ 0.,  0.,  0., ...,  0.,  0.,  0.],
        [ 0.,  7.,  5., ...,  5.,  7.,  8.],
        [ 0.,  7.,  6., ...,  5.,  6.,  7.],
        ...,
        [ 0.,  6.,  7., ...,  5.,  5.,  6.],
        [ 0.,  2.,  5., ...,  5.,  5.,  4.],
        [ 1.,  1.,  3., ...,  1.,  1.,  0.]])]], schema=[(u'id', <type 'long'>), (u'imagematrix', matrix)])
        </skip>

        #Access ndarray
        >>> image_ndarray= pixeldata.data[0][1]

        >>> type(image_ndarray)
        <type 'numpy.ndarray'>

        #Dimesions of the image matrix stored
        >>> image_ndarray.shape
        (512, 512)

        #Use python matplot lib package to verify image visually
        <skip>
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
        TakeResult(data=[[0L, array([[ 0.,  0.,  0., ...,  0.,  0.,  0.],
        [ 0.,  7.,  5., ...,  5.,  7.,  8.],
        [ 0.,  7.,  6., ...,  5.,  6.,  7.],
        ...,
        [ 0.,  6.,  7., ...,  5.,  5.,  6.],
        [ 0.,  2.,  5., ...,  5.,  5.,  4.],
        [ 1.,  1.,  3., ...,  1.,  1.,  0.]])]], schema=[(u'id', <type 'long'>), (u'imagematrix', matrix)])

        >>> load_image_ndarray= load_pixeldata.data[0][1]

        >>> type(load_image_ndarray)
        <type 'numpy.ndarray'>

        >>> load_image_ndarray.shape
        (512, 512)

        #Inspect metadata property to see dicom metadata xml content

        >>> load_dicom.metadata.inspect(truncate=30)
        [#]  id  metadata
        =======================================
        [0]   0  <?xml version="1.0" encodin...
        [1]   1  <?xml version="1.0" encodin...
        [2]   2  <?xml version="1.0" encodin...

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

        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata                        SOPInstanceUID
        =======================================================================
        [0]   0  <?xml version="1.0" encodin...  1.3.12.2.1107.5.2.5.11090.5...
        [1]   1  <?xml version="1.0" encodin...  1.3.12.2.1107.5.2.5.11090.5...
        [2]   2  <?xml version="1.0" encodin...  1.3.12.2.1107.5.2.5.11090.5...
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
        return self._scala.toString()

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

    def _get_new_scala(self):
        return self._tc.sc._jvm.org.trustedanalytics.sparktk.dicom.Dicom(self._metadata._scala, self._pixeldata._scala)

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

