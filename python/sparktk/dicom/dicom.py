import logging
logger = logging.getLogger('sparktk')

from sparktk.propobj import PropertiesObject
import types

# import constructors for the API's sake (not actually dependencies of the Dicom class)
from sparktk.dicom.constructors.import_dcm import import_dcm


class Dicom(PropertiesObject):
    """
    sparktk Dicom

    Represents a collection of DICOM data objects. Reference: [https://en.wikipedia.org/wiki/DICOM](https://en.wikipedia.org/wiki/DICOM)

    The metadata property is a sparktk frame which defines the metadata of the collection of DICOM objects.
    Its schema has a column named "id" which holds a unique integer ID for the record and another column which
    holds a string of XML comprised of the metadata.  Users can run XQuery or invoke canned column extraction/filter
    operations on this frame.

    The imagedata property is a sparktk frame which defines the imagedata of the collection of DICOM objects.
    Its schema has a column named "id" which holds a unique integer ID for the record and another column which
    holds a matrix(internally it is a numpy.ndarray) comprised of the imagedata.  Users can run numpy supported transformations on it.

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

        #imagedata property is sparktk frame
        >>> imagedata = dicom.imagedata.take(1)

        #dispaly
        >>> imagedata
        TakeResult(data=[[0L, array([[ 0.,  0.,  0., ...,  0.,  0.,  0.],
        [ 0.,  7.,  5., ...,  5.,  7.,  8.],
        [ 0.,  7.,  6., ...,  5.,  6.,  7.],
        ...,
        [ 0.,  6.,  7., ...,  5.,  5.,  6.],
        [ 0.,  2.,  5., ...,  5.,  5.,  4.],
        [ 1.,  1.,  3., ...,  1.,  1.,  0.]])]], schema=[(u'id', <type 'long'>), (u'imagematrix', matrix)])

        #Access ndarray
        >>> image_ndarray= imagedata.data[0][1]

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
        </skip>

        #Save method helps to save dicom object in parquet format
        >>> dicom.save("sandbox/dicom_data")

        #Loading the saved dicom object
        >>> load_dicom = tc.load("sandbox/dicom_data")

        #Re-check whether we loaded back the dicom object or not
        >>> type(load_dicom)
        <class 'sparktk.dicom.dicom.Dicom'>

        #Again access imagedata and perform same operations as above
        >>> load_imagedata = load_dicom.imagedata.take(1)

        #Order may differ when you load back dicom object
        <skip>
        >>> load_imagedata
        TakeResult(data=[[0L, array([[ 0.,  0.,  0., ...,  0.,  0.,  0.],
        [ 0.,  7.,  5., ...,  5.,  7.,  8.],
        [ 0.,  7.,  6., ...,  5.,  6.,  7.],
        ...,
        [ 0.,  6.,  7., ...,  5.,  5.,  6.],
        [ 0.,  2.,  5., ...,  5.,  5.,  4.],
        [ 1.,  1.,  3., ...,  1.,  1.,  0.]])]], schema=[(u'id', <type 'long'>), (u'imagematrix', matrix)])
        </skip>

        >>> load_image_ndarray= load_imagedata.data[0][1]

        >>> type(load_image_ndarray)
        <type 'numpy.ndarray'>

        >>> load_image_ndarray.shape
        (512, 512)

        #Inspect metadata property to see dicom metadata xml content
        <skip>
        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata
        =======================================
        [0]   0  <?xml version="1.0" encodin...
        [1]   1  <?xml version="1.0" encodin...
        [2]   2  <?xml version="1.0" encodin...
        </skip>

        #Using to built-in xml libraries to run xquery on metadata
        >>> import xml.etree.ElementTree as ET

        #Performing filter and add_columns operation.

        #Filter xml content with specified tag_name and tag_value.

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

        #One of tags in xml
        >>> tag_name = "SOPInstanceUID"

        #Tag value
        >>> tag_value = "1.3.12.2.1107.5.2.5.11090.5.0.5823667428974336"

        #Using filter
        >>> dicom.filter(filter_meta(tag_name, tag_value))

        #After filter
        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata
        =======================================
        [0]   0  <?xml version="1.0" encodin...

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

        >>> load_dicom.add_columns(extractor(tag_name), (tag_name, str))

        <skip>
        >>> load_dicom.metadata.inspect(truncate=30)
        [#]  id  metadata                        SOPInstanceUID
        =======================================================================
        [0]   0  <?xml version="1.0" encodin...  1.3.12.2.1107.5.2.5.11090.5...
        [1]   1  <?xml version="1.0" encodin...  1.3.12.2.1107.5.2.5.11090.5...
        [2]   2  <?xml version="1.0" encodin...  1.3.12.2.1107.5.2.5.11090.5...
        </skip>

    """

    def __init__(self, tc, scala_dicom):
        self._tc = tc
        self._scala = scala_dicom
        from sparktk.frame.frame import Frame
        self._metadata = Frame(self._tc, scala_dicom.metadata())
        self._imagedata = Frame(self._tc, scala_dicom.imagedata())


        # Imagedata frame operations added at runtime
        def transform(frame, udf):
            """
            0. Convert to python frame using _python
            1. Convert frame column data to ndarray
            2. Execute udf on ndfarray
            3. Convert column datatype to matrix
            :param frame:
            :param udf:
            :return:
            """
        self._imagedata.transform = types.MethodType(transform, self._imagedata, Frame)


        def pca(frame, **args):
            """
            pca will take frame and other parameters(yet to finalize)

            :param frame:
            :return:
            """
        self._imagedata.pca = types.MethodType(pca, self._imagedata, Frame)


        def svd(frame, **args):
            """
            svd will take frame and other parameters(yet to finalize)

            :param frame:
            :param args:
            :return:
            """
        self._imagedata.svd = types.MethodType(svd, self._imagedata, Frame)


    def __repr__(self):
        return self._scala.toString()


    @property
    def metadata(self):
        return self._metadata

    @property
    def imagedata(self):
        return self._imagedata

    @staticmethod
    def _from_scala(tc, scala_frame):
        """creates a python Frame for the given scala Frame"""
        return Dicom(tc, scala_frame)


    # Dicom Operations
    from sparktk.dicom.ops.add_columns import add_columns, add_columns_by_keyword, add_columns_by_tag
    from sparktk.dicom.ops.export_to_dcm import export_to_dcm
    from sparktk.dicom.ops.filter import filter, filter_by_keyword, filter_by_tag, drop_by_keyword, drop_by_tag
    from sparktk.dicom.ops.save import save

