import logging
logger = logging.getLogger('sparktk')

from sparktk.propobj import PropertiesObject

# import constructors for the API's sake (not actually dependencies of the Frame class)
from sparktk.dicom.constructors.import_dicom import import_dicom


class Dicom(PropertiesObject):
    """
    sparktk Dicom

    Represents a dicom with a frame defining metadata and another frame defining imagedata.
    It is implemented using dcm4che3 and methods are available to access metadata and imagedata

    A metadata frame defines the metadata for the dicom image (.dcm) and must have a schema with a column
    named "id" which provides unique dicom image ID and other column with dicom image metadata as a xml string.
    User should be able to run XQuery on the xml string and filter the records as needed.

    An imagedata frame defines the image information as the DenseMatrix(numpy.ndarray) of the dicom(.dcm) and must
    have  a schema with a column named "id" which provides unique dicom image ID (and also helps to perform join with metadata frame) and
    other column as imagedata which is "numpy.ndarray".


    Examples
    --------

        #If it is on HDFS, path should be like hdfs://<ip-address>:8020/user/<user-name>/<dicom-directory-name>
        >>> dicom_path = "../datasets/dicom_uncompressed"

        >>> dicom = tc.dicom.import_dicom(dicom_path)

        >>> type(dicom)
        <class 'sparktk.dicom.dicom.Dicom'>

        >>> imagedata_frame = dicom.imagedata.take(1)

        >>> imagedata_frame
        TakeResult(data=[[0L, array([[ 0.,  0.,  0., ...,  0.,  0.,  0.],
        [ 0.,  7.,  5., ...,  5.,  7.,  8.],
        [ 0.,  7.,  6., ...,  5.,  6.,  7.],
        ...,
        [ 0.,  6.,  7., ...,  5.,  5.,  6.],
        [ 0.,  2.,  5., ...,  5.,  5.,  4.],
        [ 1.,  1.,  3., ...,  1.,  1.,  0.]])]], schema=[(u'id', <type 'long'>), (u'imagematrix', matrix)])

        >>> image_ndarray= imagedata_frame.data[0][1]

        >>> type(image_ndarray)
        <type 'numpy.ndarray'>

        >>> image_ndarray.shape
        (512, 512)

        <skip>
        >>> import pylab
        >>> pylab.imshow(image_ndarray, cmap=pylab.cm.bone)
        >>> pylab.show()

        </skip>

        >>> dicom.save("sandbox/dicom_data")

        >>> load_dicom = tc.load("sandbox/dicom_data")

        >>> type(load_dicom)
        <class 'sparktk.dicom.dicom.Dicom'>

        >>> load_imagedata_frame = load_dicom.imagedata.take(1)

        <skip>
        >>> load_imagedata_frame
        TakeResult(data=[[0L, array([[ 0.,  0.,  0., ...,  0.,  0.,  0.],
        [ 0.,  7.,  5., ...,  5.,  7.,  8.],
        [ 0.,  7.,  6., ...,  5.,  6.,  7.],
        ...,
        [ 0.,  6.,  7., ...,  5.,  5.,  6.],
        [ 0.,  2.,  5., ...,  5.,  5.,  4.],
        [ 1.,  1.,  3., ...,  1.,  1.,  0.]])]], schema=[(u'id', <type 'long'>), (u'imagematrix', matrix)])
        </skip>

        >>> load_image_ndarray= load_imagedata_frame.data[0][1]

        >>> type(load_image_ndarray)
        <type 'numpy.ndarray'>

        >>> load_image_ndarray.shape
        (512, 512)

        <skip>
        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata
        =======================================
        [0]   0  <?xml version="1.0" encodin...
        [1]   1  <?xml version="1.0" encodin...
        [2]   2  <?xml version="1.0" encodin...
        </skip>

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

        >>> tag_value = "1.3.12.2.1107.5.2.5.11090.5.0.5823667428974336"

        >>> dicom.filter(filter_meta(tag_name, tag_value))

        #After filter
        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata
        =======================================
        [0]   0  <?xml version="1.0" encodin...

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

        >>> load_dicom.metadata.add_columns(extractor(tag_name), (tag_name, str))

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
        self._metadata = Frame(self._tc, scala_dicom.dicomFrame().metadata())
        self._imagedata = Frame(self._tc, scala_dicom.dicomFrame().imagedata())


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
        self._imagedata.__setattr__("transform", transform)

        def pca(frame, **args):
            """
            pca will take frame and other parameters(yet to finalize)

            :param frame:
            :return:
            """
            self._imagedata.__setattr__("pca", pca)

        def svd(frame, **args):
            """
            svd will take frame and other parameters(yet to finalize)

            :param frame:
            :param args:
            :return:
            """
            self._imagedata.__setattr__("svd", svd)


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

