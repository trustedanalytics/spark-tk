
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

        <skip>
        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata
        =======================================
        [0]   0  <?xml version="1.0" encodin...
        [1]   1  <?xml version="1.0" encodin...
        [2]   2  <?xml version="1.0" encodin...
        </skip>

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

        >>> dicom.export_to_dcm("dicom_test_export")

    """

    if not isinstance(path, basestring):
        raise TypeError("path must be a type of string, but found type as " % type(path))

    def f(scala_dicom):
        scala_dicom.exportToDcm(path)

    self._call_scala(f)