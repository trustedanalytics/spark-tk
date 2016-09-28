from sparktk.tkcontext import TkContext


def import_dcm(dicom_dir_path, tc=TkContext.implicit):
    """
    Creates a dicom object with metadataFrame and pixeldataFrame from a dcm file(s)

    Parameters
    ----------

    :param dicom_dir_path: (str) Local/HDFS path of the dcm file(s)
    :return: (Dicom) returns a dicom object with metadata and pixeldata frames


    Examples
    --------
        #Path can be local/hdfs to dcm file(s)
        >>> dicom_path = "../datasets/dicom_uncompressed"

        #use import_dcm available inside dicom module to create a dicom object from given dicom_path
        >>> dicom = tc.dicom.import_dcm(dicom_path)

        #Type of dicom object created
        >>> type(dicom)
        <class 'sparktk.dicom.dicom.Dicom'>

        #Inspect metadata property to see dicom metadata xml content
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

        <skip>

        >>> pixeldata
        [[0L, array([[ 0.,  0.,  0., ...,  0.,  0.,  0.],
        [ 0.,  7.,  5., ...,  5.,  7.,  8.],
        [ 0.,  7.,  6., ...,  5.,  6.,  7.],
        ...,
        [ 0.,  6.,  7., ...,  5.,  5.,  6.],
        [ 0.,  2.,  5., ...,  5.,  5.,  4.],
        [ 1.,  1.,  3., ...,  1.,  1.,  0.]])]]
        </skip>

    """
    if not isinstance(dicom_dir_path, basestring):
        raise ValueError("dicom_dir_path parameter must be a string, but is {0}.".format(type(dicom_dir_path)))

    TkContext.validate(tc)

    scala_dicom = tc.sc._jvm.org.trustedanalytics.sparktk.dicom.internal.constructors.Import.importDcm(tc.jutils.get_scala_sc(), dicom_dir_path)
    from sparktk.dicom.dicom import Dicom
    return Dicom._from_scala(tc, scala_dicom)