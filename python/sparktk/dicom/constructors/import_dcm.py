from sparktk.tkcontext import TkContext


def import_dcm(dicom_dir_path, tc=TkContext.implicit):
    """
    Creates a dicom object with metadataFrame and imagedataFrame from a dcm file(s)

    Parameters
    ----------

    :param dicom_dir_path: (str) Local/HDFS path of the dcm file(s)
    :return: (Dicom) returns a dicom object with metadata and imagedata frames

    Examples
    --------
        <skip>
            >>> dicom_path = "local/hdfs path"
            >>> frame = tc.dicom.import_dcm(dicom_path)
            -etc-
        </skip>
    """
    if not isinstance(dicom_dir_path, basestring):
        raise ValueError("dicom_dir_path parameter must be a string, but is {0}.".format(type(dicom_dir_path)))

    TkContext.validate(tc)

    scala_dicom = tc.sc._jvm.org.trustedanalytics.sparktk.dicom.internal.constructors.Import.importDicom(tc.jutils.get_scala_sc(), dicom_dir_path)
    from sparktk.dicom.dicom import Dicom
    return Dicom(tc, scala_dicom)