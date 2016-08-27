from sparktk.tkcontext import TkContext


def import_dicom(dicom_dir_path, tc=TkContext.implicit):
    """
    Import data from dicom directory into frame

    Parameters
    ----------

    :param dicom_dir_path: (str) hdfs path of the dicom files directory
    :return: (DicomFrame) returns a dicom frame object which contains metadata_frame and imagedata_frame

    Examples
    --------
    create a dicom frame from a given hdfs dicom files directory.

    <skip>
        >>> dicom_path = "hdfs://10.7.151.97:8020/user/kvadla/dicom_images_decompressed"
        >>> frame = tc.dicomframe.import_dicom(dicom_path)
        -etc-
    </skip>
    """
    if not isinstance(dicom_dir_path, basestring):
        raise ValueError("dicom_dir_path parameter must be a string, but is {0}.".format(type(dicom_dir_path)))

    TkContext.validate(tc)

    scala_dicom = tc.sc._jvm.org.trustedanalytics.sparktk.dicom.internal.constructors.Import.importDicom(tc.jutils.get_scala_sc(), dicom_dir_path)
    from sparktk.dicom.dicom import Dicom
    return Dicom(tc, scala_dicom)