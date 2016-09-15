def extract_keywords(self, keywords):
    """

    Extract values for the given keywords and add as new columns in the metadata frame

    Ex: keywords -> ["PatientID"]

    Parameters
    ----------

    :param keywords: (str or list(str)) List of keywords from metadata xml string


    Examples
    --------

        >>> dicom_path = "../datasets/dicom_uncompressed"

        >>> dicom = tc.dicom.import_dcm(dicom_path)

        >>> dicom.extract_keywords(["SOPInstanceUID", "Manufacturer", "StudyDate"])

    """
    self._scala.extractKeywords(self._tc.jutils.convert.to_scala_vector_string(keywords))