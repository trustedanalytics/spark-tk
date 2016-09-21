def filter_by_keywords(self, keywords_values_dict):
    """
    Filter the rows based on dictionary of {"keyword":"value"} from column holding xml string

    Ex: keywords_values_dict -> {"SOPInstanceUID":"1.3.12.2.1107.5.2.5.11090.5.0.5823667428974336", "Manufacturer":"SIEMENS", "StudyDate":"20040305"}

    Parameters
    ----------

    :param keywords_values_dict: (dict(str, str)) dictionary of keywords and values from xml string in metadata


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

        >>> keywords_values_dict = {"SOPInstanceUID":"1.3.12.2.1107.5.2.5.11090.5.0.5823667428974336", "Manufacturer":"SIEMENS", "StudyDate":"20040305"}
        >>> dicom.filter_by_keywords(keywords_values_dict)
        >>> dicom.metadata.count()
        1

        <skip>
        #After filter
        >>> dicom.metadata.inspect(truncate=30)
        [#]  id  metadata
        =======================================
        [0]   0  <?xml version="1.0" encodin...

        >>> dicom.pixeldata.inspect(truncate=30)
        [#]  id  imagematrix
        =========================================
        [0]   0  [[ 0.  0.  0. ...,  0.  0.  0.]
        [ 0.  7.  5. ...,  5.  7.  8.]
        [ 0.  7.  6. ...,  5.  6.  7.]
        ...,
        [ 0.  6.  7. ...,  5.  5.  6.]
        [ 0.  2.  5. ...,  5.  5.  4.]
        [ 1.  1.  3. ...,  1.  1.  0.]]
        </skip>

    """

    if not isinstance(keywords_values_dict, dict):
        raise TypeError("keywords_values_dict should be a type of dict, but found type as %" % type(keywords_values_dict))

    #Always scala dicom is invoked, as python joins are expensive compared to serailizations.
    def f(scala_dicom):
        scala_dicom.filterByKeywords(self._tc.jutils.convert.to_scala_map(keywords_values_dict))

    self._call_scala(f)