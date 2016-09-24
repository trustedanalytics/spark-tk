def filter_by_keywords(self, dict_keywords_values):
    """
    filter metadata with given dictionary of keywords and values

    Ex: dict_keywords_values -> {"PatientID":"12344", "SOPInstanceUID","1.3.12.2.1107.5.2.5.11090.5.0.5823667428974336"}

    :param dict_keywords_values: (dict(str, str)) dictionary of keywords and values from xml string in metadata

    """