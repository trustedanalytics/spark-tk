def drop_rows_by_tags(self, dict_tags_values):
    """
    filter metadata which does not match with given dictionary of tags and values

     Ex: dict_tags_values -> {"00100020":"12344", "00080018","1.3.12.2.1107.5.2.5.11090.5.0.5823667428974336"}

    :param dict_tags_values: (dict(str, str)) dictionary of tags and values from xml string in metadata

    """