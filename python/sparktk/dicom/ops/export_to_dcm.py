
def export_to_dcm(self, frame_with_metadata_pixeldata):
    """
    export_to_dcm helps to create .dcm image using metadata and pixeldata columns from give frame

    :param frame_with_metadata_pixeldata: frame containing metadata and pixeldata as columns

    """
    self._scala.exportToDcm(frame_with_metadata_pixeldata)