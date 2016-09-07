
def export_to_dcm(self, frame_with_metadata_imagedata):
    """
    export_to_dcm helps to create .dcm image using metadata and imagedata columns from give frame

    :param frame_with_metadata_imagedata: frame containing metadata and imagedata as columns

    """
    self._scala.exportToDcm(frame_with_metadata_imagedata)