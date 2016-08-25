import logging
logger = logging.getLogger('sparktk')

# import constructors for the API's sake (not actually dependencies of the Frame class)
from sparktk.dicom.constructors.import_dicom import import_dicom

class DicomFrame(object):

    def __init__(self, tc, dicom_dir_path):
        self._tc = tc
        self._dicomframe = import_dicom(dicom_dir_path)