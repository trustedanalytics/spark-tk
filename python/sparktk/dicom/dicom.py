import logging
logger = logging.getLogger('sparktk')

from sparktk.propobj import PropertiesObject

# import constructors for the API's sake (not actually dependencies of the Frame class)
from sparktk.dicom.constructors.import_dicom import import_dicom

class Dicom(PropertiesObject):

    def __init__(self, tc, scala_dicom):
        self._tc = tc
        self._dicom = scala_dicom
        from sparktk.frame.frame import Frame
        self._metadata = Frame(self._tc, scala_dicom.dicomFrame().metadata())
        self._imagedata = Frame(self._tc, scala_dicom.dicomFrame().imagedata())

    def __repr__(self):
        return self._dicom.toString()


    @property
    def metadata(self):
        return self._metadata

    @property
    def imagedata(self):
        return self._imagedata

    @staticmethod
    def _from_scala(tc, scala_frame):
        """creates a python Frame for the given scala Frame"""
        return Dicom(tc, scala_frame)

    # Dicom Operations
    from sparktk.dicom.ops.save import save
