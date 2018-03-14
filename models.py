import datetime
import pytz
import logging
from django.db import models as db_models

from tardis.tardis_portal.models.datafile import DataFile
from tardis.tardis_portal.models.dataset import Dataset

logger = logging.getLogger(__name__)

# For timestamps use UTC, which doesn't have discontinuities when
# daylight saving changes.

def get_utcnow_str():
    return str(datetime.datetime.now(pytz.utc))


class DatafileCertificationLock(db_models.Model):
    '''A lightweight model for synchronising tasks that update NIFCert
    metadata for DataFiles and Datasets.

    '''
    UNDEFINED =               0     # initial state
    DATAFILE_UPLOADED =       1     # uploaded but not yet verified
    DATAFILE_VERIFIED =       2     # verified after upload
    DATAFILE_PROCESSING =     3     # deriving DataFile NIFCert metadata
    DATAFILE_METADATA_SAVED = 4     # saved NIFCert metadata
    DATASET_PROCESSING =      5     # deriving parent dataset NIFCert metadata
    DATASET_METADATA_SAVED =  6     # saved parent dataset NIFCert metadata
    FAILED =                  15    # error, no NIFCert metadata

    state_name_strings = {
        UNDEFINED:               "UNDEFINED",
        DATAFILE_UPLOADED:       "DATAFILE_UPLOADED",
        DATAFILE_VERIFIED:       "DATAFILE_VERIFIED",
        DATAFILE_PROCESSING:     "DATAFILE_PROCESSING",
        DATAFILE_METADATA_SAVED: "DATAFILE_METADATA_SAVED",
        DATASET_PROCESSING:      "DATASET_PROCESSING",
        DATASET_METADATA_SAVED:  "DATASET_METADATA_SAVED",
        FAILED:                  "FAILED"
    }

    datafile = db_models.OneToOneField(DataFile,
                                       primary_key=True,
                                       on_delete=db_models.CASCADE,
                                       db_index=False)
    state = db_models.PositiveSmallIntegerField(default=UNDEFINED)
    time_updated = db_models.DateTimeField(default=get_utcnow_str)


    def set_state(self, new_state):
        if new_state in DatafileCertificationLock.state_name_strings:
            self.state = new_state
            self.time_updated = get_utcnow_str()


    @staticmethod
    def state_to_string(state):
        return DatafileCertificationLock.state_name_strings.get(
            state, "===UNKNOWN STATE===")
