import logging
from django.conf import settings
from celery import chain
from nifcert import metadata
from nifcert import tasks

class DataFileMetadataFilter(object):
    """MyTardis filter to process each uploaded DataFile, adding any
    TruDat metadata required to indicate its compliance with standards
    and procedures.  May also update the metadata for the DataFile's
    Dataset.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)


    def __call__(self, sender, **kwargs):
        """Post save call back to invoke this filter.
        Parameters
        ----------
        sender: Model
            class of the model
        datafile: Datafile model Instance
            Instance of Datafile model being saved.
        created: boolean
            Specifies whether a new record is being created.
        """
        self.logger.info("nifcert.metadata_filter Datafile save callback "
                         "begin")
        datafile = kwargs.get('instance')
        if not metadata.is_datafile_instrument_nifcert(datafile.id):
            self.logger.info("nifcert.metadata_filter Datafile save callback "
                             "Datafile[%d] is not from a NIFCert-enabled"
                             "instrument (skipping)", datafile.id)
            return

        dataset_id = datafile.dataset.id if datafile.dataset else -1
        self.logger.info("nifcert.metadata_filter Datafile save callback "
                         "Datafile[%d]  Dataset[%d]",
                         datafile.id, dataset_id)

        # Asynchronously update the DataFile metadata, then the
        # Dataset metadata.  Don't assume a result backend is
        # available. Use sequential tasks in a chain with immutable
        # signatures since they don't need to communicate.
        q_name_setting = 'DATACERT_QUEUE'
        q = getattr(settings, q_name_setting, 'celery')
        chain(
            tasks.update_datafile_status.si(metadata.scan_datafile_for_metadata,
                                            datafile.id, False, True),
            tasks.update_dataset_status.si(dataset_id, datafile.id, False, True)
        ).apply_async(queue=q)
        self.logger.info("nifcert.metadata_filter Datafile save callback end")

def make_filter():
    return DataFileMetadataFilter()

make_filter.__doc__ = DataFileMetadataFilter.__doc__
