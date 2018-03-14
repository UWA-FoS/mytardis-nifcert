import logging
from datetime import datetime
from django.conf import settings
from django.db import transaction
from django.db import DatabaseError
from celery import chain

from tardis.tardis_portal.models import DataFile
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


    def assert_lock_datafile_verified(self, datafile_id):
        """Try to acquire exclusive ownership of the NIFCert
        DatafileCertificationLock state that indicates that its holder can
        initiate the chain of DataFile / Dataset metadata update tasks for a
        newly ingested DataFile.

        This helps prevent a race condition when the MyTardis DataFile verify
        operation completes at around the same time as the initial Datafile
        updload / create.  Both the upload and verify cause the Middleware to
        invoke any filter registered to run when a DataFile is modified.  If
        the verify has completed by the time the upload is processed by the
        middleware, the two filter invocations may be indistinguishable.

        This function optimises a common case where an asynchronous DataFile
        verify completes before the Middleware invokes the DataFile modified
        filters for the intitial upload.  When the initial Middleware filter
        invocation triggered by the DataFile creation calls this function, it
        is allowed to advance the lock state directly to DATAFILE_VERIFIED.
        The subsequent Middleware filter call to this function for the same
        DataFile (triggered by the asynchronous update to DataFile.verified)
        detects this and returns False, indicating the first call has already
        handled the DATAFILE_VERIFIED state.

        The caller is responsible for explicitly managing the lock state
        acquired by this function.

        Parameters
        ----------
        datafile_id: id of the DataFile being processed

        Returns
        -------
        True if the caller acquired the lock for the specified DataFile,
        otherwise False (if the lock state has already been acquired,
        presumably by a concurrent or already completed thread / task).  A
        caller acquiring the lock may safely assume it is their responsibility
        to start the NIFCert DataFile / Dataset metadata update task(s).

        """
        lock_state_asserted = False
        try:
            with transaction.atomic():
                df_rows = DataFile.objects.filter(id=datafile_id)
                num_df_rows = len(df_rows)
                if num_df_rows != 1:
                    self.logger.error(
                        "nifcert.metadata_filter error re-fetching unique "
                        "DataFile[%d] to check async verify, found %d "
                        "matches, not 1", datafile_id)
                    return False
                df = df_rows.first()
                if df is None: # shouldn't happen, but just in case
                    self.logger.error(
                        "nifcert.metadata_filter found 1 match but couldn't "
                        " fetch DataFile[%d] to check async verify",
                        datafile_id)
                    return False
                datafile_verified = df.verified

                # Prevent a race between the Datafile upload and verification
                # threads (likelihood increases for smaller Datafiles).
                from nifcert.models import DatafileCertificationLock \
                    as DataCertLock
                lock, was_created = (
                    DataCertLock.objects.get_or_create(datafile=df))
                if was_created:
                    if datafile_verified:
                        lock.set_state(DataCertLock.DATAFILE_VERIFIED)
                        lock_state_asserted = True
                    else:
                        lock.set_state(DataCertLock.DATAFILE_UPLOADED)
                        # Middleware will call filter again after verify
                    lock.save()
                elif datafile_verified:
                    # Queue Datafile metadata processing, unless underway
                    if lock.state == DataCertLock.DATAFILE_UPLOADED:
                        lock.set_state(DataCertLock.DATAFILE_VERIFIED)
                        lock.save()
                        lock_state_asserted = True
                    elif (lock.state == DataCertLock.UNDEFINED):
                        # should never happen but just in case something breaks
                        self.logger.error(
                            "nifcert.metadata_filter DataFile[%d] lock is in "
                            "UNDEFINED state", datafile_id)
                        return False
                    # else: lock.state >= DATAFILE_VERIFIED.  A concurrent
                    # task is processing the Datafile metadata.  Do nothing.
                #
                # else: this DataFile has been uploaded but not yet verified.
                # Do nothing.  Middleware will call filter again after verify

        except DatabaseError, e:
            self.logger.error(
                "nifcert.metadata_filter DataCertLock handling for "
                "DataFile[%d] returned a DatabaseError:  %s", datafile_id, e)
            return False
        return lock_state_asserted


    def __call__(self, sender, **kwargs):
        """Post-save call back to invoke this filter.
        Parameters
        ----------
        sender: Model
            class of the model
        instance: Datafile model Instance (kwargs)
            Instance of Datafile model being saved.
        created: boolean
            Specifies whether a new record is being created.
        """
        self.logger.debug("nifcert.metadata_filter Datafile save callback "
                          "begin")

        # The kwargs['instance'] datafile.verified value may be stale.  Get a
        # fresh DataFile instance to check its verified status.
        datafile = kwargs.get('instance')
        if not metadata.is_datafile_instrument_nifcert(datafile.id):
            self.logger.debug("nifcert.metadata_filter Datafile save callback "
                              "Datafile[%d] is not from a NIFCert-enabled"
                              "instrument (skipping)", datafile.id)
            return
        datafile_id = datafile.id
        dataset_id = datafile.dataset.id if datafile.dataset else -1
        if dataset_id == -1:
            self.logger.error("nifcert.metadata_filter Datafile save callback "
                              "Datafile[%d] has no Dataset", datafile_id)
            return

        self.logger.debug("nifcert.metadata_filter Datafile save callback "
                          "Datafile[%d] Dataset[%d], asserting lock "
                          "DATAFILE_VERIFIED", datafile_id, dataset_id)

        if not self.assert_lock_datafile_verified(datafile_id):
            self.logger.debug("nifcert.metadata_filter Datafile save callback "
                              "Datafile[%d] Dataset[%d], didn't acquire lock "
                              "DATAFILE_VERIFIED", datafile_id, dataset_id)
        else:
            self.logger.debug("nifcert.metadata_filter Datafile save callback "
                              "Datafile[%d] Dataset[%d], asserted lock "
                              "DATAFILE_VERIFIED", datafile_id, dataset_id)
            # Asynchronously update the DataFile metadata, then the Dataset
            # metadata.  Don't assume a result backend is available. Use
            # sequential tasks in a chain with immutable signatures (.si)
            # since the tasks don't really need to communicate.
            self.logger.debug("nifcert.metadata_filter Datafile[%d] starting "
                              "metadata tasks", datafile_id)
            q_name_setting = 'DATACERT_QUEUE'
            q = getattr(settings, q_name_setting, 'celery')
            chain(
                tasks.update_datafile_status.si(
                    metadata.scan_datafile_for_metadata, datafile_id,
                    False, True),
                tasks.update_dataset_status.si(
                    dataset_id, datafile_id, False, True)
            ).apply_async(queue=q)

        self.logger.debug("nifcert.metadata_filter Datafile[%d] save callback "
                          "end", datafile_id)

def make_filter():
    return DataFileMetadataFilter()

make_filter.__doc__ = DataFileMetadataFilter.__doc__
