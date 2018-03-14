"Celery tasks and related functions for setting Datafile and Dataset metadata"
import logging
from django.db import transaction
from celery.task import task
from tardis.tardis_portal.models import Schema
from tardis.tardis_portal.models import ParameterName
from tardis.tardis_portal.models import Dataset
from tardis.tardis_portal.models import DatasetParameterSet
from tardis.tardis_portal.models import DatasetParameter
from tardis.tardis_portal.models import DataFile
from tardis.tardis_portal.models import DatafileParameterSet
from tardis.tardis_portal.models import DatafileParameter

logger = logging.getLogger(__name__)

def assert_lock_datafile_failed_instance(lock_instance):
    """Set the the NIFCert DatafileCertificationLock state for lock_instance
    to FAILED

    Take care to call this outside any transaction that may rollback the lock
    state to its previous value.

    Parameters
    ----------
    lock_instance: nifcert.DatafileCertificationLock
        The DatafileCertificationLock instance to update to the failed state

    Returns
    -------
    True if the lock state was updated, otherwise False.

    """
    from nifcert.models import DatafileCertificationLock as DataCertLock
    lock.set_state(DataCertLock.FAILED)
    lock.save()
    return True
    

def assert_lock_datafile_failed(datafile_id):
    """Set the the NIFCert DatafileCertificationLock state for a particular
    DataFile to FAILED

    Creates a new DatafileCertificationLock for the Datafile if one doesn't
    exist.

    Take care to call this outside any transaction that may rollback the lock
    state to its previous value (pre-FAILED).

    Parameters
    ----------
    datafile_id: Database id of the DataFile being processed

    Returns
    -------
    True if the lock state was updated, otherwise False.

    """
    from nifcert.models import DatafileCertificationLock as DataCertLock
    lock, was_created = DataCertLock.objects.get_or_create(datafile=datafile_id)
    if lock is None: # shouldn't happen, but just in case...
        logger.error(
            "nifcert.tasks.assert_lock_datafile_failed() "
            "get_or_create() failed for DataFile[%d]", datafile_id)
        return False
    return assert_lock_datafile_failed_instance(lock)


def assert_lock_datafile_processing(datafile_id):

    """Try to acquire exclusive ownership of the NIFCert
    DatafileCertificationLock state that indicates its holder is scanning the
    DataFile for NIFCert metadata and adding it to the associated NIFCert
    DatafileParameterSet.

    This function wraps its database accesses in a transaction.  The
    caller is not required to do so.

    The lock state acquired by this function should be advanced to
    DATAFILE_METADATA_SAVED by the caller after the NIFCert metadata
    has been saved.

    The lock state acquired by this function does not need to be
    released by the caller.  It's state will be updated by the tasks
    as processing of the NIFCert metadata progresses.

    Parameters
    ----------
    datafile_id: id of the DataFile being processed

    Returns
    -------
    True if the lock state was updated, otherwise False.

    """ 
    lock_state_asserted = False
    try:
        with transaction.atomic():
            from nifcert.models import DatafileCertificationLock \
                as DataCertLock
            lock_rows = DataCertLock.objects.filter(datafile=datafile_id)
            num_lock_rows = len(lock_rows)
            if num_lock_rows != 1:
                logger.error(
                    "nifcert.tasks.assert_lock_datafile_processing() "
                    "lock query for DataFile[%d] returned %d matches, not, 1",
                    datafile_id, num_lock_rows)
                return False
            lock = lock_rows.first()
            if lock is None: # first() shouldn't fail, but just in case...
                logger.error(
                    "nifcert.tasks.assert_lock_datafile_processing() "
                    "found 1 match but couldn't fetch DataFile[%d] lock",
                    datafile_id)
                return False
            expected_state = DataCertLock.DATAFILE_VERIFIED
            if lock.state != expected_state:
                logger.debug(
                    "nifcert.tasks.assert_lock_datafile_processing() "
                    "lock.state for DataFile[%d] is %s, not expected %s",
                    datafile_id, DataCertLock.state_to_string(lock.state),
                    DataCertLock.state_to_string(expected_state))
                return False
            lock.set_state(DataCertLock.DATAFILE_PROCESSING)
            lock.save()
            lock_state_asserted = True
    except DatabaseError, e:
        logger.error(
            "nifcert.tasks.assert_lock_datafile_processing() "
            "DataCertLock handling for DataFile[%d] returned a "
            "DatabaseError:  %s", datafile_id, e)
        raise
    return lock_state_asserted


def assert_lock_datafile_metadata_saved(datafile_id):
    """Try to acquire exclusive ownership of the NIFCert
    DatafileCertificationLock state that indicates its holder has finnished
    scanning the DataFile for NIFCert metadata and adding it to the associated
    NIFCert DatafileParameterSet.

    This function does not wrap its database accesses in a transaction or
    "try" statement.  The caller should include invoke this function in the
    same "try" / transaction that updates the NIFCert DataFile metadata.

    The lock state acquired by this function does not need to be released by
    the caller.  The lock state will be updated by a subsequent NIFCert
    metadata processing task if required.

    Parameters
    ----------
    datafile_id: id of the DataFile being processed

    Returns
    -------
    True if the required lock state was acquired, otherwise False.

    """ 
    lock_state_asserted = False
    from nifcert.models import DatafileCertificationLock as DataCertLock
    lock_rows = DataCertLock.objects.filter(datafile=datafile_id)
    num_lock_rows = len(lock_rows)
    if num_lock_rows != 1:
        logger.error(
            "nifcert.tasks.assert_lock_datafile_metadata_saved() "
            "lock query for DataFile[%d] returned %d matches, not, 1",
            datafile_id, num_lock_rows)
        return False
    lock = lock_rows.first()
    if lock is None: # first() shouldn't fail, but just in case...
        logger.error(
            "nifcert.tasks.assert_lock_datafile_metadata_saved() "
            "found 1 match but couldn't fetch DataFile[%d] lock",
            datafile_id)
        return False
    expected_state = DataCertLock.DATAFILE_PROCESSING
    if lock.state != expected_state:
        logger.debug(
            "nifcert.tasks.assert_lock_datafile_metadata_saved() "
            "lock.state for DataFile[%d] is %s, not expected %s",
            datafile_id, DataCertLock.state_to_string(lock.state),
            DataCertLock.state_to_string(expected_state))
        return False
    lock.set_state(DataCertLock.DATAFILE_METADATA_SAVED)
    lock.save()
    lock_state_asserted = True
    return lock_state_asserted


def assert_lock_dataset_processing(datafile_id):
    """Try to acquire exclusive ownership of the NIFCert
    DatafileCertificationLock state that indicates the task that holds it is
    scanning the DatafileParametersets for this DataFile and its siblings in
    the same Dataset, then adding NIFCert metadata to the Dataset's
    DatasetParameterSet.

    This function wraps its database accesses in a transaction.  The
    caller is not required to do so.

    The lock state acquired by this function should be advanced to
    DATASET_METADATA_SAVED by the caller after the NIFCert metadata
    has been saved.

    The caller is responsible for explicitly managing the lock state acquired
    by this function.

    Parameters
    ----------
    datafile_id: id of the DataFile being processed

    Returns
    -------
    True if the required lock state was acquired, otherwise False.

    """ 
    lock_state_asserted = False
    try:
        with transaction.atomic():
            from nifcert.models import DatafileCertificationLock \
                as DataCertLock
            lock_rows = DataCertLock.objects.filter(datafile=datafile_id)
            num_lock_rows = len(lock_rows)
            if num_lock_rows != 1:
                logger.error(
                    "nifcert.tasks.assert_lock_dataset_processing() lock query "
                    "for DataFile[%d] returned %d matches, not, 1",
                    datafile_id, num_lock_rows)
                return False
            lock = lock_rows.first()
            if lock is None: # first() shouldn't fail, but just in case...
                logger.error(
                    "nifcert.tasks.assert_lock_dataset_processing() found 1 "
                    "match but couldn't fetch DataFile[%d] lock", datafile_id)
                return False
            expected_state = DataCertLock.DATAFILE_METADATA_SAVED
            if lock.state != expected_state:
                logger.debug(
                    "nifcert.tasks.assert_lock_dataset_processing() "
                    "lock.state for DataFile[%d] is %s, not expected %s",
                    datafile_id, DataCertLock.state_to_string(lock.state),
                    DataCertLock.state_to_string(expected_state))
                return False
            lock.set_state(DataCertLock.DATASET_PROCESSING)
            lock.save()
            lock_state_asserted = True
    except DatabaseError, e:
        logger.error(
            "nifcert.tasks.assert_lock_dataset_processing() "
            "DataCertLock handling for DataFile[%d] returned a "
            "DatabaseError:  %s", datafile_id, e)
        return False
    return lock_state_asserted


def assert_lock_dataset_metadata_saved(datafile_id):
    """Try to acquire exclusive ownership of the NIFCert
    DatafileCertificationLock state that indicates the task that holds it has
    finished scanning the DatafileParametersets for this DataFile and its
    siblings in the same Dataset, then adding NIFCert metadata to the
    Dataset's DatasetParameterSet.

    This function does not wrap its database accesses in a transaction or
    "try" statement.  The caller should include invoke this function in the
    same "try" / transaction that updates the NIFCert Dataset metadata.

    The caller is responsible for explicitly managing the lock state acquired
    by this function.

    Parameters
    ----------
    datafile_id: id of the DataFile being processed

    Returns
    -------
    True if the required lock state was acquired, otherwise False.

    """ 
    lock_state_asserted = False
    from nifcert.models import DatafileCertificationLock as DataCertLock
    lock_rows = DataCertLock.objects.filter(datafile=datafile_id)
    num_lock_rows = len(lock_rows)
    if num_lock_rows != 1:
        logger.error(
            "nifcert.tasks.assert_lock_dataset_metadata_saved() "
            "lock query for DataFile[%d] returned %d matches, not, 1",
            datafile_id, num_lock_rows)
        return False
    lock = lock_rows.first()
    if lock is None: # first() shouldn't fail, but just in case...
        logger.error(
            "nifcert.tasks.assert_lock_dataset_metadata_saved() "
            "found 1 match but couldn't fetch DataFile[%d] lock",
            datafile_id)
        return False
    expected_state = DataCertLock.DATASET_PROCESSING
    if lock.state != expected_state:
        logger.debug(
            "nifcert.tasks.assert_lock_dataset_metadata_saved() "
            "lock.state for DataFile[%d] is %s, not expected %s",
            datafile_id, DataCertLock.state_to_string(lock.state),
            DataCertLock.state_to_string(expected_state))
        return False
    lock.set_state(DataCertLock.DATASET_METADATA_SAVED)
    lock.save()
    return True


def save_datafile_parameters(schema_id, param_set, params):
    """Save a given set of parameters as DatafileParameters.

    Parameters
    ----------
    schema: tardis.tardis_portal.models.Schema
        Schema that describes the parameter names.
    param_set: tardis.tardis_portal.models.DatafileParameterSet
        DatafileParameterSet that these parameters are to be associated with.
    params: dict
        Dictionary with ParameterNames as keys and the Parameters as values.
        Parameters (values) can be singular strings/numerics or a list of
        strings/numeric. If it's a list, each element will be saved as a
        new DatafileParameter.
    Returns
    -------
    None
    """

    for paramk, paramv in params.iteritems():
        param_name = ParameterName.objects.get(schema__id=schema_id,
                                               name=paramk)
        dfp = DatafileParameter(parameterset=param_set, name=param_name)
        if paramv != "":
            if param_name.isNumeric():
                dfp.numerical_value = paramv
            else:
                dfp.string_value = paramv
            dfp.save()


def save_dataset_parameters(schema_id, param_set, params):
    """Save a given set of parameters as DatasetParameters.

    Parameters
    ----------
    schema_id: tardis.tardis_portal.models.Schema.id
        Database key for the Schema that describes the parameter names.
    param_set: tardis.tardis_portal.models.DatasetParameterSet
        DatasetParameterSet that these parameters are to be associated with.
    params: dict
        Dictionary with ParameterNames as keys and the Parameters as values.
        Parameters (values) can be singular strings/numerics or a list of
        strings/numeric. If it's a list, each element will be saved as a
        new DatasetParameter.
    Returns
    -------
    None
    """

    for paramk, paramv in params.iteritems():
        param_name = ParameterName.objects.get(schema__id=schema_id,
                                               name=paramk)
        dfp = DatasetParameter(parameterset=param_set, name=param_name)
        if paramv != "":
            if param_name.isNumeric():
                dfp.numerical_value = paramv
            else:
                dfp.string_value = paramv
            dfp.save()


def get_datafile_metadata(df, get_metadata_func, kwargs):
    """
    Extract metadata for a DataFile using a function provided

    Parameters
    ----------
    df : tardis.tardis_portal.models.DataFile
        The DataFile instance to process.
    get_metadata_func: Function
        A function that accepts a file path argument, keyword args and
        returns a dict with Schema namespaces as keys and dicts as
        values.  Each item in the dict associated with a Schema
        namespace is one DatafileParameterName (key) and its
        corresponding value for that Schema.  Returns None if an error
        occurred or no metadata was found.
    Returns
    -------
    The result of calling get_metadata_func(), or None if df's
    preferred DataFileObject is inaccessible.
    """

    meta = None
    dfo = df.get_preferred_dfo()
    if dfo:
        df_path = dfo.get_full_path()
        logger.debug("nifcert.get_datafile_metadata scanning DataFile[%d]='%s'",
                     df.id, df_path)
        # Call the function supplied as an argument to get metadata
        meta = get_metadata_func(df_path, **kwargs)
        logger.debug("nifcert.get_datafile_metadata scanned  DataFile[%d]='%s'",
                     df.id, df_path)
    return meta


def get_dataset_metadata(dataset_id):
    """Scan the NIF_certified status of all Datafiles in a Dataset and
    return the NIF_Certified metadata for the Dataset as a whole.

    Only Datasets associated with an Instrument that has
    NIF_certification_enabled=yes will generate metadata.

    To be valid, a Dataset must have exactly one Bruker Biospec
    Datafile with NIFCert=yes (a valid .PvDataSets file).  Any other
    Bruker Biospec Datafiles will make the Dataset NIFCert=no.  The
    Dataset may have any number of DataFiles that are not Bruker
    Biospec files but they must not have NIFCert=yes.

    Parameters
    ----------
    dataset_id:
        Database id of the Dataset to scan.

    Returns
    -------
    dict:
        A dictionary with
        nifcert.schemas.dataset.nifcert.SCHEMA_NAMESPACE as its key
        and as its value, a dictionary of parameter names and values.
        nifcert.schemas.dataset.nifcert.CERTIFIED_NAME is the only key
        returned in the parameters dictionary.
        Returns an empty outer dictionary if none of the DataFiles in
        the Dataset have NIF Certified metadata.

    """
    valid = True
    cert_file_id = -1

    # Dataset must only have one Bruker BioSpec file
    from nifcert.brukerbiospec.files import FILE_EXT_LOWER
    bruker_files = DataFile.objects.filter(
        dataset=dataset_id,
        filename__iendswith=FILE_EXT_LOWER)
    num_bruker_files = len(bruker_files)
    valid = num_bruker_files == 1
    if valid:
        cert_file_id = bruker_files.first().id
    logger.debug("nifcert.get_dataset_metadata %d Bruker files in dataset",
                 num_bruker_files)

    # Exactly one DataFile in the Dataset must have NIFCert=yes metadata
    import nifcert.schemas.datafile.nifcert as ndfs
    import nifcert.schemas.dataset.nifcert as ndss
    if valid:
        cert_file_params = DatafileParameter.objects.filter(
            string_value__exact=ndfs.CERTIFIED_YES_VALUE,
            name__name__exact=ndfs.CERTIFIED_NAME,
            name__schema__namespace__exact=ndfs.SCHEMA_NAMESPACE,
            name__schema__name__exact=ndfs.SCHEMA_NAME,
            parameterset__datafile__dataset__id=dataset_id)
        num_cert_file_params = len(cert_file_params)
        logger.debug("nifcert.get_dataset_metadata %d datafile nifcert=yes "
                     "params matched", num_cert_file_params)

        # The NIFCert=yes DataFile must be the one Bruker BioSpec file found
        if num_cert_file_params == 1:
            cert_params_file_id = (
                cert_file_params.first().parameterset.datafile.id)
            valid = cert_file_id == cert_params_file_id
            logger.debug("nifcert.get_dataset_metadata "
                         "Datafile[%d] NIFCert, Datafile[%d] NIFCert=yes param",
                         cert_file_id, cert_params_file_id)
        else:
            valid = False

    if valid:
        value = ndss.CERTIFIED_YES_VALUE
    else:
        value = ndss.CERTIFIED_NO_VALUE
    meta = dict()
    if value:
        nif_meta = dict()
        nif_meta[ndss.CERTIFIED_NAME] = value
        meta[ndss.SCHEMA_NAMESPACE] = nif_meta

    logger.debug("nifcert.get_dataset_metadata Dataset[%d] %s=%s",
                 dataset_id, ndss.CERTIFIED_NAME, value)
    return meta


def set_datafile_metadata(datafile, metadata, replace_metadata):
    """
    Set the nifcert metadata for a DataFile.

    This app is solely responsible for managing metadata associated
    with its schema namespaces.  Rather than attempting to check,
    merge and re-use existing DatafileParameterSets and
    DatafileParameters, delete and rebuild them, for consistency.

    Parameters
    ----------
    datafile: DataFile
        The DataFile having its metadata updated.

    metadata: dict
        A dictionary of dictionaries containing the new metadata.
        The outer dict has Schema namespaces for keys and dictionaries
        as values.  Each Schema's dictionary contains ParameterName keys
        with associated parameter values.

    replace_metadata: bool
        If replace_metadata is True, and any full, partial or empty
        NIFCert of DICOM Stats DatafileParameterSet is currently
        associated with the DataFile, this function will return without
        modifying any data.

    Returns
    -------
    If successful, returns the number of DatafileParameterSets added
    (always a positive number if there is metadata to add).
    If replace_metadata is False and prevents metadata being added,
    returns the negated number of obstructing DatafileParameterSets.
    Returns zero if an error occurs and no metadata was changed.
    """
    if not datafile:
        return 0

    # Validate namespace keys in metadata
    import nifcert.schemas as ns
    schema_namespaces = ns.get_datafile_schema_tree().keys()
    if set(metadata.keys()) > set(schema_namespaces):
        logger.error("nifcert.set_datafile_metadata DataFile[%d] "
                     "%d database Schemas don't include all %d found in "
                     "metadata dictionary",
                     len(schema_namespaces), len(metadata))
        return 0

    # Fetch Schema instances for (re)creating ParameterSets
    schemas = dict()
    schema_rows = Schema.objects.filter(namespace__in=schema_namespaces)
    for s in schema_rows:
        schemas[s.namespace] = s
    # There must be a database Schema for each namespace key in metadata
    if set(schemas.keys()) != set(schema_namespaces):
        logger.error("nifcert.set_datafile_metadata DataFile[%d] "
                     "expected %d Schemas, found %d in database",
                     datafile.id, len(schema_namespaces), len(schema_rows))
        return 0

    # Delete existing ParameterSets (and their parameters; cascaded)
    datafile_param_sets = (
        DatafileParameterSet.objects.filter(
            schema__namespace__in=schema_namespaces,
            datafile=datafile))
    num_param_sets = len(datafile_param_sets)
    logger.debug("DataFile[%d] has %d existing DatasetParameterSets",
                 datafile.id, num_param_sets)
    if num_param_sets:
        if replace_metadata:
            # datafile_param_sets.delete() doesn't call any delete() method
            # that may in future be added to DatafileParameterSet, so
            # delete() each row.  Parameters in set are cascade deleted.
            [ps.delete() for ps in datafile_param_sets]
        else:
            return -num_param_sets

    num_added = 0
    for schema_name in metadata.keys():
        schema = schemas[schema_name]
        ps = DatafileParameterSet(schema=schema, datafile=datafile)
        ps.save()
        num_added += 1
        logger.debug("  - Saving DataFile parameters for: "
                     "DataFile[%d]    Schema[%d]:'%s='%s'",
                     datafile.id, schema.id, schema.name, schema.namespace)
        save_datafile_parameters(schema.id, ps, metadata[schema_name])
    return num_added


def set_dataset_metadata(dataset_id, metadata, replace_metadata):
    """
    Set the nifcert metadata for a Dataset.

    This app is solely responsible for managing metadata associated
    with its schema namespaces.  Rather than attempting to check,
    merge and re-use existing DatasetParameterSets and
    DatasetParameters, delete and rebuild them, for consistency.

    Parameters
    ----------
    dataset_id: tardis.tardis_portal.models.Dataset.id
        The Dataset having its metadata updated.

    metadata: dict
        A dictionary of dictionaries containing the new metadata.
        The outer dict has Schema namespaces for keys and dictionaries
        as values.  Each Schema's dictionary contains ParameterName keys
        with associated parameter values.

    replace_metadata: bool
        If replace_metadata is True, and any full, partial or empty
        NIFCert of DICOM Stats DatasetParameterSet is currently
        associated with the Dataset, this function will return without
        modifying any data.

    Returns
    -------
    If successful, returns the number of DatasetParameterSets added
    (currently 1, always a positive number if there is metadata to add).
    If replace_metadata is False and prevents metadata being added,
    returns the number of obstructing DatasetParameterSets, negated.
    Returns zero if an error occurs and no metadata was changed.
    """
    dataset_rows = Dataset.objects.filter(id=dataset_id)
    num_dataset_rows = len(dataset_rows)
    if num_dataset_rows != 1:
        logger.error("nifcert.set_dataset_metadata Dataset[%d] "
                     "couldn't fetch unique Dataset by id, found %d",
                     dataset_id, num_dataset_rows)
        return 0
    dataset = dataset_rows.first()

    # Validate namespace keys in metadata
    import nifcert.schemas as ns
    schema_namespaces = ns.get_dataset_schema_tree().keys()
    if set(metadata.keys()) != set(schema_namespaces):
        logger.error("nifcert.set_dataset_metadata Dataset[%d] "
                     "expected %d Schemas, found %d in metadata dictionary",
                     dataset_id, len(schema_namespaces), len(metadata))
        return 0

    # Fetch Schema instances for (re)creating ParameterSets
    schemas = dict()
    schema_rows = Schema.objects.filter(namespace__in=schema_namespaces)
    for s in schema_rows:
        schemas[s.namespace] = s
    # There must be a database Schema for each namespace key in metadata
    if set(schemas.keys()) != set(schema_namespaces):
        logger.error("nifcert.set_dataset_metadata Dataset[%d] "
                     "expected %d Schemas, found %d in database",
                     dataset_id, len(schema_namespaces), len(schema_rows))
        return 0

    # Delete existing ParameterSets (and their parameters; cascaded)
    dataset_param_sets = (
        DatasetParameterSet.objects.filter(
            schema__namespace__in=schema_namespaces,
            dataset__id=dataset_id))
    num_param_sets = len(dataset_param_sets)
    logger.debug("Dataset[%d] has %d existing DatasetParameterSets",
                 dataset_id, num_param_sets)
    if num_param_sets:
        if replace_metadata:
            # dataset_param_sets.delete() doesn't call any delete() method
            # that may in future be added to DatasetParameterSet, so
            # delete() each row.  Parameters in set are cascade deleted.
            [ps.delete() for ps in dataset_param_sets]
        else:
            return -num_param_sets

    num_added = 0
    for schema_name in metadata.keys():
        schema = schemas[schema_name]
        ps = DatasetParameterSet(schema=schema, dataset=dataset)
        ps.save()
        num_added += 1
        logger.debug("  - Saving Dataset parameters for: "
                     "Dataset[%d]    Schema[%d]:'%s='%s'",
                     dataset_id, schema.id, schema.name, schema.namespace)
        save_dataset_parameters(schema.id, ps, metadata[schema_name])
    return num_added


@task(name="nifcert.update_datafile_status", bind=True, ignore_result=True)
def update_datafile_status(self,
                           get_metadata_func,
                           datafile_id,
                           check_nifcert_instrument=True,
                           replace_file_metadata=True,
                           **kwargs):
    """Extract metadata from a DataFile using a function provided as a
    parameter and save the outputs as DatafileParameters.

    Note: the task decorator parameter list above uses both bind=True
    and ignore_result=True in case someone forgets to use an immutable
    signature (.si) when scheduling this task.  Do this in case a Celery
    result backend is not available / configured / desirable.

    Parameters
    ----------
    get_metadata_func: Function
        Function to extract metadata from a file. Function must have
        input_file_path as an argument e.g.:
        def meta_proc(input_file_path, **kwargs):
            ...
        It must return a dict containing ParameterNames as keys and the
        Parameters to be saved as values. Parameters (values) can be singular
        strings/numerics or a list of strings/numeric. If it's a list, each
        element will be saved as a new DatafileParameter.
    datafile_id: tardis.tardis_portal.models.DataFile.id
        Database id of the DataFile instance to process.
    check_nifcert_instrument: boolean (default: True)
        If False, don't check whether the DataFile came from an Instrument
        with NIF_certification_enabled=yes and always add NIFCert metadata.
        If True, check the instrument is enabled=yes before adding metadata.
    replace_file_metadata: boolean (default: True)
        WARNING: setting this to False may leave the metadata for the
        DataFile and its containing Dataset in an inconsistent state.
        Expert use only.
        If True, any existing DataFile ParameterSets / metadata this
        code maintains for the DataFile will be deleted, then replaced
        with freshly computed metadata.
        If False, and there is existing metadata for the DataFile
        maintained by this code, that metadata will be left as-is.
        If False, and there is no existing metadata for the DataFile
        maintained by this code, that metadata will be computed and
        saved.

    Returns
    -------
    None

    """
    from nifcert import metadata
    logger.debug("nifcert.update_datafile_status begin DataFile[%d]",
                 datafile_id)

    # TODO: refactor function so that return False => terminate task chain
    # (once in stead of multiple occurrentces).  Use same code block to set
    # DataCertLock.state to FAILED.

    if check_nifcert_instrument: # checked by metadata_filter, but re-check
        if not metadata.is_datafile_instrument_nifcert(datafile_id):
            logger.debug("nifcert.update_datafile_status DataFile[%d] is not "
                         "associated with a " "NIF_certification_enabled "
                         "instrument", datafile_id)
            self.request.callbacks = None # terminate task chain
            return

    datafile_matches = DataFile.objects.filter(id=datafile_id)
    num_matches = len(datafile_matches)
    if num_matches != 1:        # shouldn't happen, but just in case
        logger.error("nifcert.update_datafile_status couldn't fetch unique "
                     "DataFile[%d], found %d matches",
                     datafile_id, num_datafiles)
        assert_lock_datafile_failed(datafile_id)
        self.request.callbacks = None # terminate task chain
        return
    datafile = datafile_matches.first()
    if datafile is None:
        logger.error("nifcert.update_datafile_status found 1 match but "
                     "couldn't fetch DataFile[%d]", datafile_id)
        assert_lock_datafile_failed(datafile_id)
        self.request.callbacks = None # terminate task chain
        return

    meta = None
    terminate_task_chain = True
    logger.debug("nifcert.update_datafile_status asserting lock "
                 "DATAFILE_PROCESSING for DataFile[%d]", datafile_id)

    if not assert_lock_datafile_processing(datafile_id):
        logger.error("nifcert.update_datafile_status didn't acquire lock "
                     "DATAFILE_PROCESSING for DataFile[%d], skipping metadata",
                     datafile_id)
    else:
        logger.debug("nifcert.update_datafile_status asserted lock "
                     "DATAFILE_PROCESSING for DataFile[%d]", datafile_id)
        try:
            with transaction.atomic():
                meta = (
                    get_datafile_metadata(datafile, get_metadata_func, kwargs))
                # All files from a NIFCert instrument get NIFCert metadata
                if meta == None or len(meta) == 0:
                    meta = metadata.get_not_nifcert_metadata_value()
                metadata_saved = False
                if meta:
                    metadata_saved = (
                        set_datafile_metadata(
                            datafile, meta, replace_file_metadata))
                logger.debug("nifcert.update_datafile_status %s metadata for "
                             "DataFile[%d]",
                             "saved" if metadata_saved else "didn't save",
                             datafile_id)
                if metadata_saved:
                    if assert_lock_datafile_metadata_saved(datafile_id):
                        terminate_task_chain = False
                        logger.debug("nifcert.update_datafile_status asserted "
                                     "lock DATAFILE_METADATA_SAVED for "
                                     "DataFile[%d]", datafile_id)
        except Exception, e:
            logger.warning("nifcert.update_datafile_status Exception caught "
                           "whilst processing DataFile[%d]:\n  exception='%s'",
                           datafile_id, e)
            assert_lock_datafile_failed_instance(datafile_id)
            # Propagate important exceptions like Celery.exceptions.Retry
            raise

    if terminate_task_chain:
        logger.error("nifcert.update_datafile_status error saving NIFCert "
                     "metadata for DataFile[%d], asserting FAILED lock and "
                     "terminating NIFCert metadata task chain", datafile_id)
        assert_lock_datafile_failed_instance(datafile_id)
        self.request.callbacks = None # terminate task chain
        return

    logger.debug("nifcert.update_datafile_status saved NIFCert metadata for "
                 "DataFile[%d] and asserted DATAFILE_METADATA_SAVED lock",
                 datafile_id)


@task(name="nifcert.update_dataset_status", ignore_result=True)
def update_dataset_status(dataset_id,
                          datafile_id=-1,
                          check_nifcert_instrument=True,
                          replace_dataset_metadata=True,
                          **kwargs):
    """Update the NIRCert metadata for a Dataset, usually in response to
    a new DataFile being added.

    Only Datasets associated with an Instrument that has
    NIF_certification_enabled=yes will be updated.

    See nifcert.tasks.get_dataset_metadata() for details of how the
    metadata is computed.

    Parameters
    ----------
    dataset_id: tardis.tardis_portal.models.Dataset.id
        Database id of the Dataset to process.
    datafile_id: tardis.tardis_portal.models.DataFile.id
        Database id of the DataFile to process.
    check_nifcert_instrument: boolean (default: True)
        If False, don't check whether the Dataset came from an Instrument
        with NIF_certification_enabled=yes and always add NIFCert metadata.
        If True, check the instrument is enabled=yes before adding metadata.
    replace_dataset_metadata: boolean (default: True)
        WARNING: setting this to False may leave the metadata for the
        DataFiles and their containing Dataset in an inconsistent state.
        The only time this is normally done is when processing batches
        of Datafiles from the same Dataset.
        If True, any existing Dataset ParameterSets / metadata this
        code maintains for the DataFile's Dataset will be deleted,
        then replaced with freshly computed metadata.
        If False, and there is existing metadata for the DataFile's
        Dataset maintained by this code, that metadata will be left
        as-is.
        If False, and there is no existing metadata for the DataFile's
        Dataset, the metadata will be created.

    Returns
    -------
    None

    """
    logger.debug("nifcert.update_dataset_status begin Dataset[%d] DataFile[%d]",
                 dataset_id, datafile_id)

    if check_nifcert_instrument:
        from nifcert import metadata
        if not metadata.is_datafile_instrument_nifcert(datafile_id):
            logger.debug("nifcert.update_dataset_status Dataset[%d] "
                         "DataFile[%d] is not associated with a "
                         "NIF_certification_enabled instrument", dataset_id)
            return

    # Ensure only one instance of the task for updating this DataFile's
    # Dataset is ever run.

    metadata_saved = False
    logger.debug("nifcert.update_dataset_status asserting lock "
                 "DATASET_PROCESSING for Dataset[%d] DataFile[%d]",
                 dataset_id, datafile_id)

    if not assert_lock_dataset_processing(datafile_id):
        logger.error("nifcert.update_dataset_status didn't acquire lock "
                     "DATASET_PROCESSING for Dataset[%d] DataFile[%d], "
                     "skipping metadata", dataset_id, datafile_id)
    else:
        logger.debug("nifcert.update_dataset_status asserted lock "
                     "DATASET_PROCESSING for Dataset[%d] DataFile[%d]",
                     dataset_id, datafile_id)
        try:
            with transaction.atomic():
                meta = get_dataset_metadata(dataset_id)
                if meta:
                    n = set_dataset_metadata(dataset_id, meta,
                                             replace_dataset_metadata)
                    if n > 0:
                        if assert_lock_dataset_metadata_saved(datafile_id):
                            metadata_saved = True
        except Exception, e:
            logger.warning("nifcert.update_dataset_status Exception caught "
                           "whilst processing Dataset[%d] DataFile[%d]:\n  "
                           "exception='%s'", dataset_id, datafile_id, e)
            assert_lock_datafile_failed(datafile_id)
            # Propagate important exceptions like Celery.exceptions.Retry
            raise

    if not metadata_saved:
        logger.debug("nifcert.update_dataset_status error saving NIFCert "
                     "metadata for Dataset[%d] DataFile[%d], asserting FAILED "
                     "lock", dataset_id, datafile_id)
        assert_lock_datafile_failed(datafile_id)

    logger.debug("nifcert.update_dataset_status saved NIFCert metadata for "
                 "Dataset[%d] DataFile[%d] and asserted DATASET_METADATA_SAVED "
                 "lock", dataset_id, datafile_id)
