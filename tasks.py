"Celery tasks and related functions for setting Datafile and Dataset metadata"
import logging
from django.core.cache import caches
from django.db import transaction
from celery.task import task
from tardis.tardis_portal.models import Schema
from tardis.tardis_portal.models import ParameterName
from tardis.tardis_portal.models import Dataset
from tardis.tardis_portal.models import DatasetParameterSet
from tardis.tardis_portal.models import DatasetParameter
from tardis.tardis_portal.models import DatafileParameterSet
from tardis.tardis_portal.models import DatafileParameter

from nifcert import trudat

logger = logging.getLogger(__name__)

# Locks are used to prevent concurrent access by Celery workers.
# Choose timeouts proportional to the amount of work being done,
# allowing for very heavily loaded machines.

DATASET_LOCK_TIMEOUT = 60 * 5  # Lock expires in 5 minutes
DATAFILE_LOCK_TIMEOUT = 60 * 1  # Lock expires in 1 minute

# Default cache name used for Celery locks.

DEFAULT_CELERY_LOCK_CACHE = 'celery-locks'


def generate_lockid(object_type, object_id):
    """Return a lock id for database operations"""
    return "tardis_portal_nifcert_lock_{}_{}".format(object_type, object_id)


def acquire_dataset_lock(dataset_id, cache_name=DEFAULT_CELERY_LOCK_CACHE):
    """Lock a dataset to prevent filters from running mutliple times on
    the same dataset in quick succession.

    Parameters
    ----------
    dataset_id: int
        ID of the dataset
    cache_name: string (default = "celery-locks")
        Optional specify the name of the lock cache to store this lock in
    Returns
    -------
    locked: boolean
        Boolean representing whether dataset is locked

    """
    lockid = generate_lockid('dataset', dataset_id)
    cache = caches[cache_name]
    return cache.add(lockid, 'true', DATASET_LOCK_TIMEOUT)


def release_dataset_lock(dataset_id, cache_name=DEFAULT_CELERY_LOCK_CACHE):
    """Release the lock on a Dataset from acquire_dataset_lock().

    Parameters
    ----------
    dataset_id: int
        ID of the dataset
    cache_name: string (default = "celery-locks")
        Optional specify the name of the lock cache to store this lock in

    """
    lockid = generate_lockid('dataset', dataset_id)
    cache = caches[cache_name]
    cache.delete(lockid)


def acquire_datafile_lock(datafile_id, cache_name=DEFAULT_CELERY_LOCK_CACHE):
    """Lock a datafile to prevent filters from running mutliple times on
    the same datafile in quick succession.

    Parameters
    ----------
    datafile_id: int
        ID of the datafile
    cache_name: string (default = "celery-locks")
        Optional specify the name of the lock cache to store this lock in
    Returns
    -------
    locked: boolean
        Boolean representing whether datafile is locked

    """
    lockid = generate_lockid('datafile', datafile_id)
    cache = caches[cache_name]
    return cache.add(lockid, 'true', DATAFILE_LOCK_TIMEOUT)


def release_datafile_lock(datafile_id, cache_name=DEFAULT_CELERY_LOCK_CACHE):
    """Release the lock on a Datafile from acquire_datafile_lock().

    Parameters
    ----------
    datafile_id: int
        ID of the datafile
    cache_name: string (default = "celery-locks")
        Optional specify the name of the lock cache to store this lock in

    """
    lockid = generate_lockid('datafile', datafile_id)
    cache = caches[cache_name]
    cache.delete(lockid)


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
    def save_df_param(paramk, paramv):
        """Save a value (or list of values) as a DatafileParameter
        Parameters
        ----------
        paramk: string for ParameterName
            name of the DatafileParameter to be saved
        paramv: any valid DatafileParameter (Parameter) value type
            value for the DatafileParameter to be saved
        Returns
        -------
        None
        """
        param_name = ParameterName.objects.get(schema__id=schema_id,
                                               name=paramk)
        dfp = DatafileParameter(parameterset=param_set, name=param_name)
        if paramv != "":
            if param_name.isNumeric():
                dfp.numerical_value = paramv
            else:
                dfp.string_value = paramv
            dfp.save()

    for paramk, paramv in params.iteritems():
        if isinstance(paramv, list):
            [save_df_param(paramk, v) for v in paramv]
        else:
            save_df_param(paramk, paramv)


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

    def save_ds_param(paramk, paramv):
        """Save a value (or list of values) as a DatafileParameter
        Parameters
        ----------
        paramk: string for ParameterName
            name of the DatafileParameter to be saved
        paramv: any valid DatafileParameter (Parameter) value type
            value for the DatafileParameter to be saved
        Returns
        -------
        None
        """
        param_name = ParameterName.objects.get(schema__id=schema_id,
                                               name=paramk)
        dfp = DatasetParameter(parameterset=param_set, name=param_name)
        if paramv != "":
            if param_name.isNumeric():
                dfp.numerical_value = paramv
            else:
                dfp.string_value = paramv
            dfp.save()

    for paramk, paramv in params.iteritems():
        if isinstance(paramv, list):
            [save_ds_param(paramk, v) for v in paramv]
        else:
            save_ds_param(paramk, paramv)


def get_datafile_metadata(df, get_metadata_func, kwargs):
    """Extract metadata for a Datafile using a function provided

    Parameters
    ----------
    df : tardis.tardis_portal.models.Datafile
        The Datafile instance to process.
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
        logger.debug("nifcert.get_datafile_metadata scanning Datafile[%d]='%s'",
                     df.id, df_path)
        # Call the function supplied as an argument to get metadata
        meta = get_metadata_func(df_path, **kwargs)
        logger.debug("nifcert.get_datafile_metadata scanned  Datafile[%d]='%s'",
                     df.id, df_path)
    return meta


def get_dataset_metadata(dataset_id):
    """Scan the NIF_certified status of all Datafiles in a Dataset and
    return the NIF_Certified metadata for the Dataset as a whole.

    Any Datafile in a Dataset with NIF_certified=no makes the Dataset
    NIF_certified=no; otherwise one or more Datafiles with
    NIF_certified=yes makes the Dataset NIF_certified=yes; otherwise
    the Dataset has no NIF_certified metadata.

    Parameters
    ----------
    dataset_id:
        Database id of the Dataset to scan.

    Returns
    -------
    dict:
        A dictionary with trudat.NIFCERT_DATASET_NAMESPACE as its key
        and as its value, a dictionary of parameter names and values.
        trudat.NIFCERT_DATASET_CERTIFIED_KEY is the only key returned
        in the parameters dictionary.
        Returns an empty outer dictionary if

    """

    # TODO: use QuerySet anotate & aggregate to condense work into one query?
    #
    # .annotate(num_yes=
    #           Count(string_value__exact=trudat.NIFCERT_IS_CERTIFIED_VALUE,
    #                 distinct=True),
    #           num_no=
    #           Count(string_value__exact=trudat.NIFCERT_NOT_CERTIFIED_VALUE))

    dataset_certified_file_params = (
        DatafileParameter.objects.filter(
            parameterset__datafile__dataset__id=dataset_id,
            name__name=trudat.NIFCERT_DATAFILE_CERTIFIED_KEY))
    num_files_certified = 0
    num_files_not_certified = 0
    i = 0
    for p in dataset_certified_file_params:
        if p.string_value == trudat.NIFCERT_IS_CERTIFIED_VALUE:
            num_files_certified += 1
        elif p.string_value == trudat.NIFCERT_NOT_CERTIFIED_VALUE:
            num_files_not_certified += 1
        logger.debug("nifcert.process_meta     param match[%d] : "
                     "Datafile[%d,%3d] '%s'='%s'",
                     i, p.parameterset.datafile.dataset.id,
                     p.parameterset.datafile.id, p.name, p.string_value)
        i += 1

    if num_files_not_certified > 0:
        value = trudat.NIFCERT_NOT_CERTIFIED_VALUE
    elif num_files_certified > 0:
        value = trudat.NIFCERT_IS_CERTIFIED_VALUE
    else:
        value = None
    meta = dict()
    if value:
        nif_meta = dict()
        nif_meta[trudat.NIFCERT_DATASET_CERTIFIED_KEY] = value
        meta[trudat.NIFCERT_DATASET_NAMESPACE] = nif_meta

    logger.debug("nifcert.process_meta categorised Datafiles in Dataset[%d]:   "
                 "%s=%d  %s=%d  other=%d  total=%d",
                 dataset_id,
                 trudat.NIFCERT_IS_CERTIFIED_VALUE, num_files_certified,
                 trudat.NIFCERT_NOT_CERTIFIED_VALUE, num_files_not_certified,
                 i - (num_files_certified + num_files_not_certified), i)
    return meta


def set_datafile_metadata(datafile, metadata, replace_metadata):
    """Set the nifcert metadata for a Datafile.

    This app is solely responsible for managing metadata associated
    with its schema namespaces.  Rather than attempting to check,
    merge and re-use existing DatafileParameterSets and
    DatafileParameters, delete and rebuild them, for consistency.

    Parameters
    ----------
    datafile: Datafile
        The Datafile having its metadata updated.

    metadata: dict
        A dictionary of dictionaries containing the new metadata.
        The outer dict has Schema namespaces for keys and dictionaries
        as values.  Each Schema's dictionary contains ParameterName keys
        with associated parameter values.

    replace_metadata: bool
        If replace_metadata is True, and any full, partial or empty
        NIFCert of DICOM Stats DatafileParameterSet is currently
        associated with the Datafile, this function will return without
        modifying any data.

    Returns
    -------
    If successful, returns the number of DatafileParameterSets added
    (currently 2, always a positive number if there is metadata to add).
    If replace_metadata is False and prevents metadata being added,
    returns the number of obstructing DatafileParameterSets, negated.
    Returns zero if an error occurs and no metadata was changed.
    """
    if not datafile:
        return 0

    # Validate namespace keys in metadata
    schema_namespaces = trudat.NAMESPACE_TREE[Schema.DATAFILE].keys()
    if set(metadata.keys()) != set(schema_namespaces):
        logger.error("nifcert.set_datafile_metadata Datafile[%d] "
                     "expected %d Schemas, found %d in metadata dictionary",
                     len(schema_namespaces), len(metadata))
        return 0

    # Fetch Schema instances for (re)creating ParameterSets
    schemas = dict()
    schema_rows = Schema.objects.filter(namespace__in=schema_namespaces)
    for s in schema_rows:
        schemas[s.namespace] = s
    # There must be a database Schema for each namespace key in metadata
    if set(schemas.keys()) != set(schema_namespaces):
        logger.error("nifcert.set_datafile_metadata Datafile[%d] "
                     "expected %d Schemas, found %d in database",
                     len(schema_namespaces), len(schema_rows))
        return 0

    # Delete existing ParameterSets (and their parameters; cascaded)
    datafile_param_sets = (
        DatafileParameterSet.objects.filter(
            schema__namespace__in=schema_namespaces,
            datafile=datafile))
    num_param_sets = len(datafile_param_sets)
    logger.debug("Datafile[%d] has %d existing DatasetParameterSets",
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
        logger.debug("  - Saving Datafile parameters for: "
                     "Datafile[%d]    Schema[%d]:'%s='%s'",
                     datafile.id, schema.id, schema.name, schema.namespace)
        save_datafile_parameters(schema.id, ps, metadata[schema_name])
    return num_added


def set_dataset_metadata(dataset, metadata, replace_metadata):
    """Set the nifcert metadata for a Dataset.

    This app is solely responsible for managing metadata associated
    with its schema namespaces.  Rather than attempting to check,
    merge and re-use existing DatasetParameterSets and
    DatasetParameters, delete and rebuild them, for consistency.

    Parameters
    ----------
    dataset: Dataset
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
    if not dataset:
        return 0

    # Validate namespace keys in metadata
    schema_namespaces = trudat.NAMESPACE_TREE[Schema.DATASET].keys()
    if set(metadata.keys()) != set(schema_namespaces):
        logger.error("nifcert.set_dataset_metadata Dataset[%d] "
                     "expected %d Schemas, found %d in metadata dictionary",
                     dataset.id, len(schema_namespaces), len(metadata))
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
                     dataset.id, len(schema_namespaces), len(schema_rows))
        return 0

    # Delete existing ParameterSets (and their parameters; cascaded)
    dataset_param_sets = (
        DatasetParameterSet.objects.filter(
            schema__namespace__in=schema_namespaces,
            dataset=dataset))
    num_param_sets = len(dataset_param_sets)
    logger.debug("Dataset[%d] has %d existing DatasetParameterSets",
                 dataset.id, num_param_sets)
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
                     dataset.id, schema.id, schema.name, schema.namespace)
        save_dataset_parameters(schema.id, ps, metadata[schema_name])
    return num_added


@task(name="nifcert.process_meta", ignore_result=True)
def process_meta(get_metadata_func, df,
                 replace_file_metadata=True,
                 replace_dataset_metadata=True,
                 **kwargs):
    """Extract metadata from a Datafile using a provided function and save the
    outputs as DatafileParameters.

    This may also trigger an update of the metadata for the Dataset
    containing the Datafile.

    Computing the Dataset's NIF_certified metadata, will check the
    NIF_certified metadata for all Datafiles in df's Dataset.
    The new status will be 'no' if any Datafile has NIF_certified='no',
    otherwise 'yes' if any Datafile has NIF_certified='yes',
    otherwise there is no NIF_certified status for the Dataset.

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
    df: tardis.tardis_portal.models.Datafile
        Datafile instance to process.
    replace_file_metadata: boolean (default: True)
        WARNING: setting this to False may leave the metadata for df and
        its containing DataSet in an inconsistent state.  Expert use only.
        If True, any existing Datafile ParameterSets / metadata this
        code maintains for df will be deleted, then replaced with freshly
        computed metadata.
        If False, and there is existing metadata for df maintained by
        this code, that metadata and any metadata this code maintains for
        df's DataSet will be left as-is.
        If False, and there is no existing metadata for df maintained by
        this code, that metadata will be computed and saved, then any
        metadata this code maintains for df's DataSet will be created or
        updated, provided replace_dataset_metadata permits that.
    replace_dataset_metadata: boolean (default: True)
        WARNING: setting this to False may leave the metadata for df and
        its containing DataSet in an inconsistent state.  The only time
        this is normally done is when processing batches of Datafiles from
        the same DataSet.  To prevent needless recomputation, only the
        last file in the batch needs to compute the containing DataSet's
        metadata (True for the last file, False for all the others).
        If True, any existing Dataset ParameterSets / metadata this
        code maintains for df's Dataset will be deleted, then replaced
        with freshly computed metadata.
        If False, and there is existing metadata for df's Dataset
        maintained by this code, that metadata will be left as-is.
        If False, and there is no existing metadata for df's DataSet
        will be created or updated.
    Returns
    -------
    None
    """

    # NOTE: be very careful with locking and exceptions.  Catching all
    # Exceptions to handle database exceptions like DoesNotExist or
    # MultipleObjectsReturned may interfere with Celery's use of
    # Exceptions.  Example: celery.app.task.retry() throws
    # celery.exceptions.Retry to signal a worker to retry a task (see
    # others in the docs for celery.exceptions).

    # TODO: Celery docs recommend passing df as an id not a model
    # instance.  Replace the original mytardisbf design.

    meta = None
    held_datafile_lock = False
    logger.debug("nifcert.process_meta locking Datafile[%d]", df.id)
    if acquire_datafile_lock(df.id):
        held_datafile_lock = True
        logger.debug("nifcert.process_meta locked Datafile[%d]", df.id)

        try:
            with transaction.atomic():
                meta = get_datafile_metadata(df, get_metadata_func, kwargs)
                if meta != None and len(meta) == 0:
                    # Recognised file type, but bad contents.  Mark as invalid.
                    meta = metadata.get_non_nifcert_metadata()
                if meta:
                    set_datafile_metadata(df, meta, replace_file_metadata)
                    logger.debug("nifcert.process_meta updated metadata for "
                                 "Datafile[%d]", df.id)
        except Exception, e:
            logger.warning("nifcert.process_meta Exception caught whilst "
                           "processing Datafile[%d]:\n  exception='%s'",
                           df.id, e)
            # Propagate important exceptions like Celery's retry() / Retry()
            raise
        finally:
            release_datafile_lock(df.id)

    if not held_datafile_lock:
        logger.debug("nifcert.process_meta didn't acquire Datafile[%d] lock, "
                     "skipping Dataset update", df.id)
        return
    if meta == None:
        logger.debug("nifcert.process_meta no metadata to save for "
                     "Datafile[%d]", df.id)
        return

    # TODO: split Dataset metadata update into a separate task

    dataset_id = df.dataset.id  # TODO: pass in via task parameter

    logger.debug("nifcert.process_meta locking Dataset[%d]", dataset_id)
    if acquire_dataset_lock(dataset_id):
        logger.debug("nifcert.process_meta locked Dataset[%d]", dataset_id)
        try:
            with transaction.atomic():
                meta = get_dataset_metadata(dataset_id)
                if not meta:
                    # TODO: allow NIF Certified status to be deleted?
                    return
                set_dataset_metadata(df.dataset, meta, replace_dataset_metadata)

            logger.debug("nifcert.process_meta finished Dataset[%d]",
                         dataset_id)

        except Exception, e:
            logger.warning("nifcert.process_meta Exception caught whilst "
                           "processing Dataset:\n  '%s'", e)
            # Propagate important exceptions like Celery's retry() / Retry()
            raise
        finally:
            release_dataset_lock(dataset_id)
            logger.debug("nifcert.process_meta unlocked Dataset[%d]",
                         dataset_id)
