import logging
from nifcert.brukerbiospec.metadata.dicom import find_dicom_files
from nifcert.brukerbiospec.files import is_bruker_biospec_file_path

logger = logging.getLogger(__name__)


def is_datafile_instrument_nifcert(datafile_id):
    """
    Return True if a Datafile is associated with a
    NIF_certification_enabled instrument.

    Parameters
    ----------
    datafile_id: tardis.tardis_portal.models.Datafile.id

    Returns
    -------
    True if the DataFile specified by datafile_id exists, has an
    Instrument associated with it and the Instrument has its
    NIF_certification_enabled metadata parameter set to "yes" (see
    nifcert.schemas.instrument.nifcert for details). Returns False
    otherwise.

    In the unlikely event there are multiple instances of the
    parameter (multiple query matches), the existence of at least one
    "yes" value causes any other values to be ignored.

    """
    from tardis.tardis_portal.models.parameters import InstrumentParameter
    from nifcert.schemas.instrument import nifcert as nis

    logger.debug("checking instrument nifcert params")
    params = InstrumentParameter.objects.filter(
        string_value__exact=nis.CERTIFICATION_ENABLED_YES_VALUE,
        name__name__exact=nis.CERTIFICATION_ENABLED_NAME,
        name__schema__namespace__exact=nis.SCHEMA_NAMESPACE,
        name__schema__name__exact=nis.SCHEMA_NAME,
        parameterset__instrument__dataset__datafile__id=datafile_id)
    logger.debug("%d instrument nifcert params matched",
                 len(params))
    return len(params) > 0


def get_not_nifcert_metadata_value():

    """Return a a dict containing NIFCert and DICOM stats metadata
    suitable for marking a Datafile as not NIFCert.
    """
    import nifcert.schemas.datafile.nifcert as ndf
    import nifcert.schemas.datafile.dicom_statistics as ds
    return { 
        ndf.SCHEMA_NAMESPACE: {
            ndf.CERTIFIED_NAME: ndf.CERTIFIED_NO_VALUE
        }
    }


def scan_datafile_for_metadata(file_path, **kwargs):
    """Scan an instrument-specific input file to produce metadata
    that can be added to the file's DataFile and Dataset objects
    by one or more subsequent processing stages.

    At present only Bruker BioSpec data files ending with a
    ".PvDatasets" file extension are scanned for metadata.

    Parameters
    ----------
    file_path: str
        Path of input file to be scanned.
    Returns
    -------
    dict:
        A dictionary of dictionaries.  Each key in the outermost
        dictionary is a tardis_portal.Schema.namespace string
        (URI key), which identifies a unique DataFile ParameterSet
        (a DataFile may have multiple ParameterSets).   The
        corresponding value is a dictionary containing the parameter
        names (key) and values (value) to use in the ParameterSet.

        Returns an empty dictionary if file_path is valid but an
        error occurs while reading the file, or the file is empty.

    None:
        If file_path doesn't look like a Bruker BioSpec file name
        (typically has a ".PvDatasets" file extension).
    """

    logger.debug('nifcert.scan_datafile_for_metadata checking new '
                 'Datafile="%s"', file_path)

    if not is_bruker_biospec_file_path(file_path):
        logger.debug('nifcert.scan_datafile_for_metadata ignoring '
                     'Datafile="%s"', file_path)
        return None
    dm = None
    try:
        dm = find_dicom_files(file_path)
        if (dm[1] != None):     # error message
            logger.error("nifcert.scan_datafile_for_metadata "
                         "find_dicom_files('%s') => '%s'", file_path, dm[1])
    except Exception, e:
        logger.error("nifcert.scan_datafile_for_metadata "
                     "find_dicom_files('%s') EXCEPTION = '%s'", file_path, e)
        raise                   # propagate any exceptions (Celery locking)

    if dm is None or dm[1] != None:
        return get_not_nifcert_metadata_value()

    result = dict()

    # Append TruDat DICOM statistics data to result list.
    import nifcert.schemas.datafile.dicom_statistics as ds

    dmd = dm[0]                 # get the dict containing DICOM metadata
    if (ds.NUM_DICOM_DIRS_NAME in dmd
        and ds.NUM_DICOM_FILES_NAME
        and ds.TOTAL_DICOM_BYTES_NAME in dmd):
        logger.debug(
            '{0:4} dirs   {1:6} files   {2:12} bytes\n'
            .format(dmd[ds.NUM_DICOM_DIRS_NAME],
                    dmd[ds.NUM_DICOM_FILES_NAME],
                    dmd[ds.TOTAL_DICOM_BYTES_NAME]))
    result[ds.SCHEMA_NAMESPACE] = dmd

    # Append TruDat NIFCert Datafile schema data to result list.
    import nifcert.schemas.datafile.nifcert as ndf
    nif_meta = dict()
    num_dicom_dirs = dmd.get(ds.NUM_DICOM_DIRS_NAME, 0)
    if num_dicom_dirs > 0:
        cert_val = ndf.CERTIFIED_YES_VALUE
    else:
        cert_val = ndf.CERTIFIED_NO_VALUE
    nif_meta[ndf.CERTIFIED_NAME] = cert_val
    result[ndf.SCHEMA_NAMESPACE] = nif_meta

    logger.debug("nifcert.scan_datafile_for_metadata result='%s'", result)
    return result
