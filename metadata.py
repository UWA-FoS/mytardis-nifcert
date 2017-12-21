import logging
from nifcert.brukerbiospec.metadata.dicom import find_dicom_files
from nifcert.brukerbiospec.files import is_bruker_biospec_file_path
from nifcert import trudat

logger = logging.getLogger(__name__)


def get_non_nifcert_metadata():
    """Return a a dict containing NIFCert and DICOM stats metadata
    suitable for marking a Datafile as not NIFCert.
    """
    return { 
        trudat.NIFCERT_DATAFILE_NAMESPACE: {
            trudat.NIFCERT_DATAFILE_CERTIFIED_KEY:
            trudat.NIFCERT_NOT_CERTIFIED_VALUE
        },
        trudat.DICOM_STATS_DATAFILE_NAMESPACE: {
            trudat.DICOM_STATS_DATAFILE_NUM_FILES_KEY: 0,
            trudat.DICOM_STATS_DATAFILE_NUM_BYTES_KEY: 0,
            trudat.DICOM_STATS_DATAFILE_NUM_DIRS_KEY: 0,
            trudat.DICOM_STATS_DATAFILE_DIRS_KEY: {}
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
        return get_non_nifcert_metadata()

    result = dict()

    # Abbrev. global namespace ids (suffix: D=DICOM, N=NIFCERT, K=KEY, V=VALUE)
    NUM_DIRS_DK =  trudat.DICOM_STATS_DATAFILE_NUM_DIRS_KEY
    NUM_FILES_DK = trudat.DICOM_STATS_DATAFILE_NUM_FILES_KEY
    NUM_BYTES_DK = trudat.DICOM_STATS_DATAFILE_NUM_BYTES_KEY

    # Append TruDat DICOM statistics data to result list.
    dmd = dm[0]                 # get the dict containing DICOM metadata
    if (NUM_DIRS_DK in dmd and NUM_FILES_DK and NUM_BYTES_DK in dmd):
        logger.debug(
            '{0:4} dirs   {1:6} files   {2:12} bytes\n'
            .format(dmd[NUM_DIRS_DK], dmd[NUM_FILES_DK], dmd[NUM_BYTES_DK]))
    result[trudat.DICOM_STATS_DATAFILE_NAMESPACE] = dmd

    # Abbrev. global namespace ids (suffix: D=DICOM, N=NIFCERT, K=KEY, V=VALUE)
    FILE_CERT_NK = trudat.NIFCERT_DATAFILE_CERTIFIED_KEY
    IS_CERT_NV =   trudat.NIFCERT_IS_CERTIFIED_VALUE
    NOT_CERT_NV =  trudat.NIFCERT_NOT_CERTIFIED_VALUE

    # Append TruDat NIFCert Datafile schema data to result list.
    nif_meta = dict()
    num_dicom_dirs = dmd[NUM_DIRS_DK] if NUM_DIRS_DK in dmd else 0
    nif_meta[FILE_CERT_NK] = NOT_CERT_NV if num_dicom_dirs == 0 else IS_CERT_NV
    result[trudat.NIFCERT_DATAFILE_NAMESPACE] = nif_meta

    logger.debug("nifcert.scan_datafile_for_metadata result='%s'", result)
    return result
