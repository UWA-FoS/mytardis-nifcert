"""
Definitions for working with TruDat metadata and schemas.
"""

from tardis.tardis_portal.models import Schema as TardisSchema

# Root of the TruDat schemas namespace

NAMESPACE_ROOT = "http://trudat.cmca.uwa.edu.au/schemas"

NIFCERT_IS_CERTIFIED_VALUE = "yes"
NIFCERT_NOT_CERTIFIED_VALUE = "no"

##############################################################################

# TruDat NIF Certified (NIFCert) Dataset schema namespace
#
NIFCERT_DATASET_NAMESPACE = (
    NAMESPACE_ROOT + "/dataset/open-format/1.0"
)

# Keys used to access metadata in the namespace
#
NIFCERT_DATASET_CERTIFIED_KEY = "NIF_certified"

NIFCERT_DATASET_ALL_KEYS = [NIFCERT_DATASET_CERTIFIED_KEY]

##############################################################################

# TruDat NIF Certified (NIFCert) Datafile schema namespace
#
NIFCERT_DATAFILE_NAMESPACE = (
    NAMESPACE_ROOT + "/datafile/open-format/1.0"
)

# Keys used to access metadata in the namespace
#
NIFCERT_DATAFILE_CERTIFIED_KEY = "NIF_certified"

NIFCERT_DATAFILE_ALL_KEYS = [NIFCERT_DATAFILE_CERTIFIED_KEY]

##############################################################################

# TruDat DICOM statistics schema namespace
#
DICOM_STATS_DATAFILE_NAMESPACE = (
    NAMESPACE_ROOT + "/datafile/open-format/dicom-stats/1.0"
)

# Keys used to access metadata in the namespace
#
DICOM_STATS_DATAFILE_NUM_FILES_KEY = "numDicomFiles"
DICOM_STATS_DATAFILE_NUM_BYTES_KEY = "numDicomBytes"
DICOM_STATS_DATAFILE_NUM_DIRS_KEY = "numDicomDirs"
DICOM_STATS_DATAFILE_DIRS_KEY = "dicomDirsMetadata"

DICOM_STATS_DATAFILE_ALL_KEYS = [
    DICOM_STATS_DATAFILE_NUM_FILES_KEY,
    DICOM_STATS_DATAFILE_NUM_BYTES_KEY,
    DICOM_STATS_DATAFILE_NUM_DIRS_KEY,
    DICOM_STATS_DATAFILE_DIRS_KEY
]

##############################################################################

# A complete tree containing every possible metadata ParameterName for
# this app along with its parent ParameterSet's Schema namespace and
# Schema type.
#
# NOTE: In real metadata, some parts of the tree may be missing.
#
# This tree can be used to distinguish between metadata created by
# this app and metadata from other sources (other apps and users).

NAMESPACE_TREE = {
    TardisSchema.DATASET : {
        NIFCERT_DATASET_NAMESPACE: NIFCERT_DATASET_ALL_KEYS
    },
    TardisSchema.DATAFILE : {
        NIFCERT_DATAFILE_NAMESPACE:     NIFCERT_DATAFILE_ALL_KEYS,
        DICOM_STATS_DATAFILE_NAMESPACE: DICOM_STATS_DATAFILE_ALL_KEYS
    },
}

##############################################################################

class Schemas(object):
    """
    Helper class for working with TruDat Schemas and related metadata
    """

    # Cached database ids for frequently-used schemas managed by this
    # app.  Caching is safe because the schemas are meant to be
    # immutable.  Neither the ids nor the data should change.

    _nifcert_dataset_schema_id = None
    _nifcert_datafile_schema_id = None
    _dicom_stats_datafile_schema_id = None

    @staticmethod
    def get_nifcert_dataset_schema_id(use_cache=True):
        """
        Return the database id for the TruDat NIFCert Dataset
        Schema object.

        The result is valid if >= 0, or None if an error occurred.
        This function tries to avoid throwing any exceptions.
        """
        if Schemas._nifcert_dataset_schema_id != None and use_cache:
            if Schemas._nifcert_dataset_schema_id >= 0:
                return Schemas._nifcert_dataset_schema_id
            if Schemas._nifcert_dataset_schema_id == -1:
                return None
        ds_schemas = (
            Schema.objects.filter(
                namespace__exact=NIFCERT_DATASET_NAMESPACE))
        if len(ds_schemas) != 1:
            Schemas._nifcert_dataset_schema_id = -1
            return None
        Schemas._nifcert_dataset_schema_id = ds_schemas.first().id
        return Schemas._nifcert_dataset_schema_id


    @staticmethod
    def get_nifcert_datafile_schema_id(use_cache=True):
        """
        Return the database id for the TruDat NIFCert Datafile
        Schema object.

        The result is valid if >= 0, or None if an error occurred.
        This function tries to avoid throwing any exceptions.
        """
        if Schemas._nifcert_datafile_schema_id is not None and use_cache:
            if Schemas._nifcert_datafile_schema_id >= 0:
                return Schemas._nifcert_datafile_schema_id
            if Schemas._nifcert_datafile_schema_id == -1:
                return None
        ds_schemas = (
            Schema.objects.filter(
                namespace__exact=NIFCERT_DATAFILE_NAMESPACE))
        if len(ds_schemas) != 1:
            Schemas._nifcert_datafile_schema_id = -1
            return None
        Schemas._nifcert_datafile_schema_id = ds_schemas.first().id
        return Schemas._nifcert_datafile_schema_id


    @staticmethod
    def get_dicom_stats_datafile_schema_id(use_cache=True):
        """
        Return the database id for the TruDat DICOM Statistics
        Datafile Schema object.

        The result is valid if >= 0, or None if an error occurred.
        This function tries to avoid throwing any exceptions.
        """
        if Schemas._dicom_stats_datafile_schema_id is not None and use_cache:
            if Schemas._dicom_stats_datafile_schema_id >= 0:
                return Schemas._dicom_stats_datafile_schema_id
            if Schemas._dicom_stats_datafile_schema_id == -1:
                return None
        ds_schemas = (
            Schema.objects.filter(
                namespace__exact=DICOM_STATS_DATAFILE_NAMESPACE))
        if len(ds_schemas) != 1:
            Schemas._dicom_stats_datafile_schema_id = -1
            return None
        Schemas._dicom_stats_datafile_schema_id = ds_schemas.first().id
        return Schemas._dicom_stats_datafile_schema_id
