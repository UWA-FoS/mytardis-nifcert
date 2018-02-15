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
