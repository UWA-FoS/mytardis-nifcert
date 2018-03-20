"""
Definitions for working with TruDat Datafile DICOM statistics metadata.
"""

# Convenient access to common data for this app's schemas
from .. shared import app_schema

# Details about this schema
#
SCHEMA_NAME =         "TruDat DataFile DICOM Statistics"
SCHEMA_VERSION_MAJOR = 1
SCHEMA_VERSION_MINOR = 0
SCHEMA_VERSION =       "{0}.{1}".format(SCHEMA_VERSION_MAJOR,
                                        SCHEMA_VERSION_MINOR)
SCHEMA_NAMESPACE = (
    app_schema.NAMESPACE_ROOT + "/"
    + app_schema.NAMESPACE_SUBTREE_DATAFILE + "/"
    + app_schema.NAMESPACE_SUBTREE_DICOM_STATISTICS + "/"
    + SCHEMA_VERSION
)

# Metadata parameter for reporting the total number of DICOM files
# contained by a DataFile (typically a ZIP file that contains other
# files).
#
NUM_DICOM_FILES_NAME =   "num_DICOM_files"
NUM_DICOM_FILES_LABEL = "Number of DICOM files"
NUM_DICOM_FILES_TYPE =  app_schema.tardis_param.NUMERIC

# Metadata parameter for reporting the total size in bytes of all
# DICOM files contined by a DataFile (typically a ZIP file that
# contains other files).
#
TOTAL_DICOM_BYTES_NAME =   "total_DICOM_bytes"
TOTAL_DICOM_BYTES_LABEL = "Total bytes of DICOM data"
TOTAL_DICOM_BYTES_TYPE =  app_schema.tardis_param.NUMERIC

# Metadata parameter for reporting the total number of directories
# containing one or more DICOM files (non-recursive), all within a
# DataFile (typically a ZIP file that contains other files).
#
NUM_DICOM_DIRS_NAME =   "num_DICOM_dirs"
NUM_DICOM_DIRS_LABEL = "Number of DICOM directories"
NUM_DICOM_DIRS_TYPE =  app_schema.tardis_param.NUMERIC

# Metadata parameter providing statistics for each directory within a
# DataFile (typically a ZIP file that contains other files) that
# directly contains at least one DICOM file as a direct child
# (non-recursive).  The value is a string containing a JSON-encoded
# dictionary with directory paths as the keys.  Each value is itself a
# dictionary with NUM_DICOM_FILES_NAME and TOTAL_DICOM_BYTES_NAME as
# keys (see above), and values calculated non-recursively for the
# corresponding directory.
#
# Only directories with DICOM files immediately within them appear as
# keys in the top-level ditionary.  Their parent directories' paths
# must be inferred if a full directory hierarchy needs to be
# constructed.
#
# The type chosen here indicates the data should be displayed by the
# user interface as a STRING.  MyTardis uses the JSON display value
# for other purposes.
#
DICOM_DIRS_DICT_NAME =   "DICOM_dirs_list"
DICOM_DIRS_DICT_LABEL = "DICOM directories list"
DICOM_DIRS_DICT_TYPE =  app_schema.tardis_param.STRING

# Tree structure of this namespace
#
SCHEMA_METADATA = (
    SCHEMA_NAMESPACE,
    {
        NUM_DICOM_FILES_NAME:   NUM_DICOM_FILES_TYPE,
        TOTAL_DICOM_BYTES_NAME: TOTAL_DICOM_BYTES_TYPE,
        NUM_DICOM_DIRS_NAME:    NUM_DICOM_DIRS_TYPE,
        DICOM_DIRS_DICT_NAME:   DICOM_DIRS_DICT_TYPE 
    }
)
