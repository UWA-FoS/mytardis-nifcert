"""
Definitions for working with TruDat Instrument Trusted Data
Repository (TDR) metadata.

"""

# Convenient access to common data for this app's schemas
from .. shared import app_schema

# Details about this schema
#
SCHEMA_NAME =         "TruDat Registered Instrument"
SCHEMA_VERSION_MAJOR = 1
SCHEMA_VERSION_MINOR = 0
SCHEMA_VERSION =       "{0}.{1}".format(SCHEMA_VERSION_MAJOR,
                                        SCHEMA_VERSION_MINOR)
SCHEMA_NAMESPACE = (
    app_schema.NAMESPACE_ROOT + "/"
    + app_schema.NAMESPACE_SUBTREE_INSTRUMENT + "/"
    + app_schema.NAMESPACE_SUBTREE_TRUSTED_DATA_REPO + "/"
    + SCHEMA_VERSION
)

# Metadata parameter for a Trusted Data Repository Instrument's
# Quality Control (QC) project URL.
#
QC_PROJECT_NAME =   "QC_project"
QC_PROJECT_LABEL = "QC Project"
QC_PROJECT_TYPE =  app_schema.tardis_param.LINK

# Metadata parameter for a Trusted Data Repository Instrument's
# Research Data Australia (RDA) Service Record URL.
#
RDA_SERVICE_RECORD_NAME =   "RDA_service_record"
RDA_SERVICE_RECORD_LABEL = "RDA service record"
RDA_SERVICE_RECORD_TYPE =  app_schema.tardis_param.LINK

# Tree structure of this namespace
#
SCHEMA_METADATA = (
    SCHEMA_NAMESPACE,
    {
        QC_PROJECT_NAME:         QC_PROJECT_TYPE,
        RDA_SERVICE_RECORD_NAME: RDA_SERVICE_RECORD_TYPE
    }
)
