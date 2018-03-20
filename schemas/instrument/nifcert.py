"""
Definitions for working with TruDat Instrument NIFCert metadata.
"""

# Convenient access to common data for this app's schemas
from .. shared import app_schema

# Details about this schema
#
SCHEMA_NAME =         "TruDat Instrument NIF Certified"
SCHEMA_VERSION_MAJOR = 1
SCHEMA_VERSION_MINOR = 0
SCHEMA_VERSION =       "{0}.{1}".format(SCHEMA_VERSION_MAJOR,
                                        SCHEMA_VERSION_MINOR)
SCHEMA_NAMESPACE = (
    app_schema.NAMESPACE_ROOT + "/"
    + app_schema.NAMESPACE_SUBTREE_INSTRUMENT + "/"
    + app_schema.NAMESPACE_SUBTREE_NIFCERT + "/"
    + SCHEMA_VERSION
)

# Metadata parameter that controls whether an Instrument generates
# NIF Certified DataFiles and Datasets
#
CERTIFICATION_ENABLED_NAME =   "NIF_certification_enabled"
CERTIFICATION_ENABLED_LABEL = "NIF Certification Enabled"
CERTIFICATION_ENABLED_TYPE =  app_schema.tardis_param.STRING
# Values:
CERTIFICATION_ENABLED_YES_VALUE = "yes"
CERTIFICATION_ENABLED_NO_VALUE =  "no"

# Tree structure of this namespace
#
SCHEMA_METADATA = (
    SCHEMA_NAMESPACE,
    {
        CERTIFICATION_ENABLED_NAME: CERTIFICATION_ENABLED_TYPE
    }
)
