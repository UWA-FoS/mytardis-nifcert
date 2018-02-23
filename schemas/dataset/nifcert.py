"""
Definitions for working with TruDat Dataset NIFCert metadata.
"""

# Convenient access to common data for this app's schemas
from .. shared import app_schema

# Details about this schema
#
SCHEMA_NAME =         "TruDat Dataset NIF Certified"
SCHEMA_VERSION_MAJOR = 0
SCHEMA_VERSION_MINOR = 0
SCHEMA_VERSION =       "{0}.{1}".format(SCHEMA_VERSION_MAJOR,
                                        SCHEMA_VERSION_MINOR)
SCHEMA_NAMESPACE = (
    app_schema.NAMESPACE_ROOT + "/"
    + app_schema.NAMESPACE_SUBTREE_DATASET + "/"
    + app_schema.NAMESPACE_SUBTREE_NIFCERT + "/"
    + SCHEMA_VERSION
)

# Metadata parameter that indicates whether a Dataset contains a
# valid collection of NIF Certified DataFiles.
#
CERTIFIED_NAME =   "NIF_certified"
CERTIFIED_LABEL = "NIF Certified"
CERTIFIED_TYPE =  app_schema.tardis_param.STRING
# Values:
CERTIFIED_YES_VALUE = app_schema.CERTIFIED_YES_VALUE
CERTIFIED_NO_VALUE =  app_schema.CERTIFIED_NO_VALUE

# Tree structure of this namespace
#
SCHEMA_METADATA = (
    SCHEMA_NAMESPACE,
    {
        CERTIFIED_NAME: CERTIFIED_TYPE
    }
)
