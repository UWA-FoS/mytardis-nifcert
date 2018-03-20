"""
Definitions for working with TruDat metadata and schemas.
"""

from tardis.tardis_portal.models import Schema as tardis_schema
from tardis.tardis_portal.models import ParameterName as tardis_param

##############################################################################

# Namespace definitions

# Root URI of the TruDat schemas namespace

NAMESPACE_ROOT =                "http://trudat.cmca.uwa.edu.au/schemas"

# Subtree names in the TruDat schemas namespace

NAMESPACE_SUBTREE_INSTRUMENT =          "instrument"
NAMESPACE_SUBTREE_DATASET =             "dataset"
NAMESPACE_SUBTREE_DATAFILE =            "datafile"

NAMESPACE_SUBTREE_NIFCERT =             "nifcert"
NAMESPACE_SUBTREE_DICOM_STATISTICS =    "dicom-statistics"
NAMESPACE_SUBTREE_TRUSTED_DATA_REPO =   "trusted-data-repo"

##############################################################################

# Parameter values common to multiple TruDat Schemas

# NIF Certified parameter values for DataFiles and Datasets

CERTIFIED_YES_VALUE = "yes"
CERTIFIED_NO_VALUE =  "no"
