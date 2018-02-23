from . shared import app_schema


def get_schema_tree():
    """Return a complete tree containing every possible metadata
    ParameterName for this app along with its parent ParameterSet's
    Schema namespace and Schema type.  NOTE: In real metadata, some
    parts of the tree may be missing.

    The result is a dictionary with a
    tardis.tardis_portal.models.Schema.type as a key (INSTRUMENT,
    DATASET, etc).  The corresponding value is a dictionary of all
    this app's Schemas for that type.

    Within the app's Schemas dictionary value above, each key is a
    tardis.tardis_portal.models.Schema.namespace (URI).  The
    corresponding value is yet another dictionary containing an item
    for each tardis.tardis_portal.models.ParameterName in the
    tardis_portal Schema.namespace.  The item key is the
    ParameterName.name.  The item value is the ParameterName.type.

    """

    result = dict()
    tree = dict();
    from . instrument import nifcert as instr_nifcert
    tree[instr_nifcert.SCHEMA_METADATA[0]] = instr_nifcert.SCHEMA_METADATA[1]
    result[app_schema.tardis_schema.INSTRUMENT] = tree

    tree = dict();
    from . dataset import nifcert as ds_nifcert
    tree[ds_nifcert.SCHEMA_METADATA[0]] = ds_nifcert.SCHEMA_METADATA[1]
    result[app_schema.tardis_schema.DATASET] = tree

    tree = dict();
    from . datafile import nifcert as df_nifcert
    tree[df_nifcert.SCHEMA_METADATA[0]] = df_nifcert.SCHEMA_METADATA[1]
    from . datafile import dicom_statistics as df_ds
    tree[df_ds.SCHEMA_METADATA[0]] = df_ds.SCHEMA_METADATA[1]
    result[app_schema.tardis_schema.DATAFILE] = tree
    return result


SCHEMA_TREE = get_schema_tree()

def get_instrument_schema_tree():
    """Return a dictionary describing all the possible Instrument
    ParameterNames and types this app defines within its Instrument
    Schemas.  The dictionary's keys are Schema.namespaces (URIs).
    Each corresponding value is also a dictionary, mapping each of the
    Schema's ParameterName.names to its associated ParameterName.type.

    The result must not be modified.

    """
    return SCHEMA_TREE[app_schema.tardis_schema.INSTRUMENT]


def get_dataset_schema_tree():
    """Return a dictionary describing all the possible Dataset
    ParameterNames and types this app defines within its Dataset
    Schemas.  The dictionary's keys are Schema.namespaces (URIs).
    Each corresponding value is also a dictionary, mapping each of the
    Schema's ParameterName.names to its associated ParameterName.type.

    The result must not be modified.

    """
    return SCHEMA_TREE[app_schema.tardis_schema.DATASET]


def get_datafile_schema_tree():
    """Return a dictionary describing all the possible DataFile
    ParameterNames and types this app defines within its DataFile
    Schemas.  The dictionary's keys are Schema.namespaces (URIs).
    Each corresponding value is also a dictionary, mapping each of the
    Schema's ParameterName.names to its associated ParameterName.type.

    The result must not be modified.

    """
    return SCHEMA_TREE[app_schema.tardis_schema.DATAFILE]
