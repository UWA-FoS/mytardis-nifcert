from django.conf import settings
from nifcert import metadata, tasks

class MetadataFilter(object):
    """MyTardis filter to process uploaded data sets and data files, adding
       metadata indicating their compliance with standards and procedures.
    Attributes
    ----------
    name: str
        Short name for schema
    schema: str
        Name of the schema to load the metadata into.
    """
    def __init__(self, name, schema):
        self.name = name
        self.schema = schema

    def __call__(self, sender, **kwargs):
        """Post save call back to invoke this filter.
        Parameters
        ----------
        sender: Model
            class of the model
        instance: model Instance
            Instance of model being saved.
        created: boolean
            Specifies whether a new record is being created.
        """
        instance = kwargs.get('instance')
        q = getattr(settings, 'DATACERT_QUEUE', 'celery')

        tasks.process_meta.apply_async(args=[metadata.get_meta, instance, self.schema,False], queue=q)

def make_filter(name, schema):
    return MetadataFilter(name, schema)

make_filter.__doc__ = MetadataFilter.__doc__
