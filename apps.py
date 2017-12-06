import logging
from django.apps import AppConfig
from tardis.tardis_portal.models import Schema

logger = logging.getLogger(__name__)

CHECK_SCHEMA
    "http://trudat.uwa.edu.au/schemas/dataset/open-format/1.0"

class MyTardisDataCertConfig(AppConfig):
    name = 'mytardisdatacert'
    verbose_name = "MyTardis Data Certification"

    # At startup, ensure any Schemas and associated ParameterNames
    # required for this app's metadata are loaded if they don't
    # already exist.
    #
    # This ensures that the async tasks called to add metadata to
    # ParameterSets have a place to save their results.
    #
    # See the MyTardis source for details of the data model:
    #     mytardis/tardis/tardis_portal/models/parameters.py

    def ready(self):
        logger.debug("MyTardisDataCertConfig.ready() called")
        #
        # TODO: use Django migrations and / or one Schema
        # (ParameterSet) per file instead of calling loaddata() on one
        # file if CHECK_SCHEMA doesn't exist.  The current way may
        # impact other existing Schemas (new version loaded from file)
        # and fail to create new Schemas from the file if the sample
        # Schema does exist.
        #
        if not Schema.objects.filter(namespace__exact=CHECK_SCHEMA):
            from django.core.management import call_command

            # Import fixtures/trudat-schemas.json into the database
            #
            logger.debug("    loading TruDat schemas...")
            call_command('loaddata', 'trudat-schemas.json')
logger.debug("    loaded TruDat schemas")