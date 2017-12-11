# -*- coding: utf-8 -*-
#
#   This data migration inserts into the tardis_portal app's database
# the information required for this app (NIFCert) to add TruDat
# metadata to newly ingested DataFiles (and their parent Datasets).
#
#   See the MyTardis source for details of the data model:
#     mytardis/tardis/tardis_portal/models/parameters.py

from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings
 
def create_TruDat_schemas(apps_registry, schema_editor):

    # TODO: use a proper migration instead of this quick hack with
    # call_command("loaddata")
    #
    from django.core.management import call_command
    call_command('loaddata', 'nifcert-trudat-schemas.json')


class Migration(migrations.Migration):

    dependencies = [
        # The mytardis_portal app must create its Schema and
        # ParameterSet Models before this app can add data to them.
        ('tardis_portal', '0001_initial'),
    ]

    operations = [
        migrations.RunPython(create_TruDat_schemas),
    ]
