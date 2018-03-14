# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
import nifcert.models


class Migration(migrations.Migration):

    dependencies = [
        ('tardis_portal', '0011_auto_20160505_1643'),
        ('nifcert', '0002_trudat_schemas'),
    ]

    operations = [
        migrations.CreateModel(
            name='DatafileCertificationLock',
            fields=[
                ('datafile', models.OneToOneField(primary_key=True, serialize=False, to='tardis_portal.DataFile', db_index=False)),
                ('state', models.PositiveSmallIntegerField(default=0)),
                ('time_updated', models.DateTimeField(default=nifcert.models.get_utcnow_str)),
            ],
        ),
    ]
