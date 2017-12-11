# nifcert/apps.py
# # https://docs.djangoproject.com/en/1.11/ref/applications/

from django.apps import AppConfig
from tardis.tardis_portal.models import Schema

class NifCertConfig(AppConfig):
    name = 'nifcert'
    verbose_name = "MyTardis NIF Certification"
