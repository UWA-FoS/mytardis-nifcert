# -*- coding: utf-8 -*-
#
#   This data migration inserts into the tardis_portal app's database
# the information required for this app (NIFCert) to add TruDat
# metadata to newly ingested DataFiles (and their parent Datasets).
#
#   See the MyTardis source for details of the data model:
#     mytardis/tardis/tardis_portal/models/parameters.py

from __future__ import unicode_literals

import logging
from django.db import models, migrations
from django.conf import settings
from tardis.tardis_portal.models import Schema
from tardis.tardis_portal.models import ParameterName

logger = logging.getLogger(__name__)


def create_instrument_nifcert_metadata_schema():
    """
    Create any Instrument Schema(s) and ParameterName(s) required by
    this app.
    """
    stage = "initialising"
    try:
        import nifcert.schemas.instrument.nifcert as ni_info

        stage = "creating NIFCert Instrument metadata Schema"
        logger.debug(
            "nifcert.create_instrument_nifcert_metadata_schema: %s", stage)
        instr_schema = (
            Schema(
                namespace=ni_info.SCHEMA_NAMESPACE,
                name=ni_info.SCHEMA_NAME,
                type=Schema.INSTRUMENT,
                immutable=True,
                hidden=False))
        instr_schema.save()

        stage_fmt = 'creating NIFCert Instrument metadata parameter="{}"'
        stage = stage_fmt.format(ni_info.CERTIFICATION_ENABLED_NAME)
        logger.debug(
            "nifcert.create_instrument_nifcert_metadata_schema: %s", stage)
        param = (
            ParameterName(
                schema=instr_schema,
                name=ni_info.CERTIFICATION_ENABLED_NAME,
                full_name=ni_info.CERTIFICATION_ENABLED_LABEL,
                data_type=ni_info.CERTIFICATION_ENABLED_TYPE,
                immutable=True,
                comparison_type=ParameterName.EXACT_VALUE_COMPARISON,
                is_searchable=True,
                choices="",
                order=9999))
        param.save()

        stage = "finalised"
        logger.debug(
            "nifcert.create_instrument_nifcert_metadata_schema: %s", stage)

    except Exception, e:
        logger.error("nifcert.create_instrument_nifcert_metadata_schema: "
                     "exception at stage: %s:\n %s", stage, e)


def create_instrument_tdr_metadata_schema():
    """
    Create any Instrument Trusted Data Repository (TDR Schema(s) and
    ParameterName(s) required by this app or its users.

    """
    stage = "initialising"
    try:
        import nifcert.schemas.instrument.tdr as ti_info

        stage = "creating NIFCert Instrument TDR metadata Schema"
        logger.debug("nifcert.create_instrument_tdr_metadata_schema: %s", stage)
        instr_schema = (
            Schema(
                namespace=ti_info.SCHEMA_NAMESPACE,
                name=ti_info.SCHEMA_NAME,
                type=Schema.INSTRUMENT,
                immutable=True,
                hidden=False))
        instr_schema.save()

        stage_fmt = 'creating NIFCert Instrument TDR metadata parameter="{}"'
        stage = stage_fmt.format(ti_info.QC_PROJECT_NAME)
        logger.debug("nifcert.create_instrument_tdr_metadata_schema: %s", stage)
        param = (
            ParameterName(
                schema=instr_schema,
                name=ti_info.QC_PROJECT_NAME,
                full_name=ti_info.QC_PROJECT_LABEL,
                data_type=ti_info.QC_PROJECT_TYPE,
                immutable=True,
                comparison_type=ParameterName.EXACT_VALUE_COMPARISON,
                is_searchable=True,
                choices="",
                order=9999))
        param.save()

        stage_fmt = 'creating NIFCert Instrument TDR metadata parameter="{}"'
        stage = stage_fmt.format(ti_info.RDA_SERVICE_RECORD_NAME)
        logger.debug("nifcert.create_instrument_tdr_metadata_schema: %s", stage)
        param = (
            ParameterName(
                schema=instr_schema,
                name=ti_info.RDA_SERVICE_RECORD_NAME,
                full_name=ti_info.RDA_SERVICE_RECORD_LABEL,
                data_type=ti_info.RDA_SERVICE_RECORD_TYPE,
                immutable=True,
                comparison_type=ParameterName.EXACT_VALUE_COMPARISON,
                is_searchable=True,
                choices="",
                order=9999))
        param.save()

        stage = "finalised"
        logger.debug("nifcert.create_instrument_tdr_metadata_schema: %s", stage)

    except Exception, e:
        logger.error("nifcert.create_instrument_tdr_metadata_schema: "
                     "exception at stage: %s:\n %s", stage, e)


def create_dataset_metadata_schema():
    """
    Create any Dataset Schema(s) and ParameterName(s) required by this
    app.
    """
    stage = "initialising"
    try:
        import nifcert.schemas.dataset.nifcert as nds_info

        stage = "creating NIFCert Dataset metadata Schema"
        logger.debug("nifcert.create_dataset_metadata_schema: %s", stage)
        ds_schema = (
            Schema(
                namespace=nds_info.SCHEMA_NAMESPACE,
                name=nds_info.SCHEMA_NAME,
                type=Schema.DATASET,
                immutable=True,
                hidden=False))
        ds_schema.save()

        stage_fmt = 'creating NIFCert Dataset metadata parameter="{}"'
        stage = stage_fmt.format(nds_info.CERTIFIED_NAME)
        logger.debug("nifcert.create_dataset_metadata_schema: %s", stage)
        param = (
            ParameterName(
                schema=ds_schema,
                name=nds_info.CERTIFIED_NAME,
                full_name=nds_info.CERTIFIED_LABEL,
                data_type=nds_info.CERTIFIED_TYPE,
                immutable=True,
                comparison_type=ParameterName.EXACT_VALUE_COMPARISON,
                is_searchable=True,
                choices="",
                order=9999))
        param.save()

        stage = "finalised"
        logger.debug("nifcert.create_dataset_metadata_schema: %s", stage)

    except Exception, e:
        logger.error("nifcert.create_dataset_metadata_schema: "
                     "exception at stage: %s:\n %s", stage, e)


def create_datafile_nifcert_metadata_schema():
    """
    Create any DataFile NIFCert Schema(s) and ParameterName(s)
    required by this app.
    """
    stage = "initialising"
    try:
        import nifcert.schemas.datafile.nifcert as ndf_info

        stage = "creating NIFCert DataFile metadata Schema"
        logger.debug("nifcert.create_datafile_nifcert_metadata_schema: %s",
                     stage)
        ds_schema = (
            Schema(
                namespace=ndf_info.SCHEMA_NAMESPACE,
                name=ndf_info.SCHEMA_NAME,
                type=Schema.DATAFILE,
                immutable=True,
                hidden=False))
        ds_schema.save()

        stage_fmt = 'creating NIFCert DataFile metadata parameter="{}"'
        stage = stage_fmt.format(ndf_info.CERTIFIED_NAME)
        logger.debug("nifcert.create_datafile_nifcert_metadata_schema: %s",
                     stage)
        param = (
            ParameterName(
                schema=ds_schema,
                name=ndf_info.CERTIFIED_NAME,
                full_name=ndf_info.CERTIFIED_LABEL,
                data_type=ndf_info.CERTIFIED_TYPE,
                immutable=True,
                comparison_type=ParameterName.EXACT_VALUE_COMPARISON,
                is_searchable=True,
                choices="",
                order=9999))
        param.save()

        stage = "finalised"
        logger.debug("nifcert.create_datafile_nifcert_metadata_schema: %s",
                     stage)

    except Exception, e:
        logger.error("nifcert.create_datafile_nifcert_metadata_schema: "
                     "exception at stage: %s:\n %s", stage, e)


def create_datafile_dicom_statistics_metadata_schema():
    """
    Create any DataFile DICOM statistics Schema(s) and ParameterName(s)
    required by this app.
    """
    stage = "initialising"
    try:
        import nifcert.schemas.datafile.dicom_statistics as ds_info

        stage = "creating DICOM statistics DataFile metadata Schema"
        logger.debug("nifcert.create_datafile_dicom_statistics_"
                     "metadata_schema: %s", stage)
        ds_schema = (
            Schema(
                namespace=ds_info.SCHEMA_NAMESPACE,
                name=ds_info.SCHEMA_NAME,
                type=Schema.DATAFILE,
                immutable=True,
                hidden=False))
        ds_schema.save()

        stage_fmt = 'creating DICOM statistics DataFile metadata parameter="{}"'
        stage = stage_fmt.format(ds_info.NUM_DICOM_FILES_NAME)
        logger.debug("nifcert.create_datafile_dicom_statistics_"
                     "metadata_schema: %s", stage)
        param = (
            ParameterName(
                schema=ds_schema,
                name=ds_info.NUM_DICOM_FILES_NAME,
                full_name=ds_info.NUM_DICOM_FILES_LABEL,
                data_type=ds_info.NUM_DICOM_FILES_TYPE,
                immutable=True,
                comparison_type=ParameterName.EXACT_VALUE_COMPARISON,
                is_searchable=False,
                choices="",
                order=9999))
        param.save()

        stage_fmt = 'creating DICOM statistics DataFile metadata parameter="{}"'
        stage = stage_fmt.format(ds_info.TOTAL_DICOM_BYTES_NAME)
        logger.debug("nifcert.create_datafile_dicom_statistics_"
                     "metadata_schema: %s", stage)
        param = (
            ParameterName(
                schema=ds_schema,
                name=ds_info.TOTAL_DICOM_BYTES_NAME,
                full_name=ds_info.TOTAL_DICOM_BYTES_LABEL,
                data_type=ds_info.TOTAL_DICOM_BYTES_TYPE,
                immutable=True,
                comparison_type=ParameterName.EXACT_VALUE_COMPARISON,
                is_searchable=False,
                choices="",
                order=9999))
        param.save()

        stage_fmt = 'creating DICOM statistics DataFile metadata parameter="{}"'
        stage = stage_fmt.format(ds_info.NUM_DICOM_DIRS_NAME)
        logger.debug("nifcert.create_datafile_dicom_statistics_"
                     "metadata_schema: %s", stage)
        param = (
            ParameterName(
                schema=ds_schema,
                name=ds_info.NUM_DICOM_DIRS_NAME,
                full_name=ds_info.NUM_DICOM_DIRS_LABEL,
                data_type=ds_info.NUM_DICOM_DIRS_TYPE,
                immutable=True,
                comparison_type=ParameterName.EXACT_VALUE_COMPARISON,
                is_searchable=False,
                choices="",
                order=9999))
        param.save()

        stage_fmt = 'creating DICOM statistics DataFile metadata parameter="{}"'
        stage = stage_fmt.format(ds_info.DICOM_DIRS_DICT_NAME)
        logger.debug("nifcert.create_datafile_dicom_statistics_"
                     "metadata_schema: %s", stage)
        param = (
            ParameterName(
                schema=ds_schema,
                name=ds_info.DICOM_DIRS_DICT_NAME,
                full_name=ds_info.DICOM_DIRS_DICT_LABEL,
                data_type=ds_info.DICOM_DIRS_DICT_TYPE,
                immutable=True,
                comparison_type=ParameterName.EXACT_VALUE_COMPARISON,
                is_searchable=True,
                choices="",
                order=9999))
        param.save()

        stage = "finalised"
        logger.debug("nifcert.create_datafile_dicom_statistics_"
                     "metadata_schema: %s", stage)

    except Exception, e:
        logger.error("nifcert.create_datafile_dicom_statistics_"
                     "metadata_schema: exception at stage: %s:\n %s", stage, e)


def create_app_metadata_schema(apps_registry, schema_editor):
    """
    Create any Schema(s) and ParameterName(s) required by this app.
    """
    create_instrument_nifcert_metadata_schema()
    create_instrument_tdr_metadata_schema()
    create_dataset_metadata_schema()
    create_datafile_nifcert_metadata_schema()
    create_datafile_dicom_statistics_metadata_schema()


class Migration(migrations.Migration):

    dependencies = [
        # The mytardis_portal app must create its Schema and
        # ParameterSet Models before this app can add data to them.
        ('tardis_portal', '0011_auto_20160505_1643'),
    ]

    operations = [
        migrations.RunPython(create_app_metadata_schema),
    ]
