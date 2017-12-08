INSTALLED_APPS += (
    'nifcert',
)

POST_SAVE_FILTERS.append(
    (
        "nifcert.filters.metadata_filter.make_filter",
        [
            "NifCertMetadata",
            "http://trudat.cmca.uwa.edu.au/schemas/datafile/open-format/dicom-stats/1.0",
        ],
    ),
)
