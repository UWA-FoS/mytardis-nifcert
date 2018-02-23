import os

from nifcert.zipmeta.matchlist import get_zip_metadata_matches

def find_dicom_files(pvds_file_name):
    """
    Build a dictionary containing metadata about DICOM files in a
    .PvDatasets file from a Bruker BioSpec 9.4T MRI scanner and

    Parameters
    ----------
    pvds_file_name: string
        Should be a .PvDatasets file, but does not need to have the
        usual file extension.

    Returns
    -------
    A tuple containing:

    [0]: a dictionary containing metadata about DICOM files in a zip file.
    [1]: An error message string if an error occurs, or None otherwise.

    The dictionary contains the following keys (all strings) and values:

    nifcert.schemas.datafile.dicom_statistics.NUM_DICOM_FILES_NAME
        The total number of DICOM files found in all directories.

    nifcert.schemas.datafile.dicom_statistics.TOTAL_DICOM_BYTES_NAME
        The total size in bytes of all DICOM files found in all
        directories.

    nifcert.schemas.datafile.dicom_statistics.NUM_DICOM_DIRS_NAME
        The total number of directories containing one or more DICOM
        files (non-recursive).

    nifcert.schemas.datafile.dicom_statistics.DICOM_DIRS_DICT_NAME
        A dictionary with one item for each directory that directly
        contains one or more child DICOM files (non-recursive).
        The key is the directory path.  The value is a dictionary with
        two key/value items:
            nifcert.schemas.datafile.dicom_statistics.NUM_DICOM_FILES_NAME
                the number of DICOM files and the total size of the
                DICOM files within the directory (non-recursive).
            nifcert.schemas.datafile.dicom_statistics.TOTAL_DICOM_BYTES_NAME
                the total size of the DICOM files within the directory
                (non-recursive).
        Only directories with DICOM files immediately within them appear
        as keys.  Their parent directories' paths must be inferred if a
        full directory hierarchy needs to be constructed.
    """

    zipMeta = get_zip_metadata_matches(pvds_file_name, ['[^/]\.dcm$'])
    if zipMeta[1] != None:
        return ({}, zipMeta[1])

    dicomFileCount = 0
    dicomDirCount = 0
    dicomBytesCount = 0
    dicomDirs = {}      # key: dir name, value: (fileCount, byteCount)
    for meta in zipMeta[0]:
        # sys.stdout.write('{0:8} {1}\n'.format(meta[1], meta[0]))
        dicomFileCount += 1
        dicomBytesCount += meta[1]
        (dirName, fileName) = os.path.split(meta[0])
        if dirName in dicomDirs:
            dirMeta = dicomDirs[dirName]
            dicomDirs[dirName] = [ dirMeta[0] + 1, dirMeta[1] + meta[1] ]
        else:
            dicomDirCount += 1
            dicomDirs[dirName] = [ 1, meta[1] ]

    # sys.stdout.write('\ntotal {0:4} dirs  {1:6} files  {2:12} bytes\n\n'
    #                  .format(dicomDirCount, dicomFileCount, dicomBytesCount))

    import nifcert.schemas.datafile.dicom_statistics as ds
    resultDirs = {}
    for dirName, dirMeta in dicomDirs.items():
        # sys.stdout.write('{0:3} files  {1:8} bytes  {2}\n'
        #                  .format(dirMeta[0], dirMeta[1], dirName))
        resultDirs[dirName] = {
            ds.NUM_DICOM_FILES_NAME: dirMeta[0],
            ds.TOTAL_DICOM_BYTES_NAME: dirMeta[1]
        }
    result = {
        ds.NUM_DICOM_FILES_NAME: dicomFileCount,
        ds.TOTAL_DICOM_BYTES_NAME: dicomBytesCount,
        ds.NUM_DICOM_DIRS_NAME: dicomDirCount,
        ds.DICOM_DIRS_DICT_NAME: resultDirs
    }
    return (result, None)
