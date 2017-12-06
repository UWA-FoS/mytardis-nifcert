import os
from zipmeta.matchlist import getMatchingZippedFilesMetadata

# Build a dictionary containing metadata about DICOM files in a
# .PvDatasets file from a Bruker BioSpec MRI scanner and
#
# The zipFileName argument should be a PvDatasets file, but does noot
# need to have the usual file extension.
#
# Return a tuple containing:
#
# [0]: a dictionary containing metadata about DICOM files in a zip file.
# [1]: An error message string if an error occurs, or None otherwise.
#
# The dictionary contains:
#
# ['numFiles']
#     The total number of DICOM files found in all directories.
#
# ['numBytes']
#     The total size in bytes of all DICOM files found in all directories.
#
# ['numDirs']
#     The total number of directories containing one or more DICOM
#     files (non-recursive).
#
# ['dirs']
#     A dictionary with one item for each directory that directly contains
#     one or more child DICOM files (non-recursive).  The key is the
#     directory path.  The value is a dictionary with two keys:
#     'numFiles' and 'numBytes'.  The values are the number of DICOM
#     files and the total size of the DICOM files within the directory
#     (non-recursive).

def findDicomFiles(zipFileName):
    zipMeta = getMatchingZippedFilesMetadata(zipFileName, ['[^/]\.dcm$'])
    if zipMeta[1] != None:
        return ({}, zipMeta[1])

    dicomFileCount = 0
    dicomDirCount = 0
    dicomBytesCount = 0
    dicomDirs = {}      # key: dir name, value: (fileCount, byteCount)
    numFilesKey = 'numFiles'
    numBytesKey = 'numBytes'
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

    resultDirs = {}
    for dirName, dirMeta in dicomDirs.items():
        # sys.stdout.write('{0:3} files  {1:8} bytes  {2}\n'
        #                  .format(dirMeta[0], dirMeta[1], dirName))
        resultDirs[dirName] = {
            numFilesKey: dirMeta[0],
            numBytesKey: dirMeta[1]
        }
    result = {
        numFilesKey: dicomFileCount,
        numBytesKey: dicomBytesCount,
        'numDirs': dicomDirCount,
        'dirs': resultDirs
    }
    return (result, None)