import os
import sys
import string
import zipfile
import re

# Open the Zip file named zipFileName, search the table of contents
# for filenames matching zero or more regexes and return a tuple
# containing:
#
# [0]: A list of tuples containing metadata about each matching file.
#      Details below.  Empty if an error occurs processing the Zip file.
# [1]: An error message string if an error occurs, or None otherwise.
#
# Each tuple for a matching filename contains:
#
# [0]: The file's path/name
# [1]: The size of the uncompressed file in bytes

def getMatchingZippedFilesMetadata(zipFileName,
                                   regexList=[],
                                   regexFlags=re.IGNORECASE):
    try:
        z = zipfile.ZipFile(zipFileName, "r", allowZip64 = True)
    except zipfile.BadZipfile:
        return ([],
                'Not a zip file: "{}"'.format(zipFileName))
    except zipfile.LargeZipFile:
        return ([],
                'ZIP64 files are not enabled on this system')
    except :
        return ([],
                'Uknown error reading zip file: "{}"'.format(zipFileName))

    metaList = []
    matchers = [re.compile(s, regexFlags) for s in regexList]
    if (len(regexList) == 0):   # common special case: match all files
        for zi in z.infolist():
            metaList.append((zi.filename, zi.file_size))
    else:
        for zi in z.infolist():
            for m in matchers:
                if m.search(zi.filename) != None:
                    metaList.append((zi.filename, zi.file_size))
                    break
    return (metaList, None)