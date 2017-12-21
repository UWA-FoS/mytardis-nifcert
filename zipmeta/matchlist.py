import os
import sys
import string
import zipfile
import re

def get_zip_metadata_matches(zip_path,
                             regex_list=[],
                             regex_flags=re.IGNORECASE):
    """Open the Zip file named zip_path, search the table of contents
    for file names matching zero or more regexes and return metadata
    about the matching files.

    Parameters
    ----------
    zip_path: string
        The Zip file to scan.
    regex_list: list
        A list of regular expression strings.  Any file within the zip file
        that has a path matching one or more regular expressions in the list
        will have its metadata returned by this function.
    regex_flags: int
        Flags passed to re.compile(), used when matching file names.

    Returns
    -------
    A tuple containing:

    [0]: A list of tuples containing metadata about each matching file.
         Details below.  Empty if an error occurs processing the Zip file.
    [1]: An error message string if an error occurs, or None otherwise.

    Each tuple in the list of metadata for matching filenames (see [0]
    above) contains:
    
    [0]: The file's path/name
    [1]: The size of the uncompressed file in bytes

    """

    try:
        z = zipfile.ZipFile(zip_path, "r", allowZip64 = True)
    except zipfile.BadZipfile:
        return ([],
                'Not a zip file: "{}"'.format(zip_path))
    except zipfile.LargeZipFile:
        return ([],
                'ZIP64 files are not enabled on this system')
    except :
        return ([],
                'Uknown error reading zip file: "{}"'.format(zip_path))

    metaList = []
    matchers = [re.compile(s, regex_flags) for s in regex_list]
    if (len(regex_list) == 0):   # common special case: match all files
        for zi in z.infolist():
            metaList.append((zi.filename, zi.file_size))
    else:
        for zi in z.infolist():
            for m in matchers:
                if m.search(zi.filename) != None:
                    metaList.append((zi.filename, zi.file_size))
                    break
    return (metaList, None)
