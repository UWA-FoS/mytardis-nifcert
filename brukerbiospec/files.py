"""Definitions for working with Bruker BioSpec 9.4T MRI data files"""

import os

FILE_EXT =       ".PvDatasets"
FILE_EXT_LOWER = FILE_EXT.lower()


def is_bruker_biospec_file_path(path):
    """
    Return a boolean value indicating whether a filename appears to be
    a Bruker Biospec MRI data file or not.

    Parameters
    ----------
    path: string
        The file path to check.
    Returns
    -------
    True if path looks like a Bruker BioSpec file name, False otherwise.

    """
    base, ext = os.path.splitext(os.path.basename(path))
    return ext.lower() == FILE_EXT_LOWER
