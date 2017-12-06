import logging
import os
import tardis.apps.mytardisdatacert.mytardisdatacert.brukerbiospec

logger = logging.getLogger(__name__)

def get_meta(input_file_path, **kwargs):
    """ Extract specific metadata typically used in bio-image analysis. Also
    outputs a preview image to the output directory.
    Parameters
    ----------
    input_file_path: str
        Input file path
    Returns
    -------
    meta: dict
        A dictionary with key/value pairs corresponding to the
        .PvDatasets metadata to be written to the parameter set for this
        DataSet type.
        
    """

    logger.debug('scanning for metadata: "{}"'.format(input_file_path))

    base, ext = os.path.splitext(os.path.basename(input_file_path))
    if ext.lower() != '.pvdatasets':
        logger.debug('unsupported file extension: "{}"'.format(ext))
        return

    dm = brukerbiospec.metadata.dicom.findDicomFiles(input_file_path)
    if (dm[1] != None):
        logger.error('brukerbiospec.meta.dicom.findDicomFiles({}) {}\n'
                     .format(input_file_path, dm[1]))
        return []

    dmd = dm[0]                 # get the dict containing DICOM metadata
    if 'numDirs' in dmd and 'numFiles' in dmd and 'numBytes' in dmd:
        logger.debug('{0:4} dirs   {1:6} files   {2:12} bytes\n'
                     .format(dmd['numDirs'], dmd['numFiles'], dmd['numBytes']))

    # Prepare a list of dictionaries containing TruDat metadata to insert
    # into the database.
    #
    # Avoid exceptions - may impact on Celery/Database? (TODO: check docs)

    result = list()

    # Initialise "TruDat open format" data and append it to result list
    #
    namespace = 'http://trudat.uwa.edu.au/schemas/datafile/open-format/1.0'
    nifMeta = dict()
    nifMeta['.schema'] = namespace
    numDicomDirs = dmd['numDirs'] if 'numDirs' in dmd else 0
    nifMeta['NIF_certified'] = 'no' if numDicomDirs == 0 else 'yes'
    result.append(nifMeta)

    # Initialise "TruDat DICOM stats" data and append it to result list
    #
    namespace =
        'http://trudat.uwa.edu.au/schemas/datafile/open-format/dicom-stats/1.0'
    dcmMeta = dict()
    dcmMeta['.schema'] = namespace
    dcmMeta['numDicomDirs'] = numDicomDirs
    if 'numFiles' in dmd:
        dcmMeta['numDicomFiles'] = dmd['numFiles']
    if 'numBytes' in dmd:
        dcmMeta['numDicomBytes'] = dmd['numBytes']
    result.append(dcmMeta)

    return result


if __name__ == '__main__':
    import sys
    import mytardisdatacert.brukerbiospec
    if len(sys.argv) < 2:
        sys.stderr.write('Please specify a .PvDatasets file\n')
        sys.exit(1)
    m = get_meta(sys.argv[1])
    if len(m) == 0:
        sys.exit(1)
    for d in m:
        print d
        # print('{0:4} dirs  {1:6} files  {2:12} bytes\n'
        #       .format(m['numDicomDirs'], m['numDicomFiles'], m['numDicomBytes']))
        

    # TODO: replace code below with Python ZIP scanner

    # for i, img_meta in enumerate(meta_xml.findall('ome:Image', ome_ns)):
    #     smeta = dict()
    #     output_file_path = os.path.join(output_path,
    #                                     input_fname+"_s%s.png" % i)
    #     logger.debug("Generating series %s preview from image: %s"
    #                  % (i, input_fname+ext))
    #     img = previewimage.get_preview_image(input_file_path, omexml, series=i)
    #     logger.debug("Saving series %s preview from image: %s"
    #                  % (i, input_fname+ext))
    #     previewimage.save_image(img, output_file_path, overwrite=True)
    #     logger.debug("Extracting metadata for series %s preview from image: %s"
    #                  % (i, input_fname+ext))
    #     smeta['id'] = img_meta.attrib['ID']
    #     smeta['name'] = img_meta.attrib['Name']
    #     smeta['previewImage'] = output_file_path
    #     for pix_meta in img_meta.findall('ome:Pixels', ome_ns):
    #         for k, v in pix_meta.attrib.iteritems():
    #             if k.lower() not in pix_exc:
    #                 smeta[k.lower()] = v

    #         for c, channel_meta in enumerate(pix_meta.findall('ome:Channel', ome_ns)):
    #             for kc, vc in channel_meta.attrib.iteritems():
    #                 if kc.lower() not in channel_exc:
    #                     if kc.lower() not in smeta:
    #                         smeta[kc.lower()] = ["Channel %s: %s" % (c, vc)]
    #                     else:
    #                         smeta[kc.lower()].append("Channel %s: %s" % (c, vc))