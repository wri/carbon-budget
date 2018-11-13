import subprocess

import util
import sys
sys.path.append('../')
import universal_util
import constants_and_names


def create_input_files(tile_id):

    print "get extent of", tile_id
    ymax, xmin, ymin, xmax = universal_util.coords(tile_id)

    print "clip soil"
    extra_param = ['-tr', '.00025', '.00025', '-dstnodata', '0']
    clip_soil_tile = util.clip('hwsd_oc_final.tif', '{}_soil.tif'.format(tile_id), xmin, ymin, xmax, ymax, extra_param)

    print "removing no data flag from soil"
    cmd = ['gdal_edit.py', '-unsetnodata', clip_soil_tile]
    subprocess.check_call(cmd)

    print "uploading soil tile to s3"
    util.upload(clip_soil_tile, constants_and_names.soil_C_dir)

    print "rasterizing eco zone"
    rasterized_eco_zone_tile = util.rasterize('fao_ecozones_bor_tem_tro.shp',
                                              "{}_fao_ecozones_bor_tem_tro.tif".format(tile_id),
                                              xmin, ymin, xmax, ymax, '.008', 'Byte', 'recode', '0')

    print "resampling eco zone"
    resampled_ecozone = util.resample(rasterized_eco_zone_tile, "{0}_{1}.tif".format(tile_id, constants_and_names.pattern_fao_ecozone_processed))

    print "upload ecozone to input data"
    util.upload(resampled_ecozone, constants_and_names.fao_ecozone_processed_dir)

    print "clipping srtm"
    tile_srtm = util.clip('srtm.vrt', '{}_srtm.tif'.format(tile_id), xmin, ymin, xmax, ymax)

    print "resampling srtm"
    tile_res_srtm = util.resample(tile_srtm, '{0}_{1}.tif'.format(tile_id, constants_and_names.pattern_srtm))

    print "upload srtm to input data"
    util.upload(tile_res_srtm, constants_and_names.srtm_processed_dir)

    print "clip precip"
    clipped_precip_tile = util.clip('add_30s_precip.tif', '{}_clip_precip.tif'.format(tile_id), xmin, ymin, xmax, ymax)

    print "resample precip"
    resample_precip_tile = util.resample(clipped_precip_tile, '{0}_{1}.tif'.format(tile_id, constants_and_names.pattern_precip))

    print "upload precip to input data"
    util.upload(resample_precip_tile, constants_and_names.precip_processed_dir)
