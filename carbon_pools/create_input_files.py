import subprocess

import util


def create_input_files(tile_id, carbon_budget_input_data_dir, biomass_tile):
    print "get extent of biomass tile"
    print biomass_tile
    xmin, ymin, xmax, ymax = util.get_extent(biomass_tile)

    print "clip soil"
    extra_param = ['-tr', '.00025', '.00025', '-a_nodata', '0']
    clip_soil_tile = util.clip('hwsd_oc_final.tif', '{}_soil.tif'.format(tile_id), xmin, ymin, xmax, ymax, extra_param)

    print "removing no data flag from soil"
    cmd = ['gdal_edit.py', '-unsetnodata', clip_soil_tile]
    subprocess.check_call(cmd)

    print 'uploading soil tile to s3'
    util.upload(clip_soil_tile, carbon_budget_input_data_dir + 'soil/')

    print "rasterizing eco zone"
    rasterized_eco_zone_tile = util.rasterize('fao_ecozones_bor_tem_tro.shp',
                                              "{}_fao_ecozones_bor_tem_tro.tif".format(tile_id),
                                              xmin, ymin, xmax, ymax, '.008', 'Byte', 'recode', '0')

    print "resampling eco zone"
    resampled_ecozone = util.resample(rasterized_eco_zone_tile, "{}_res_fao_ecozones_bor_tem_tro.tif".format(tile_id))

    print "upload ecozone to input data"
    util.upload(resampled_ecozone, carbon_budget_input_data_dir + 'fao_ecozones_bor_tem_tro/')

    print "clipping srtm"
    tile_srtm = util.clip('srtm.vrt', '{}_srtm.tif'.format(tile_id), xmin, ymin, xmax, ymax)

    print "resampling srtm"
    tile_res_srtm = util.resample(tile_srtm, '{}_res_srtm.tif'.format(tile_id))

    print "upload srtm to input data"
    util.upload(tile_res_srtm, carbon_budget_input_data_dir + 'srtm/')

    print "clip precip"
    clipped_precip_tile = util.clip('add_30s_precip.tif', '{}_clip_precip.tif'.format(tile_id), xmin, ymin, xmax, ymax)

    print "resample precip"
    resample_precip_tile = util.resample(clipped_precip_tile, '{}_res_precip.tif'.format(tile_id))

    print "upload precip to input data"
    util.upload(resample_precip_tile, carbon_budget_input_data_dir + 'precip/')