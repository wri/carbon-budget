import util
import datetime
import sys
sys.path.append('../')
import universal_util as uu
import constants_and_names as cn


def create_input_files(tile_id):

    # Start time
    start = datetime.datetime.now()

    print "Getting extent of", tile_id
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    print "Rasterizing ecozone"
    util.rasterize('fao_ecozones_bor_tem_tro.shp',
                                              "{0}_{1}.tif".format(tile_id, cn.pattern_fao_ecozone_processed),
                                              str(xmin), str(ymin), str(xmax), str(ymax), cn.Hansen_res, 'Int16', '0', 'recode')

    # print "Resampling eco zone"
    # resampled_ecozone = util.resample(rasterized_eco_zone_tile, "{0}_{1}.tif".format(tile_id, cn.pattern_fao_ecozone_processed))

    # print "Uploading processed ecozone"
    # util.upload(resampled_ecozone, cn.fao_ecozone_processed_dir)

    print "Clipping srtm"
    uu.warp_to_Hansen('srtm.vrt', '{0}_{1}.tif'.format(tile_id, cn.pattern_elevation), xmin, ymin, xmax, ymax)

    # print "Resampling srtm"
    # tile_res_srtm = util.resample(tile_srtm, '{0}_{1}.tif'.format(tile_id, cn.pattern_srtm))
    #
    # print "Uploading processed srtm"
    # util.upload(tile_res_srtm, cn.srtm_processed_dir)

    print "Clipping precipitation"
    uu.warp_to_Hansen('add_30s_precip.tif', '{0}_{1}.tif'.format(tile_id, cn.pattern_precip), xmin, ymin, xmax, ymax)

    # print "Resampling precipitation"
    # resample_precip_tile = util.resample(clipped_precip_tile, '{0}_{1}.tif'.format(tile_id, cn.pattern_precip))
    #
    # print "Uploading processed precipitation"
    # util.upload(resample_precip_tile, cn.precip_processed_dir)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_precip)
