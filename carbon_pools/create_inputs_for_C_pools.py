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

    print "Rasterizing ecozone for", tile_id
    uu.rasterize('fao_ecozones_bor_tem_tro.shp',
                                              "{0}_{1}.tif".format(tile_id, cn.pattern_fao_ecozone_processed),
                                              xmin, ymin, xmax, ymax, 1024, cn.Hansen_res, 'Int16', '0', 'recode')

    print "Clipping srtm for", tile_id
    uu.warp_to_Hansen('srtm.vrt', '{0}_{1}.tif'.format(tile_id, cn.pattern_elevation), xmin, ymin, xmax, ymax)

    print "Clipping precipitation for", tile_id
    uu.warp_to_Hansen('add_30s_precip.tif', '{0}_{1}.tif'.format(tile_id, cn.pattern_precip), xmin, ymin, xmax, ymax)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_precip)
