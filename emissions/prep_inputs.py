import datetime
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def data_prep(tile_id):

    # Start time
    start = datetime.datetime.now()

    print "Getting extent of", tile_id
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    print "Warping climate zone tile", tile_id
    uu.warp_to_Hansen(cn.climate_zone_raw, '{0}_{1}.tif'.format(tile_id, cn.pattern_climate_zone), xmin, ymin, xmax, ymax, 'Byte')

    print "Warping IDN/MYS pre-2000 plantation tile", tile_id
    uu.warp_to_Hansen('{}.tif'.format(cn.pattern_plant_pre_2000_raw), '{0}_{1}.tif'.format(tile_id, cn.pattern_climate_zone),
                                      xmin, ymin, xmax, ymax, 'Byte')

    print "Warping tree cover loss tile", tile_id
    uu.warp_to_Hansen('{}.tif'.format(cn.pattern_drivers_raw), '{0}_{1}.tif'.format(tile_id, cn.pattern_drivers), xmin, ymin, xmax, ymax, 'Byte')

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_drivers)
