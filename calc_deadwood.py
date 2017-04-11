import subprocess
import datetime

import get_extent

# start with biomass tile
tile_id = '00N_000E'
biomass_tile = r"S:\biomass\mtc02\00N_000E_mtc02.tif"

# get extent
xmin, ymin, xmax, ymax = get_extent.get_extent(biomass_tile)

# rasterize climate zone
fao_eco_zones = r'U:\sgibbes\workplan_2017\dead_wood_carbon_pool\FAO_Ecozones\fao_ecozones.shp'

rasterized_eco_zone_tile = "{}_ecozone.tif".format(tile_id)

rasterize = ['gdal_rasterize', '-co', 'COMPRESS=LZW', '-te', str(xmin), str(ymin), str(xmax), str(ymax), 
'-tr', '0.008', '0.008', '-ot', 'Byte', '-a', 'ATTRIBUTE', '-a_nodata', 
'0', fao_eco_zones, rasterized_eco_zone_tile]

print "rasterizing eco zone"
start = datetime.datetime.now()
subprocess.check_call(rasterize)
print "elapsed time to rasterize: {}".format(datetime.datetime.now() - start)
# 21 min to rasterize with .00025

# resample eco zone raster
resampled_ecozone =  "{}_res_ecozone.tif".format(tile_id)
resample_ecozone = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', rasterized_eco_zone_tile, resampled_ecozone]
print "resampling eco zone"
start = datetime.datetime.now()
subprocess.check_call(resample_ecozone)
print "elapsed time to resample: {}".format(datetime.datetime.now() - start)

# tile srtm
tile_srtm = '{}_srtm.tif'.format(tile_id)
clip_srtm = ['gdal_translate', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin), '-co', 'COMPRESS=LZW', 'srtm.vrt', tile_srtm]
# 20 seconds to rasterize and resample at .008
#subprocess.check_call(clip_srtm)

# resample srtm
tile_res_srtm = '{}_res_srtm.tif'.format(tile_id)
resample = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', tile_srtm, tile_res_srtm]
#subprocess.check_call(resample)


# grab precip tiles...not sure which format yet
# send 1) biomass 2) rasterized climate zone 3) elevation 4) precip to "create_deadwood_tile.cpp"
# delete intermediate tiles# output is a tile matching res/extent of biomass, each pixel is mg deadwood biomass /ha