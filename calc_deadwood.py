import subprocess
import datetime

import get_extent

# start with biomass tile
start = datetime.datetime.now()
tile_id = '00N_000E'
biomass_tile = r"S:\biomass\tiles\00N_000E.tif"

# get extent
print "get extent of biomass tile"
xmin, ymin, xmax, ymax = get_extent.get_extent(biomass_tile)

# rasterize climate zone
fao_eco_zones = r'U:\sgibbes\workplan_2017\dead_wood_carbon_pool\fao_ecozones_reclass.shp'
rasterized_eco_zone_tile = "{}_ecozone.tif".format(tile_id)
rasterize = ['gdal_rasterize', '-co', 'COMPRESS=LZW', '-te', str(xmin), str(ymin), str(xmax), str(ymax), 
'-tr', '0.008', '0.008', '-ot', 'Byte', '-a', 'ATTRIBUTE', '-a_nodata', 
'0', fao_eco_zones, rasterized_eco_zone_tile]

print "rasterizing climate zone"
subprocess.check_call(rasterize)

# resample eco zone raster
resampled_ecozone =  "{}_res_ecozone.tif".format(tile_id)
resample_ecozone = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', rasterized_eco_zone_tile, resampled_ecozone]
print "resampling eco zone"
subprocess.check_call(resample_ecozone)

# tile srtm
tile_srtm = '{}_srtm.tif'.format(tile_id)
srtm = r'U:\sgibbes\workplan_2017\dead_wood_carbon_pool\srtm.vrt'
clip_srtm = ['gdal_translate', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin), '-co', 'COMPRESS=LZW', srtm, tile_srtm]
#subprocess.check_call(clip_srtm)

# resample srtm
tile_res_srtm = '{}_res_srtm.tif'.format(tile_id)
resample = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', tile_srtm, tile_res_srtm]
#subprocess.check_call(resample)


# grab precip tiles...not sure which format yet
# send 1) biomass 2) rasterized climate zone 3) elevation 4) precip to "create_deadwood_tile.cpp"


# delete intermediate tiles# output is a tile matching res/extent of biomass, each pixel is mg deadwood biomass /ha


print "elapsed time: {}".format(datetime.datetime.now() - start)