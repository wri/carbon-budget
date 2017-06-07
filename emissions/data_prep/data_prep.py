import utilities

def data_prep(tile_id, tile_list): 
    tile_list = []
    shapefiles_to_rasterize = ['fao_ecozones_bor_tem_tro', 'ifl_2000', 'peatland_drainage_proj', 'gfw_plantations']
    print tile_list
    
    for shapefile in shapefiles_to_rasterize:
        rasterized_tiles = utilities.rasterize(shapefile, tile_id, tile_list)
        
    rasters_to_resample = ['hwsd_histosoles', 'forest_model', 'climate_zone', 'cifor_peat_mask']
    
    for raster in rasters_to_resample:
        resampled_tiles = utilities.resample_clip(raster, tile_id, tile_list)
        
    # upload to s3
    for tile in tile_list:
        print "uploading {} to s3".format(tile)
        cmd = ['aws', 's3', 'mv', tile, 's3://gfw-files/sam/carbon_budget/tile_inputs/']
        subprocess.check_call(cmd)
        
data_prep("10N_100E", [])