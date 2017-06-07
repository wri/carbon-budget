import utilities
import subprocess

def upload_tile(folder, tile):

    dst_folder = 's3://gfw-files/sam/carbon_budget/data_inputs/{}/'.format(folder)
    cmd = ['aws', 's3', 'mv', tile, dst_folder]
    subprocess.check_call(cmd)
    
    
def data_prep(tile_id): 

    shapefiles_to_rasterize = ['fao_ecozones_bor_tem_tro', 'ifl_2000', 'peatland_drainage_proj', 'gfw_plantations']
    for shapefile in shapefiles_to_rasterize:
        rasterized_tile = utilities.rasterize(shapefile, tile_id)
        
        upload_tile(shapefile, rasterized_tile)
        
    rasters_to_resample = ['hwsd_histosoles', 'forest_model', 'climate_zone', 'cifor_peat_mask']
    for raster in rasters_to_resample:
        resampled_tile = utilities.resample_clip(raster, tile_id)
        
        upload_tile(raster, resampled_tile)
        