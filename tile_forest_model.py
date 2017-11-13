import subprocess

def coords(tile_id):
    NS = tile_id.split("_")[0][-1:]
    EW = tile_id.split("_")[1][-1:]

    if NS == 'S':
        ymax =-1*int(tile_id.split("_")[0][:2])
    else:
        ymax = int(str(tile_id.split("_")[0][:2]))
    
    if EW == 'W':
        xmin = -1*int(str(tile_id.split("_")[1][:3]))
    else:
        xmin = int(str(tile_id.split("_")[1][:3]))
        
    
    ymin = str(int(ymax) - 10)
    xmax = str(int(xmin) + 10)
    
    return str(ymax), str(xmin), str(ymin), str(xmax)
    
    
def iterate_tiles(tile_id):

    print "running: {}".format(tile_id)
    ymax, xmin, ymin, xmax = coords(tile_id)
    forest_model = "Goode_FinalClassification_15_50uncertain_expanded.tif"
    out = '{}_res_forest_model.tif'.format(tile_id)
    rasterize = ['gdalwarp', '-t_srs', 'EPSG:4326', '-tr', '.00025', '.00025', '-tap', '-te', xmin, ymin, xmax, ymax, '-dstnodata', '0', forest_model, 'test.tif']
    
    subprocess.check_call(rasterize)
    
    s3_folder = 's3://gfw-files/sam/carbon_budget/data_inputs/forest_model/'
    cmd = ['aws', 's3', 'mv', out, s3_folder]
    subprocess.check_call(cmd)
    
    
#iterate_tiles('10N_110E')