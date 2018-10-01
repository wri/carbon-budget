import subprocess
import glob
import os

from boto.s3.connection import S3Connection
from osgeo import gdal
conn = S3Connection(host="s3.amazonaws.com")
bucket = conn.get_bucket('gfw-files')

import mask_raster


def check_output_exists(carbon_pool):

    prefix = 'sam/carbon_budget/carbon_030218/30tcd/{}/tif/'.format(carbon_pool)

    full_path_list = [key.name for key in bucket.list(prefix='{}'.format(prefix))]

    filename_only_list = [x.split('/')[-1] for x in full_path_list]
    return filename_only_list


def download_s3(carbon_pool, tile_name):
    s3_loc = 's3://gfw-files/sam/carbon_budget/carbon_030218/30tcd/{0}/tif/{1}'.format(carbon_pool, tile_name)
    cmd = ['aws', 's3', 'cp', s3_loc, '.']
    subprocess.check_call(cmd)
    
    return tile_name
    
    
def get_min_max(tif):
    gtif = gdal.Open(tif)
    srcband = gtif.GetRasterBand(1)
    stats = srcband.GetStatistics(True, True)
    min_val = stats[0]
    max_val = stats[1]
    print min_val
    print max_val
    if min_val >= 0 and max_val < 1000:
        valid_raster = True
    else:
        valid_raster = False
    return valid_raster


def qc_minmax_vals(tilename):
    tile_name = download_s3('deadwood', tilename)
    valid_raster = get_min_max(tile_name)
    tileid = tilename[:8]
    
    if not valid_raster:
        cmd = ['touch', '{}.txt'.format(tileid)]
        subprocess.check_call(cmd)
        print "not good, remasking"            
        mask_raster.mask_raster(tileid)
    print tileid
    files = glob.glob('{}*tif*'.format(tileid))
    print files
    for f in files:
        print "removing files!!"
        os.remove(f)
        
