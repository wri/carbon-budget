from boto.s3.connection import S3Connection
from osgeo import gdal

conn = S3Connection(host="s3.amazonaws.com")
bucket = conn.get_bucket('gfw-files')



def check_output_exists(carbon_pool):

    prefix = 'sam/carbon_budget/carbon_030218/30tcd/{}/tif/'.format(carbon_pool)


    full_path_list = [key.name for key in bucket.list(prefix='{}'.format(prefix))]

    filename_only_list = [x.split('/')[-1] for x in full_path_list]
    return filename_only_list

def get min_max(tif):
    gtif = gdal.Open(tif)
    srcband = gtif.GetRasterBand(1)
    stats = srcband.GetStatistics(True, True)
    min_val = stats[0]
    max_val = stats[1]

    if min_val < 1000 < max_val:
        valid_raster = True
    else:
        valid_rater = False
