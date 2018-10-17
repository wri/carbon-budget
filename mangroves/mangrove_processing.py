import utilities
import subprocess

def create_mangrove_tiles(tile_id):

    xmin, xmax, ymin, ymax = utilities.coords(tile_id)

    cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-co', 'COMPRESS=LZW', '-tr', '0.00025', '0.00025', '-tap', '-te', xmin,
            ymin, xmax, ymax, '-dstnodata', '-9999', '-overwrite', utilities.mangrove_vrt, '{0}_{1}.tif'.format(utilities.mangrove_tile_out, tile_id)]
    subprocess.check_call(cmd)

    utilities.upload_final(utilities.mangrove_tile_out, utilities.out_dir, tile_id)

