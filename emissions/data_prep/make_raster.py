import itertools
import numpy as np
from osgeo import gdal, osr
from collections import OrderedDict


def main():

    # use an ordereddict so we're sure that the list comprehension
    # uses the rasters in the right order
    raster_dict =  OrderedDict((
                    ('forest_model', [0, 1, 2, 3]),
                    ('loss', [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
                    ('peat', [0, 1]),
                    ('burned', [0, 1]),
                    ('ifl', [0, 1]),
                    ('gfw_plantations', [0, 2, 3, 4]),
                    ('fao_eco_zone', [1, 2, 3]),
                    ('climate_zone', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
                    ('histosole', [1, 0]),
                    ('cifor_peat', [1, 0])
                    ))

    arrays = raster_dict.values()
    all_combinations = list(itertools.product(*arrays))

    for i, ras_name in enumerate(raster_dict.keys()):

        # extract just the values for this raster
        ras_values = np.array([x[i] for x in all_combinations])

        # add 988 1 values to make total number 295900 (divisible by 550 and by 2)
        ras_values = np.append(ras_values, np.ones(988))

        # reshape it to be 2d array with 550 rows
        ras_values = ras_values.reshape(550, -1)

        # Get a handle on our out_ras
        out_ras = create_outfile(ras_name + '.tif', gdal.GDT_Byte, 1)
        out_band = out_ras.GetRasterBand(1)
        
        # grab row count + set blocksize == 2
        rows = ras_values.shape[0]
        yblocksize = 2

        print ras_name

        # iterate over the numpy array 
        for y in range(0, rows, yblocksize):
            to_write = ras_values[y: y+yblocksize]
            out_ras.GetRasterBand(1).WriteArray(to_write, 0, y)


def create_outfile(output_raster, output_datatype, band_count):

    # set to match Hansen loss tile
    geotransform = (0.0, 0.00025, 0.0, 0.0, 0.0, -0.00025)
    originX = geotransform[0]
    originY = geotransform[3]
    pixelWidth = geotransform[1]
    pixelHeight = geotransform[5]

    # set cols and rows in new raster
    cols = 538
    rows = 550

    driver = gdal.GetDriverByName('GTiff')
    options = ['COMPRESS=LZW']

    outRaster = driver.Create(output_raster, cols, rows, band_count, output_datatype, options)
    outRaster.SetGeoTransform((originX, pixelWidth, 0, originY, 0, pixelHeight))

    proj_string = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]'

    outRasterSRS = osr.SpatialReference()
    outRasterSRS.ImportFromWkt(proj_string)
    outRaster.SetProjection(outRasterSRS.ExportToWkt())

    return outRaster

if __name__ == '__main__':
    main()
        