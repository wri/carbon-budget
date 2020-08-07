from subprocess import Popen, PIPE, STDOUT, check_call
from osgeo import gdal
import utilities
import glob
import shutil
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def stack_ba_hv(hv_tile):

    for year in range(2019, 2020): # End year is not included in burn year product

        # # Download hdf files from s3 into folders by h and v
        # output_dir = utilities.makedir('{0}/{1}/raw/'.format(hv_tile, year))
        # utilities.download_df(year, hv_tile, output_dir)

        # convert hdf to array
        hdf_files = glob.glob(output_dir + "*hdf")

        uu.print_log(hdf_files)

        if len(hdf_files) > 0:
            array_list = []
            for hdf in hdf_files:
                uu.print_log("converting hdf to array")
                array = utilities.hdf_to_array(hdf)
                array_list.append(array)

            # stack arrays, get 1 raster for the year and tile
            stacked_year_array = utilities.stack_arrays(array_list)
            max_stacked_year_array = stacked_year_array.max(0)

            # convert stacked month arrays to 1 raster for the year
            template_hdf = hdf_files[0]

            year_folder = utilities.makedir('{0}/{1}/stacked/'.format(hv_tile, year))

            stacked_year_raster = utilities.array_to_raster(hv_tile, year, max_stacked_year_array, template_hdf,
                                                            year_folder)

            # upload to somewhere on s3
            cmd = ['aws', 's3', 'cp', stacked_year_raster, 's3://gfw2-data/climate/carbon_model/other_emissions_inputs/burn_year/burn_year/20190322/']
            uu.log_subprocess_output_full(cmd)

            # remove files
            shutil.rmtree(output_dir)

        else:
            pass
