from subprocess import Popen, PIPE, STDOUT, check_call
from osgeo import gdal
import utilities
import glob
import shutil


def stack_ba_hv(hv_tile):

    for year in range(2000, 2019):

        # download hdf files
        output_dir = utilities.makedir('{0}/{1}/raw/'.format(hv_tile, year))
        utilities.download_df(year, hv_tile, output_dir)

        # convert hdf to array
        hdf_files = glob.glob(output_dir + "*hdf")

        if len(hdf_files) > 0:
            array_list = []
            for hdf in hdf_files:
                print_log("converting hdf to array")
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

            # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
            process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
            with process.stdout:
                uu.log_subprocess_output(process.stdout)

            # remove files
            shutil.rmtree(output_dir)

        else:
            pass
