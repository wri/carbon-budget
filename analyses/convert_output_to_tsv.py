### Joins tiles from the model together and converts them to tsvs so that they can be put through Hadoop
### for zonal statistics

import utilities
import subprocess
import sys
sys.path.append('../')
import constants_and_names

### git clone https://github.com/wri/raster-to-tsv
### git pull https://github.com/wri/raster-to-tsv float_output
### git clone https://github.com/wri/carbon-budget
### cd carbon-budget/analyses/

print "Making list of biomass tiles..."
# biomass_tile_list = list_tiles()
biomass_tile_list = ['10N_080W', '40N_120E', '00N_000E'] # test tiles
# biomass_tile_list = ['00N_000E'] # test tiles
print "  Biomass tile list retrieved. There are", len(biomass_tile_list), "biomass tiles total."

# Input files will be downloaded to here
local_dir = r'/home/ubuntu/data/'

# # For downloading all tiles in the input folders
# download_list = [constants_and_names.net_emis_dir, constants_and_names.emissions_total_dir,
#                  constants_and_names.annual_gain_combo_dir, constants_and_names.cumul_gain_combo_dir,
#                  constants_and_names.tcd_dir, constants_and_names.loss_dir]
#
# for input in download_list:
#     utilities.s3_folder_download('{}'.format(input), local_dir)


# For copying individual tiles to spot machine for testing
for tile in biomass_tile_list:

    utilities.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.net_emis_dir, constants_and_names.pattern_net_emis, tile), local_dir)  # cumulative aboveand belowground carbon gain for all forest types
    utilities.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.emissions_total_dir, tile, constants_and_names.pattern_emissions_total), local_dir)  # emissions from all drivers
    utilities.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.annual_gain_combo_dir, constants_and_names.pattern_annual_gain_combo, tile), local_dir)  # annual gain rate
    utilities.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.cumul_gain_combo_dir, constants_and_names.pattern_cumul_gain_combo, tile), local_dir)  # cumulative gain
    utilities.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.tcd_dir, constants_and_names.pattern_tcd, tile), local_dir)  # tree cover density
    utilities.s3_file_download('{0}{1}.tif'.format(constants_and_names.loss_dir, tile), local_dir)  # tree cover loss

out_locn = 's3://gfw2-data/climate/carbon_model/test_output_tsvs/'

# Iterates through tiles to join the desired rasters and convert them to tsvs
for tile in biomass_tile_list:

    print "Processing tile", tile

    # Names of the files that are used for this analysis
    net_emis = '{0}{1}_{2}.tif'.format(local_dir, constants_and_names.pattern_net_emis, tile)
    gross_emis = '{0}{1}_{2}.tif'.format(local_dir, tile, constants_and_names.pattern_emissions_total)
    annual_gain = '{0}{1}_{2}.tif'.format(local_dir, constants_and_names.pattern_annual_gain_combo, tile)
    cumul_gain = '{0}{1}_{2}.tif'.format(local_dir, constants_and_names.pattern_cumul_gain_combo, tile)
    tcd = '{0}{1}_{2}.tif'.format(local_dir, constants_and_names.pattern_tcd, tile)
    tcl = '{0}{1}.tif'.format(local_dir, tile)

    # Location of write-tsv.py
    ras_cwd = r'/home/ubuntu/raster-to-tsv'

    print "Joining annual gain and tcd2000 for", tile
    ras_to_vec_cmd = ['python', 'write-tsv.py', '--datasets', annual_gain, tcd, '--s3-output', '{}annualGain_tcd2000/'.format(out_locn)]
    ras_to_vec_cmd += ['--threads', '20', '--csv-process', 'emissions_gain', '--prefix', 'annualGain_tcd2000', '--separate']
    subprocess.check_call(ras_to_vec_cmd, cwd=ras_cwd)

    print "Joining cumulative gain and tcd2000 for", tile
    ras_to_vec_cmd = ['python', 'write-tsv.py', '--datasets', cumul_gain, tcd, '--s3-output', '{}cumulGain_tcd2000/'.format(out_locn)]
    ras_to_vec_cmd += ['--threads', '20', '--csv-process', 'emissions_gain', '--prefix', 'cumulGain_tcd2000', '--separate']
    subprocess.check_call(ras_to_vec_cmd, cwd=ras_cwd)

    print "Joining net emissions and tcd2000 and tree cover loss for", tile
    ras_to_vec_cmd = ['python', 'write-tsv.py', '--datasets', net_emis, tcd, tcl, '--s3-output', '{}netEmis_tcd2000_tcl/'.format(out_locn)]
    ras_to_vec_cmd += ['--threads', '20', '--csv-process', 'emissions_gain', '--prefix', 'netEmis_tcd2000_treeCoverLoss', '--separate']
    subprocess.check_call(ras_to_vec_cmd, cwd=ras_cwd)

    print "Joining gross emissions and tcd2000 and tree cover loss for", tile
    ras_to_vec_cmd = ['python', 'write-tsv.py', '--datasets', gross_emis, tcd, tcl, '--s3-output', '{}grossEmis_tcd2000_tcl/'.format(out_locn)]
    ras_to_vec_cmd += ['--threads', '20', '--csv-process', 'emissions_gain', '--prefix', 'grossEmis_tcd2000_treeCoverLoss', '--separate']
    subprocess.check_call(ras_to_vec_cmd, cwd=ras_cwd)
