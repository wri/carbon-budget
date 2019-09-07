### Joins tiles from the model together and converts them to tsvs so that they can be put through Hadoop
### for zonal statistics. This does not call multiprocessor because the write-tsv.py script can use
### use multiple processors at once.

import subprocess
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

### Need to clone the repo that actually does the conversion, as well as this repo, which has the wrapper
### git clone https://github.com/wri/raster-to-tsv
### git clone https://github.com/wri/carbon-budget
### cd carbon-budget/analyses/

print "Making list of biomass tiles..."
biomass_tile_list = uu.tile_list(cn.natrl_forest_biomass_2000_dir)
# biomass_tile_list = ['10N_080W', '40N_120E', '00N_000E'] # test tiles
# biomass_tile_list = ['00N_140W', '00N_000E'] # test tiles
print "  Biomass tile list retrieved. There are", len(biomass_tile_list), "biomass tiles total."

# Input files will be downloaded to here
local_dir = r'/home/ubuntu/data/'

# For downloading all tiles in the input folders
download_list = [
                 cn.annual_gain_AGB_BGB_all_types_dir,
                 cn.cumul_gain_AGCO2_BGCO2_all_types_dir,
                 cn.net_flux_dir,
                 cn.gross_emissions_dir,
                 cn.loss_dir,
                 cn.tcd_dir
                 ]

for input in download_list:
    uu.s3_folder_download('{}'.format(input), local_dir)


# Location of write-tsv.py
ras_cwd = r'/home/ubuntu/raster-to-tsv'

# # For copying individual tiles to spot machine for testing
# for tile in biomass_tile_list:
#
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_AGB_BGB_all_types_dir, cn.pattern_annual_gain_AGB_BGB_all_types, tile), local_dir)  # annual gain rate
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_AGCO2_BGCO2_all_types_dir, cn.pattern_cumul_gain_AGCO2_BGCO2_all_types, tile), local_dir)  # cumulative gain
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.net_flux_dir, cn.pattern_net_flux, tile), local_dir)  # cumulative aboveand belowground carbon gain for all forest types
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.gross_emissions_dir, tile, cn.pattern_gross_emissions), local_dir)  # emissions from all drivers
#     uu.s3_file_download('{0}{1}.tif'.format(cn.loss_dir, tile), local_dir)  # tree cover loss
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.tcd_dir, cn.pattern_tcd, tile), local_dir)  # tree cover density


# Iterates through annual gain tiles to join the tcd rasters and convert them to tsvs
for tile in biomass_tile_list:

    print "Processing annual gain for biomass tile", tile

    # Names of the files that are used for this analysis
    annual_gain = '{0}{1}_{2}.tif'.format(local_dir, cn.pattern_annual_gain_AGB_BGB_all_types, tile)
    tcd = '{0}{1}_{2}.tif'.format(local_dir, cn.pattern_tcd, tile)

    print "Joining annual gain and tcd2000 for", tile
    # # Code to run write-tsv.py directly on one annual gain tile
    # python write-tsv.py --datasets annual_gain_rate_AGB_BGB_t_ha_all_forest_types_00N_000E.tif Hansen_GFC2014_treecover2000_00N_000E.tif --s3-output
    # s3://gfw2-data/climate/carbon_model/test_output_tsvs/annualGain_tcd2000_v2/ --threads 3 --csv-process emissions_gain --prefix annualGain_tcd2000_00N_000E --separate
    ras_to_vec_cmd = ['python', 'write-tsv.py', '--datasets', annual_gain, tcd, '--s3-output', '{}annualGain_tcd2000/'.format(cn.tsv_output_dir)]
    ras_to_vec_cmd += ['--threads', '30', '--csv-process', 'emissions_gain', '--prefix', '{}_annualGain_tcd2000'.format(tile), '--separate']
    subprocess.check_call(ras_to_vec_cmd, cwd=ras_cwd)

    os.remove(annual_gain)

# Iterates through cumulative gain tiles to join the tcd rasters and convert them to tsvs
for tile in biomass_tile_list:
    print "Processing cumulative gain for tile", tile

    # Names of the files that are used for this analysis
    cumul_gain = '{0}{1}_{2}.tif'.format(local_dir, cn.pattern_cumul_gain_AGCO2_BGCO2_all_types, tile)
    tcd = '{0}{1}_{2}.tif'.format(local_dir, cn.pattern_tcd, tile)

    print "Joining cumulative gain and tcd2000 for", tile
    ras_to_vec_cmd = ['python', 'write-tsv.py', '--datasets', cumul_gain, tcd, '--s3-output', '{}cumulGain_tcd2000/'.format(cn.tsv_output_dir)]
    ras_to_vec_cmd += ['--threads', '30', '--csv-process', 'emissions_gain', '--prefix', '{}_cumulGain_tcd2000'.format(tile), '--separate']
    subprocess.check_call(ras_to_vec_cmd, cwd=ras_cwd)

    os.remove(cumul_gain)

# Iterates through net emissions tiles to join the tcd rasters and convert them to tsvs
for tile in biomass_tile_list:
    print "Processing net emissions for tile", tile

    # Names of the files that are used for this analysis
    net_emis = '{0}{1}_{2}.tif'.format(local_dir, cn.pattern_net_flux, tile)
    tcd = '{0}{1}_{2}.tif'.format(local_dir, cn.pattern_tcd, tile)

    print "Joining net emissions and tcd2000", tile
    ras_to_vec_cmd = ['python', 'write-tsv.py', '--datasets', net_emis, tcd, '--s3-output', '{}netEmis_tcd2000/'.format(cn.tsv_output_dir)]
    ras_to_vec_cmd += ['--threads', '30', '--csv-process', 'emissions_gain', '--prefix', '{}_netEmis_tcd2000'.format(tile), '--separate']
    subprocess.check_call(ras_to_vec_cmd, cwd=ras_cwd)

    os.remove(net_emis)

# Iterates through gross emissions tiles to join the tcd and tcl rasters and convert them to tsvs
for tile in biomass_tile_list:
    print "Processing gross emissions for tile", tile

    # Names of the files that are used for this analysis
    gross_emis = '{0}{1}_{2}.tif'.format(local_dir, tile, cn.pattern_gross_emissions)
    tcd = '{0}{1}_{2}.tif'.format(local_dir, cn.pattern_tcd, tile)
    tcl = '{0}{1}.tif'.format(local_dir, tile)

    print "Joining gross emissions and tcd2000 and tree cover loss for", tile
    ras_to_vec_cmd = ['python', 'write-tsv.py', '--datasets', gross_emis, tcd, tcl, '--s3-output', '{}grossEmis_tcd2000_tcl/'.format(cn.tsv_output_dir)]
    ras_to_vec_cmd += ['--threads', '40', '--csv-process', 'emissions_gain', '--prefix', '{}_grossEmis_tcd2000_treeCoverLoss'.format(tile), '--separate']
    subprocess.check_call(ras_to_vec_cmd, cwd=ras_cwd)

    os.remove(gross_emis)
