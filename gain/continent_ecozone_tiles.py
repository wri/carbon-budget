import utilities
import subprocess
import rasterio
import numpy as np
from scipy import stats

def create_continent_ecozone_tiles(tile_id):

    print "Processing:", tile_id

    output_dir = 's3://gfw2-data/climate/carbon_model/fao_ecozones/ecozone_continent/20181001/'
    file_name_base_raw = 'fao_ecozones_continents_raw'
    file_name_base_processed = 'fao_ecozones_continents_processed'

    print "Getting extent of biomass tile"
    ymax, xmin, ymin, xmax = utilities.coords(tile_id)
    print "ymax:", ymax, "; ymin:", ymin, "; xmax", xmax, "; xmin:", xmin

    print "Rasterizing ecozone to extent of biomass tile"

    cont_eco_raw = "{0}_{1}".format(file_name_base_raw, tile_id)

    # utilities.rasterize('fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.shp',
    #                                           cont_eco_raw, xmin, ymin, xmax, ymax, '.00025', 'Int16', 'gainEcoCon', '0')

    utilities.upload_final(file_name_base_raw, '{}raw/'.format(output_dir), tile_id)

    # Opens continent-ecozone tile
    with rasterio.open('{}.tif'.format(cont_eco_raw)) as cont_eco_raw_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = cont_eco_raw_src.meta

        # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
        windows = cont_eco_raw_src.block_windows(1)

        # Updates kwargs for the output dataset.
        # Need to update data type to float 32 so that it can handle fractional gain rates
        kwargs.update(
            driver='GTiff',
            count=1,
            compress='lzw',
            nodata=0
        )

        # Opens the output tile, giving it the arguments of the input tiles
        with rasterio.open('{0}_{1}.tif'.format(file_name_base_processed, tile_id), 'w', **kwargs) as dst:

            # Iterates across the windows (1 pixel strips) of the input tile
            for idx, window in windows:

                # Creates windows for each input raster
                cont_eco_raw = cont_eco_raw_src.read(1, window=window)
                print cont_eco_raw

                cont_eco_raw_flat = cont_eco_raw.flatten()
                print "flat"
                print cont_eco_raw_flat

                non_zeros = np.delete(cont_eco_raw_flat, np.where(cont_eco_raw_flat == 0))
                print "non-zeros"
                print non_zeros

                if non_zeros.size < 1:

                    print "All zeros"
                    mode = 0

                else:

                    print "Not all zeros"
                    mode = stats.mode(non_zeros[0])

                print mode

                cont_eco_processed = cont_eco_raw

                cont_eco_processed[cont_eco_processed == 0] = mode

                # Writes the output window to the output
                dst.write_band(1, cont_eco_processed, window=window)

    utilities.upload_final(file_name_base_processed, '{}processed/'.format(output_dir), tile_id)




