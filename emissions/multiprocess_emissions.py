import multiprocessing
import subprocess
import calc_emissions
import utilities
import tile_peat_dict
import sys


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i + n]

# print chunks(['cat', 'dog', 'mouse', 'rat'], 2)
# # sys.exit()

biomass_tile_list = utilities.tile_list('s3://gfw-files/sam/carbon_budget/carbon_030218/total_carbon/')
biomass_tile_list = ['00N_000E'] # test tile
biomass_tile_list = ['30S_080W'] # test tile
print 'Biomass tile list is: ' + str(biomass_tile_list)
tiles_in_chunk = 8

for chunk in chunks(biomass_tile_list, tiles_in_chunk):

    print 'Chunk is: ' + str(chunk)

    with open('status.txt', 'a') as textfile:
        textfile.write(str(chunk) + "\n")

    for tile_id in chunk:
        print '     tile_id is: ' + str(tile_id)

        # download files
        peat_file = tile_peat_dict.tile_peat_dict(tile_id) # based on tile id, know which peat file to download (hwsd, hist, jukka)

        files = {'carbon_pool':['bgc', 'carbon', 'deadwood', 'soil', 'litter'], 'data_prep': [peat_file, 'fao_ecozones_bor_tem_tro', 'ifl_2000', 'gfw_plantations', 'tsc_model', 'climate_zone'], 'burned_area':['burn_loss_year']}

        utilities.download(files, tile_id)

        #download hansen tile
        hansen_tile = utilities.wgetloss(tile_id)

        #if idn plant tile downloaded, mask loss with plantations because we know that idn gfw_plantations
        # were established in yr 2000.
        if tile_id in ['00N_090E', '00N_100E', '00N_110E', '00N_120E', '00N_130E', '00N_140E', '10N_090E', '10N_100E', '10N_110E', '10N_120E', '10N_130E', '10N_140E']:
            print "cutting out plantations in Indonesia, Malaysia"
            utilities.mask_loss(tile_id)
    
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=tiles_in_chunk)
    pool.map(calc_emissions.calc_emissions, chunk)
