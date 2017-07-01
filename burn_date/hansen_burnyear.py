"""
a1 = [1,2,3]
a2 = [3,2,1]
lossarray = [1,3,9]
stack = np.stack((a1, a2))
lossarray_min1 = np.subtract(lossarray, 1)
stack_con =(stack >= lossarray_min1) & (stack <= lossarray)
stack_con2 = stack_con * stack
final = stack_con2.max(0)
- then, write it out to a raster. 

"""
hansen_burnyear(tile_id):
    
    # download burn year from s3://gfw-files/sam/carbon_budget/burn_year_10degtiles_modisproj/ba_2002_80N_110W.tif
    burn_year_tiles = 's3://gfw-files/sam/carbon_budget/burn_year_10degtiles_modisproj/'
    include = 'ba_*_{}.tif'.format(tile_id)
    burn_tiles_dir = 'burn_tiles'
    if not os.path.exists(burn_tiles_dir):
        os.path.mkdir(burn_tiles_dir)
    cmd = ['aws', 's3', 'cp', burn_year_tiles, burn_tiles_dir, '--recursive', '--exclude', "*", '--include', include]  
    subprocess.check_call(cmd)
    
    
    
    # for each year tile, convert to array and stack them
    array_list = []
    ba_tifs = glob.glob(burn_tiles_dir + '/*')
    for ba_tif in ba_tifs:
        array = utilities.raster_to_array(tif)
        array_list.append(array)
        
    # stack arrays
    stacked_year_array = utilities.stack_arrays(array_list)
    
    # download hansen tile
    loss_tile = utilities.wgetloss(tile_id)
    
    # convert hansen tile to array
    loss_array = utilities.raster_to_array(loss_tile)
    
    lossarray_min1 = np.subtract(loss_array, 1)
    
    
    stack_con =(stacked_year_array >= lossarray_min1) & (stacked_year_array <= loss_array)
    stack_con2 = stack_con * stack
    lossyear_burn_array = stack_con2.max(0)
    
    # write burn pixels to raster
    outfolder = 'lossyear_burn'
    outname = '{}_burnyear.tif'.format(tile_id)
    outfile = os.path.join(outfolder, outname)
    
    utilities.array_to_raster_simple(lossyear_burn_array, outfile, loss_tile)
    