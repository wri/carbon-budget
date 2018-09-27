import subprocess


def coords(tile_id):
    NS = tile_id.split("_")[0][-1:]
    EW = tile_id.split("_")[1][-1:]

    if NS == 'S':
        ymax =-1*int(tile_id.split("_")[0][:2])
    else:
        ymax = int(str(tile_id.split("_")[0][:2]))
    
    if EW == 'W':
        xmin = -1*int(str(tile_id.split("_")[1][:3]))
    else:
        xmin = int(str(tile_id.split("_")[1][:3]))

    ymin = str(int(ymax) - 10)
    xmax = str(int(xmin) + 10)
    
    return ymax, xmin, ymin, xmax


def clip_for_model(tile_id):
    # given a delivery from TNC of a forest model, send it through this to make Hansen size tiles
    ymax, xmin, ymin, xmax = coords(tile_id)
    for_model = r'Goode_FinalClassification_15_50uncertain_expanded_wgs84.tif'
    outmodel = '{}_res_forest_model.tif'.format(tile_id)

    cmd = ['gdalwarp', '-te', str(xmin), str(ymin), str(xmax), str(ymax), '-tr', '.00025', '.00025', '-tap', '-co', 'COMPRESS=LZW', for_model, outmodel]
    subprocess.check_call(cmd)


clip_for_model('40N_010W')