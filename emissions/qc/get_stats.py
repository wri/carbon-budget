import subprocess

# use this to query the rasters on s3 and report any errors
tile_list = ['30S_150E']

for tile in tile_list:

    print tile

    cmd = ['gdalinfo', '/vsis3/gfw-files/sam/carbon_budget/carbon_111717/bgc/{}_bgc.tif'.format(tile), '-mm']

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    with open('bgc_nodata.txt', 'a') as text:

        for line in iter(p.stdout.readline, b''):
            if "ERROR" in line:    
                text.write(tile + '\n')
