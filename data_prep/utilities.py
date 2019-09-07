import subprocess
import os

# name of mangrove vrt
mangrove_vrt = 'mangrove_biomass.vrt'

def build_vrt(out_vrt):
    print "Creating vrt of mangroves..."
    os.system('gdalbuildvrt {} *.tif'.format(out_vrt))