import constants_and_names as cn
from funcs import create_masks

# Set input folders = Mask, Inputs folders and select tcd_threshold/ gain/ save intermediate values
create_masks(cn.tcd_threshold, cn.gain, cn.save_intermediates)

# Other options:
# create_masks([0, 75], cn.gain, False)
# create_masks([30], cn.gain, True)