### Global forest carbon flux model

#### Purpose and scope
This model maps gross greenhouse gas emissions from forests between 2001 and 2015, 
gross carbon removals by forests between 2001, and the difference between them 
(net flux). The model includes CO2, NH4, and N20 and all carbon pools for gross emissions.
It covers planted forests in most of the world, mangroves, and non-mangrove natural forests.
It essentially spatially applies IPCC national greenhouse gas inventory rules for forests.
It covers only forests converting to non-forests and forests remaining forests (no other land 
use transitions).

#### Inputs
Well over twenty inputs are needed to run this model. Most are spatial, but some are tabular.
All spatial data are converted to 10 x 10 degree rasters at 0.000 25 x 0.00025 degree resolution 
(approximately 30 x 30 m at the equator). The tabular data are mostly annual biomass removal (i.e. 
sequestration) rates (mangroves, planted forests, natural forests). 
Spatial data include annual tree cover loss, biomass stocks in 2000, drivers of tree cover loss, 
ecozones, tree cover extent in 2000, elevation, etc. Different inputs are needed for different
steps in the model.

#### Outputs
There are three key outputs produced: gross GHG emissions, gross removals, and net flux. 
These are produced at two resolutions: 0.00025 x 0.00025 degrees 
(approximately 30 x 30 m at the equator) in 10 x 10 degree rasters (to make outputs a 
manageable size), and 0.04 x 0.04 degrees (approximately 5 x 5 km at the equator) as global rasters. The former output is used for 
zonal statistics analyses (e.g., gross emissions in admin units, net flux in protected areas)
and visualization on the Global Forest Watch web platform while the latter is used for static maps,
like in publications and presentations. At this point, the 30-m resolution output is in
Mg CO2e/ha (for gross emissions and net flux) or Mg CO2/ha (for gross removals) (i.e. densities), 
while the 5-km resolution output is in Mg CO2e/pixel or Mg CO2/pixel 
(in order to show absolute values).
Gross emissions are traditionally given positive (+) values, 
gross removals are traditionally given negative (-) values, and net flux can be positive
or negative depending on the balance of emissions and removals in the area of interest.

In addition to these three key outputs, there are many intermediate output rasters from the model,
some of which may be useful for some purposes. All of these are at 0.00025 x 0.00025 degree
resolution and reported as per hectare values (as opposed to per pixel values), if applicable. 
Intermediate outputs include the annual aboveground and belowground biomass removal rates
for all three kinds of forests, the carbon pool stocks in 2000, and carbon pools in the year of
tree cover loss. These are not accessible on the Global Forest Watch web platform but the
10 x 10 degrees rasters are available for download and some of these are queryable through 
zonal statistics tables in the API.

#### Running the model
There are two ways to run the model: as a series of individual scripts and from a master script.
Which one to use depends on what you are trying to do. Generally, the individual scripts are
more appropriate for more development and testing, while the master script is better for running
the main part of the model from start to finish in one go. 

##### Individual scripts
The flux model is comprised of many separate scripts, each of which can be run separately and
has its own inputs and output(s). Combined, these comprise the flux model. There are several data preparation
scripts, several for the removals (sequestration) model, a few to generate carbon pools, one for calculating
gross emissions, one for calculating net flux, and one for aggregating key results into coarser 
resolution rasters for mapping. The order in which these must be run is complex; many scripts depend on 
the outputs of other scripts. Looking at the files that must be downloaded to the spot machine for the 
script to run will show what files must already be created and therefore what scripts must have already been
run. The date component of the output directory on s3 must be changed in constants_and_names.py 
for each output file. 

Each script can be run either using multiple processors or one processor. The former is for full model runs,
while the latter is for model development. The user can switch between these two versions by commenting out
the appropriate code chunks. 

##### Master script 
A master script will run through all of the non-preparatory scripts in the model: gross removals, carbon
pool generation, gross emissions, net flux, and aggregation. It includes all the arguments needed to run
every script. The user can control what model components are run to some extent and set the date part of 
the output directories. 

`python run_full_model.py -t std -s all -r true -d 20200309 -l all -ce loss -p biomass_soil -tcd 30 -ma true -pl true`


#### Running the emissions model
The gross emissions script is the only part that uses C++. Thus, it must be manually compiled before running.
There are a few different versions of the emissions script: one for the standard model and a few other for
sensitivitity analyses.
The command for compiling the C++ script is (subbing in the actual file name): 

`c++ ../carbon-budget/emissions/cpp_util/calc_gross_emissions_[VERSION].cpp -o ../carbon-budget/emissions/cpp_util/calc_gross_emissions_[VERSION].exe -lgdal`

| Argument | Required/Optional | Description | 
| -------- | ----------- | ------ |
| `model-type` | Required | Standard model (std) or a sensitivity analysis. Refer to constants_and_names.py for latest list of analyses. |
| `stages` | Required | Stages of creating Brazil legal Amazon-specific gross cumulative removals |
| `run-through` | Required | Options: true or false. true: run named stage and following stages. false: run only named stage. |
| `run-date` | Required | Date of run. Must be format YYYYMMDD. |
| `tile-id-list` | Required | List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all |
| `carbon-pool-extent` | Optional | Extent over which carbon pools should be calculated: loss or 2000 |
| `pools-to-use` | Optional | Options are soil_only or biomass_soil. Former only considers emissions from soil. Latter considers emissions from biomass and soil. |
| `tcd-threshold` | Optional | Tree cover density threshold above which pixels will be included in the aggregation. |
| `std-net-flux-aggreg` | Optional | The s3 standard model net flux aggregated tif, for comparison with the sensitivity analysis map. |
| `mangroves` | Optional | Include mangrove annual gain rate, gain year count, and cumulative gain in stages to run. true or false |
| `plantations` | Optional | Include planted forest annual gain rate, gain year count, and cumulative gain in stages to run. true or false |

#### Sensitivity analysis



#### Dependencies
This is designed to run on a particular AMI of EC2 machines on Amazon AWS that runs Linux and has various
Python packages and PostGIS installed. It will not be easy to run in other environments. There is currently
no master list of system requirements or package dependencies for this model. 

#### Contact information

David Gibbs

Global Forest Watch, World Resources Institute, Washington, D.C.

david.gibbs@wri.org