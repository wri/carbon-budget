### Global forest carbon flux model

#### Purpose and scope
This model maps gross annual greenhouse gas emissions from forests between 2001 and 2019, 
gross carbon removals by forests between 2001 and 2019, and the difference between them 
(net flux). Gross emissions includes CO2, NH4, and N2O and all carbon pools, and gross removals includes aboveground and belowground biomass carbon. 
It covers planted forests in most of the world, mangroves, and non-mangrove natural forests.
It essentially spatially applies IPCC national greenhouse gas inventory rules for forests.
It covers only forests converting to non-forests, non-forests converted to forests and forests remaining forests (no other land 
use transitions).

#### Inputs
Well over twenty inputs are needed to run this model. Most are spatial, but some are tabular.
All spatial data are converted to 10 x 10 degree rasters at 0.000 25 x 0.00025 degree resolution 
(approximately 30 x 30 m at the equator). The tabular data are mostly annual biomass removal (i.e. 
sequestration) factors (e.g., mangroves, planted forests, natural forests). 
Spatial data include annual tree cover loss, biomass densities in 2000, drivers of tree cover loss, 
ecozones, tree cover extent in 2000, elevation, etc. Different inputs are needed for different
steps in the model. This repository includes scripts for processing all of the needed inputs. Many inputs can be processed the same way (e.g., many rasters can be processed using the same gdal function) but some need special treatment.

#### Outputs
There are three key outputs produced: gross GHG emissions, gross removals, and net flux, all for 2001-2019. 
These are produced at two resolutions: 0.00025 x 0.00025 degrees 
(approximately 30 x 30 m at the equator) in 10 x 10 degree rasters (to make outputs a 
manageable size), and 0.04 x 0.04 degrees (approximately 5 x 5 km at the equator) as global rasters. The former output is used for 
zonal statistics analyses (i.e. emissions, removals, or net in polygons of interest)
and visualization on the Global Forest Watch web platform while the latter is used for static maps,
like in publications and presentations. At this point, the 30-m resolution output is in
megagrams CO2e/ha (i.e. densities), 
while the 5-km resolution output is in Mg CO2e/pixel (in order to show absolute values).
Although gross emissions are traditionally given positive (+) values and
gross removals are traditionally given negative (-) values, the 30-m gross removals rasters are positive, while the 5-km gross removals rasters are negative. 
Net flux at both scales can be positive or negative depending on the balance of emissions and removals in the area of interest.
Individual emissions can be assigned years based on Hansen loss during further analyses but removals and net flux are cumulative over the entire model run.

In addition to these three key outputs, there are many intermediate output rasters from the model,
some of which may be useful for some purposes. All of these are at 0.00025 x 0.00025 degree
resolution and reported as per hectare values (as opposed to per pixel values), if applicable. 
Intermediate outputs include the annual aboveground and belowground biomass removal rates
for all kinds of forests, the type of removal factor applied to each pixel, the carbon pool stocks in 2000, and carbon pools in the year of
tree cover loss. 

Almost all model output have metadata associated with them, viewable using the gdalinfo command line utility (https://gdal.org/programs/gdalinfo.html). 
Metadata includes units, date created, model version, geographic extent, and more. Unfortunately, the metadata is not viewable in ArcMap.

Model runs also automatically generate a txt log that is saved to s3. This log includes nearly everything that is output in the console.
This log is useful for documenting model runs and checking for mistakes/errors in retrospect.
For example, users can examine it to see if the correct input tiles were downloaded or if the intended tiles were used during the model run.  

#### Running the model
There are two ways to run the model: as a series of individual scripts and from a master script.
Which one to use depends on what you are trying to do. Generally, the individual scripts are
more appropriate for more development and testing, while the master script is better for running
the main part of the model from start to finish in one go. In either case, the code must be cloned from this 
repository.

The model runs inside a Docker container. Once you have Docker configured on your system and have cloned this repository, you can do the following.
This will enter the command line in the Docker container. 

For runs on my local computer, I use Docker-compose so that the Docker is mapped to my computer's drives. 
I do this for development and testing.
`docker-compose build`
`docker-compose run --rm -e AWS_SECRET_ACCESS_KEY=... -e AWS_ACCESS_KEY_ID=... carbon-budget`

For runs on an AWS r5d spot machine (for full model runs), I use docker build.
`docker build . -t gfw/carbon-budget`
`docker run --rm -it -e AWS_SECRET_ACCESS_KEY=... -e AWS_ACCESS_KEY_ID=... gfw/carbon-budget`

Before doing a model run, confirm that the dates of the relevant input and output s3 folders are correct in `constants_and_names.py`. 
Depending on what exactly the user is running, the user may have to change lots of dates in the s3 folders or change none.
Unfortunately, I can't really give better guidance than that; it really depends on what part of the model is being run and how.
(I want to make the situations under which users change folder dates more consistent eventually.)

##### Individual scripts
The flux model is comprised of many separate scripts, each of which can be run separately and
has its own inputs and output(s). Combined, these comprise the flux model. There are several data preparation
scripts, several for the removals (sequestration) model, a few to generate carbon pools, one for calculating
gross emissions, one for calculating net flux, and one for aggregating key results into coarser 
resolution rasters for mapping. The order in which these must be run is very specific; many scripts depend on 
the outputs of other scripts. Looking at the files that must be downloaded to the spot machine for the 
script to run will show what files must already be created and therefore what scripts must have already been
run. The date component of the output directory on s3 must be changed in constants_and_names.py 
for each output file unless a date argument is provided on the command line. 

Each script can be run either using multiple processors or one processor. The former is for full model runs,
while the latter is for model development. The user can switch between these two versions by commenting out
the appropriate code chunks. 

##### Master script 
A master script will run through all of the non-preparatory scripts in the model: some removal factor creation, gross removals, carbon
pool generation, gross emissions, net flux, and aggregation. It includes all the arguments needed to run
every script. The user can control what model components are run to some extent and set the date part of 
the output directories. The emissions C++ code has to be be compiled before running the master script (see below).

`python run_full_model.py -t std -s all -r true -d 20200822 -l all -ce loss -p biomass_soil -tcd 30 -ma true -us true -ln "This will run the entire standard model, including creating mangrove and US removal factor tiles."`

#### Running the emissions model
The gross emissions script is the only part that uses C++. Thus, it must be manually compiled before running.
There are a few different versions of the emissions script: one for the standard model and a few other for
sensitivitity analyses.
The command for compiling the C++ script is (subbing in the actual file name): 

`c++ /usr/local/app/emissions/cpp_util/calc_gross_emissions_[VERSION].cpp -o /usr/local/app/emissions/cpp_util/calc_gross_emissions_[VERSION].exe -lgdal`

| Argument | Required/Optional | Description | 
| -------- | ----------- | ------ |
| `model-type` | Required | Standard model (`std`) or a sensitivity analysis. Refer to constants_and_names.py for valid list of sensitivity analyses. |
| `stages` | Required | The model stage at which the model should start. `all` will run the following stages in this order: model_extent, forest_age_category_IPCC, annual_removals_IPCC, annual_removals_all_forest_types, gain_year_count, gross_removals_all_forest_types, carbon_pools, gross_emissions, net_flux, aggregate |
| `run-through` | Required | Options: true or false. true: run stage in `stages` argument and following stages. false: run only stage in `stages` argument. |
| `run-date` | Required | Date of run. Must be format YYYYMMDD. |
| `tile-id-list` | Required | List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all |
| `carbon-pool-extent` | Optional | Extent over which carbon pools should be calculated: loss or 2000 or loss,2000 or 2000,loss |
| `pools-to-use` | Optional | Options are soil_only or biomass_soil. Former only considers emissions from soil. Latter considers emissions from biomass and soil. |
| `tcd-threshold` | Optional | Tree cover density threshold above which pixels will be included in the aggregation. |
| `std-net-flux-aggreg` | Optional | The s3 standard model net flux aggregated tif, for comparison with the sensitivity analysis map. |
| `mangroves` | Optional | Create mangrove removal factor tiles as the first stage. true or false |
| `us-rates` | Optional | Create US-specific removal factor tiles as the first stage (or second stage, if mangroves are enabled). true or false |
| `per-pixel-results` | Optional | Create tiles of gross emissions, gross removals, and net flux with CO2e/pixel in addition to CO2e/ha |
| `log-note` | Optional | Adds text to the beginning of the log |

#### Sensitivity analysis
Several variations of the model are included; these are the sensitivity variants, as they use different inputs or parameters. 
They can be run by changing the `model-type` argument from `std` to an option found in `constants_and_names.py`. 
Each sensitivity analysis variant starts at a different stage in the model and runs to the final stage.
Some use all tiles and some use a smaller extent.

| Sensitivity analysis | Description | Extent | Starting stage | 
| -------- | ----------- | ------ | ------ |
| `std` | Standard model | Global | `mp_model_extent.py` |
| `maxgain` | Maximum number of years of gain (removals) for gain-only and loss-and-gain pixels | Global | `gain_year_count_all_forest_types.py` |
| `no_shifting_ag` | Required | Options: true or false. true: run stage in `stages` argument and following stages. false: run only stage in `stages` argument. |
| `convert_to_grassland` | Required | Date of run. Must be format YYYYMMDD. |
| `biomass_swap` | Required | List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all |
| `US_removals` | Optional | Extent over which carbon pools should be calculated: loss or 2000 or loss,2000 or 2000,loss |
| `no_primary_gain` | Optional | Options are soil_only or biomass_soil. Former only considers emissions from soil. Latter considers emissions from biomass and soil. |
| `legal_Amazon_loss` | Optional | Tree cover density threshold above which pixels will be included in the aggregation. |
| `Mekong_loss` | Optional | The s3 standard model net flux aggregated tif, for comparison with the sensitivity analysis map. |


#### Dependencies
Theoretically, this model should run anywhere that the correct Docker container can be started and there is access to the AWS s3 bucket. 
The Docker container should be self-sufficient in that it is configured to include the right Python packages, C++ compiler, GDAL, etc.
On an AWS EC2 instance, I have only run it on r5d instance types but it might be able to run on others.
At the least, it needs a certain type of memory configuration on the EC2 instance (at least one large SSD volume, I believe). 
Otherwise, I do not know the limitations and constraints on running this model. 

#### Contact information
David Gibbs

Global Forest Watch, World Resources Institute, Washington, D.C.

david.gibbs@wri.org