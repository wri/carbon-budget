## Global forest carbon flux model

### Purpose and scope
This model maps gross annual greenhouse gas emissions from forests, 
gross carbon removals (sequestration) by forests, and the difference between them 
(net flux), all between 2001 and 2021. 
Gross emissions includes CO2, NH4, and N20 and all carbon pools (abovegroung biomass, belowground biomass, 
dead wood, litter, and soil), and gross removals includes removals into aboveground and belowground biomass carbon. 
Although the model is run for all tree canopy densities (per Hansen et al. 2013), it is most relevant to
pixels with canopy density >30% in 2000 or pixels which subsequently had tree cover gain (per Hansen et al. 2013).
It covers planted forests in most of the world, mangroves, and non-mangrove natural forests, and excludes palm oil plantations that existed more than 20 years ago.
It essentially spatially applies IPCC national greenhouse gas inventory rules (2016 guidelines) for forests.
It covers only forests converting to non-forests, non-forests converted to forests and forests remaining forests (no other land 
use transitions). The model is described and published in Harris et al. (2021) Nature Climate Change
"Global maps of twenty-first century forest carbon fluxes" (https://www.nature.com/articles/s41558-020-00976-6).
Although the published model covered 2001-2019, the same methods were used to update the model to include 2021.  

### Inputs
Well over twenty inputs are needed to run this model. Most are spatial, but some are tabular.
All spatial data are converted to 10x10 degree raster tiles at 0.00025x0.00025 degree resolution 
(approximately 30x30 m at the equator) before inclusion in the model. The tabular data are generally annual biomass removal (i.e. 
sequestration) factors (e.g., mangroves, planted forests, natural forests), which are then applied to spatial data. 
Spatial data include annual tree cover loss, biomass densities in 2000, drivers of tree cover loss, 
ecozones, tree cover extent in 2000, elevation, etc. Different inputs are needed for different
steps in the model. This repository includes scripts for processing all of the needed inputs. 
Many inputs can be processed the same way (e.g., many rasters can be processed using the same gdal function) but some need special treatment.
The input processing scripts are scattered among almost all the folders, unfortunately, a historical legacy of how I built this out
which I haven't fixed. The data prep scripts are generally in the folder for which their outputs are most relevant.

Inputs can either be downloaded from AWS s3 storage or used if found locally in the folder `/usr/local/tiles/` in the Docker container
(see below for more on the Docker container).
The model looks for files locally before downloading them. 
The model can still be run without AWS credentials; inputs will be downloaded from s3 but outputs will not be uploaded to s3.
In that case, outputs will only be stored locally.

### Outputs
There are three key outputs produced: gross GHG emissions, gross removals, and net flux, all totaled for 2001-2021. 
These are produced at two resolutions: 0.00025x0.00025 degrees 
(approximately 30x30 m at the equator) in 10x10 degree rasters (to make outputs a 
manageable size), and 0.04x0.04 degrees (approximately 4x4km at the equator) as global rasters for static maps.

Model runs also automatically generate a txt log. This log includes nearly everything that is output in the console.
This log is useful for documenting model runs and checking for mistakes/errors in retrospect, although it does not capture errors that terminate the model.
For example, users can examine it to see if the correct input tiles were downloaded or if the intended tiles were used during the model run.  

Output rasters and model logs are uploaded to s3 unless the `--no-upload` flag (`-nu`) is activated as a command line argument
or no AWS s3 credentials are supplied to the Docker container.
When either of these happens, neither raster outputs nor logs are uploaded to s3. This is good for local test runs or versions
of the model that are independent of s3 (that is, inputs are stored locally and no on s3, and the user does not have 
a connection to s3 storage or s3 credentials).

#### 30-m output rasters

The 30-m outputs are used for zonal statistics analyses (i.e. emissions, removals, or net in polygons of interest)
and mapping on the Global Forest Watch web platform or at small scales (where 30-m pixels can be distinguished). 
Individual emissions can be assigned years based on Hansen loss during further analyses 
but removals and net flux are cumulative over the entire model run and cannot be assigned specific years. 
This 30-m output is in megagrams (Mg) CO2e/ha 2001-2021 (i.e. densities) and includes all tree cover densities ("full extent"):
`(((TCD2000>0 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0) NOT IN pre-2000 plantations)`.
However, the model is designed to be used specifically for forests, so the model creates three derivative 30-m
outputs for each key output (gross emissions, gross removals, net flux) as well 
(only for the standard model, not for sensitivity analyses):

1) Per pixel values for the full model extent (all tree cover densities): 
   `(((TCD2000>0 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0) NOT IN pre-2000 plantations)`
2) Per hectare values for forest pixels only (colloquially, TCD>30 or Hansen gain pixels): 
   `(((TCD2000>30 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0) NOT IN pre-2000 plantations)`
3) Per pixel values for forest pixels only (colloquially, TCD>30 or Hansen gain pixels):  
   `(((TCD2000>30 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0) NOT IN pre-2000 plantations)`

The per hectare outputs are used for making pixel-level maps (essentially showing emission and removal factors), 
while the per pixel outputs are used for getting total values within areas because the values
of those pixels can be summed within areas of interest. The per pixel maps are `per hectare * pixel area/10000`.
(The pixels of the per hectare outputs should not be summed but they can be averaged in areas of interest.)
Statistics from this model should always be based on the "forest extent" rasters, not the "full extent" rasters.
The full model extent outputs should generally not be used but are created by the model in case they are needed.

In addition to these three key outputs, there are many intermediate output rasters from the model,
some of which may be useful for QC, analyses by area of interest, or other purposes. 
All of these are at 0.00025x0.00025 degree resolution and reported as per hectare values (as opposed to per pixel values), if applicable. 
Intermediate outputs include the annual aboveground and belowground biomass removal rates
for all kinds of forests, the type of removal factor applied to each pixel, the carbon pool densities in 2000, 
carbon pool densities in the year of tree cover loss, and the number of years in which removals occurred. 

Almost all model output have metadata associated with them, viewable using the `gdalinfo` command line utility (https://gdal.org/programs/gdalinfo.html). 
Metadata includes units, date created, model version, geographic extent, and more. Unfortunately, the metadata are not viewable 
when looking at file properties in ArcMap
or in the versions of these files downloadable from the Global Forest Watch Open Data Portal (https://data.globalforestwatch.org/).

#### 4-km output rasters

The 4-km outputs are used for static large-scale maps, like in publications and presentations. 
The units are Mt CO2e/pixel/year (in order to show absolute values). They are created using the "forest extent" 
per pixel 30-m rasters, not the "full extent" 30-m rasters. They should not be used for analysis. 

#### A note on signs

Although gross emissions are traditionally given positive (+) values and
gross removals are traditionally given negative (-) values, the 30-m gross removals rasters are positive, while the 4-km gross removals rasters are negative. 
Net flux at both scales can be positive or negative depending on the balance of emissions and removals in the area of interest
(negative for net sink, positive for net source).


### Running the model
The model runs from the command line inside a Linux Docker container. 
Once you have Docker configured on your system, have cloned this repository, 
and have configured access to AWS (if desired, or have the input files stored in the correct local folder), 
you will be able to run the model. 

There are two ways to run the model: as a series of individual scripts, or from a master script, which runs the individual scripts sequentially.
Which one to use depends on what you are trying to do. Generally, the individual scripts (which correspond to specific model stages) are
more appropriate for development and testing, while the master script is better for running
the main part of the model from start to finish in one go. In either case, the code must be cloned from this repository
(on the command line in the folder you want to clone to, `git clone https://github.com/wri/carbon-budget`).
Run globally, both options iterate through a list of ~275 10 x 10 degree tiles. (Different model stages have different numbers of tiles.)
Run all tiles in the model extent fully through one model stage before starting on the next stage. 
(The master script does this automatically.) If a user wants to run the model on just one or a few tiles, 
that can be done through a command line argument (`--tile-id-list` or `-l`). 
If individual tiles are listed, only those will be run. This is a natural system for testing or for
running the model for individual countries. You can see the tile boundaries in pixel_area_tile_footprints.zip.
For example, to run the model for Madagascar, only tiles 10S_040E, 10S_050E, and 20S_040E need to be run and the
command line argument would be `-l 10S_040E,10S_050E,20S_040E`. 

You can do the following on the command line in the same folder as the repository on your system.
This will enter the command line in the Docker container

For runs on a local computer, use `docker-compose` so that the Docker is mapped to your computer's drives.
In my setup, `C:/GIS/Carbon_model/test_tiles/docker_output/` on my computer is mapped to `/usr/local/tiles` in
the Docker container in `docker-compose.yaml`. If running on another computer, you will need to change the local 
folder being mapped in `docker-compose.yaml` to match your computer's directory structure. 
I do this for development and testing. 
If you want the model to be able to download from and upload to s3, you will also need to provide 
your own AWS secret key and access key as environment variables (`-e`) in the `docker-compose run` command:

`docker-compose build`

`docker-compose run --rm -e AWS_SECRET_ACCESS_KEY=... -e AWS_ACCESS_KEY_ID=... carbon-budget`

If you don't have AWS credentials, you can still run the model in the docker container but uploads will 
not occur. In this situation, you need all the basic input files for all tiles in the docker folder `/usr/local/tiles/`
on your computer:

`docker-compose build`

`docker-compose run --rm carbon-budget`

For runs on an AWS r5d spot machine (for full model runs), use `docker build`. 
You need to supply AWS credentials for the model to work because otherwise you won't be able to get 
output tiles off of the spot machine.

`docker build . -t gfw/carbon-budget`

`docker run --rm -it -e AWS_SECRET_ACCESS_KEY=... -e AWS_ACCESS_KEY_ID=... gfw/carbon-budget`

Before doing a model run, confirm that the dates of the relevant input and output s3 folders are correct in `constants_and_names.py`. 
Depending on what exactly the user is running, the user may have to change lots of dates in the s3 folders or change none.
Unfortunately, I can't really give better guidance than that; it really depends on what part of the model is being run and how.
(I want to make the situations under which users change folder dates more consistent eventually.)

The model can be run either using multiple processors or one processor. The former is for large scale model runs,
while the latter is for model development or running on small-ish countries that use only a few tiles. 
The user can switch between these two versions by commenting out
the appropriate code chunks in each script. The single-processor option is commented out by default. 
One important thing to note is that if a user tries to use too many processors, the system will run out of memory and
can crash (particularly on AWS EC2 instances). Thus, it is important not to use too many processors at once.
Generally, the limitation in running the model is the amount of memory available on the system rather than the number of processors.
Each script has been somewhat calibrated to use a safe number of processors for an r5d.24xlarge EC2 instance,
and often the number of processors being used is 1/2 or 1/3 of the actual number available.
If the tiles were smaller (e.g., 1x1 degree), more processors could be used but then there'd also be more tiles to process, so I'm not sure that would be any faster.
Users can track memory usage in realtime using the `htop` command line utility in the Docker container. 


#### Individual scripts
The flux model is comprised of many separate scripts (or stages), each of which can be run separately and
has its own inputs and output(s). Combined, these comprise the flux model. There are several data preparation
scripts, several for the removals (sequestration/gain) model, a few to generate carbon pools, one for calculating
gross emissions, one for calculating net flux, one for aggregating key results into coarser 
resolution rasters for mapping, and one for creating per-pixel and forest-extent outputs (supplementary outputs). 
Each script really has two parts: its `mp_` (multiprocessing) part and the part that actually does the calculations
on each 10x10 degree tile.
The `mp_` scripts (e.g., `mp_create_model_extent.py`) are the ones that are run. They download input files,
do any needed preprocessing, change output folder names as needed, list the tiles that are going to be run, etc.,
then initiate the actual work done on each tile in the script without the `mp_` prefix.
The order in which the individual stages must be run is very specific; many scripts depend on
the outputs of other scripts. Looking at the files that must be downloaded for the 
script to run will show what files must already be created and therefore what scripts must have already been
run. Alternatively, you can look at the top of `run_full_model.py` to see the order in which model stages are run. 
The date component of the output directory on s3 generally must be changed in `constants_and_names.py`
for each output file. 

##### Running the emissions model
The gross emissions script is the only part of the model that uses C++. Thus, it must be manually compiled before running.
There are a few different versions of the emissions script: one for the standard model and a few other for
sensitivity analyses.
The command for compiling the C++ script is (subbing in the actual file name): 

`c++ /usr/local/app/emissions/cpp_util/calc_gross_emissions_[VERSION].cpp -o /usr/local/app/emissions/cpp_util/calc_gross_emissions_[VERSION].exe -lgdal`

For the standard model and the sensitivity analyses that don't specifically affect emissions, it is:

`c++ /usr/local/app/emissions/cpp_util/calc_gross_emissions_generic.cpp -o /usr/local/app/emissions/cpp_util/calc_gross_emissions_generic.exe -lgdal`

#### Master script 
The master script runs through all of the non-preparatory scripts in the model: some removal factor creation, gross removals, carbon
pool generation, gross emissions, net flux, aggregation, and supplementary output creation. 
It includes all the arguments needed to run
every script. Thus, the table below also explains the potential arguments for the individual model stages. 
The user can control what model components are run to some extent and set the date part of 
the output directories. The emissions C++ code has to be be compiled before running the master script (see below).
Preparatory scripts like creating soil carbon tiles or mangrove tiles are not included in the master script because
they are run very infrequently. 

| Argument | Short argument | Required/Optional | Relevant stage | Description | 
| -------- | ----- | ----------- | ------- | ------ |
| `model-type` | `-t` | Required | All | Standard model (`std`) or a sensitivity analysis. Refer to `constants_and_names.py` for valid list of sensitivity analyses. |
| `stages` | `-s` | Required | All | The model stage at which the model should start. `all` will run the following stages in this order: model_extent, forest_age_category_IPCC, annual_removals_IPCC, annual_removals_all_forest_types, gain_year_count, gross_removals_all_forest_types, carbon_pools, gross_emissions, net_flux, aggregate, create_supplementary_outputs |
| `run-through` | `-r` | Optional | All | If activated, run stage provided in `stages` argument and all following stages. Otherwise, run only stage in `stages` argument. Activated with flag. |
| `run-date` | `-d` | Required | All | Date of run. Must be format YYYYMMDD. This sets the output folder in s3. |
| `tile-id-list` | `-l` | Required | All | List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all |
| `no-upload` | `-nu` | Optional | All | No files are uploaded to s3 during or after model run (including logs and model outputs). Use for testing to save time. When AWS credentials are not available, upload is automatically disabled and this flag does not have to be manually activated. |
| `single-processor` | `-sp` | Optional | All | Tile processing will be done without `multiprocessing` module whenever possible, i.e. no parallel processing. Use for testing. |
| `log-note` | `-ln`| Optional | All | Adds text to the beginning of the log |
| `carbon-pool-extent` | `-ce` | Optional | Carbon pool creation | Extent over which carbon pools should be calculated: loss or 2000 or loss,2000 or 2000,loss |
| `pools-to-use` | `-p` | Optional | Emissions| Options are soil_only or biomass_soil. Former only considers emissions from soil. Latter considers emissions from biomass and soil. |
| `tcd-threshold` | `-tcd`| Optional | Aggregation | Tree cover density threshold above which pixels will be included in the aggregation. Defaults to 30. |
| `std-net-flux-aggreg` | `-std` | Optional | Aggregation | The s3 standard model net flux aggregated tif, for comparison with the sensitivity analysis map. |
| `save-intermdiates` | `-si`| Optional | `run_full_model.py` | Intermediate outputs are not deleted within `run_full_model.py`. Use for local model runs. If uploading to s3 is not enabled, intermediate files are automatically saved. |
| `mangroves` | `-ma` | Optional | `run_full_model.py` | Create mangrove removal factor tiles as the first stage. Activate with flag. |
| `us-rates` | `-us` | Optional | `run_full_model.py` | Create US-specific removal factor tiles as the first stage (or second stage, if mangroves are enabled). Activate with flag. |

These are some sample commands for running the flux model in various configurations. You wouldn't necessarily want to use all of these;
they simply illustrate different configurations for the command line arguments.

Run 00N_000E in standard model; save intermediate outputs; upload outputs to s3; run all model stages;
starting from the beginning; get carbon pools at time of loss; emissions from biomass and soil:

`python run_full_model.py -si -t std -s all -r -d 20229999 -l 00N_000E -ce loss -p biomass_soil -tcd 30 -ln "00N_000E test"`

Run 00N_110E in standard model; save intermediate outputs; don't upload outputs to s3;
start at forest_age_category_IPCC step; run all stages after that; get carbon pools at time of loss; emissions from biomass and soil:

`python run_full_model.py -si -nu -t std -s forest_age_category_IPCC -r -d 20229999 -l 00N_000E -ce loss -p biomass_soil -tcd 30 -ln "00N_000E test"`

Run 00N_000E and 00N_110E in standard model; don't save intermediate outputs; do upload outputs to s3;
run model_extent step; don't run sunsequent steps (no `-r` flag); run mangrove step beforehand:

`python run_full_model.py -t std -s model_extent -d 20229999 -l 00N_000E,00N_110E -ma -ln "Two tile test"`

Run 00N_000E, 00N_110E, and 30N_090W in standard model; save intermediate outputs; do upload outputs to s3;
start at gross_emissions step; run all stages after that; emissions from soil only:

`python run_full_model.py -si -t std -s gross_emissions -r -d 20229999 -l 00N_000E,00N_110E,30N_090W -p soil_only -tcd 30 -ln "Three tile test"`

FULL STANDARD MODEL RUN: Run all tiles in standard model; save intermediate outputs; do upload outputs to s3;
run all model stages; starting from the beginning; get carbon pools at time of loss; emissions from biomass and soil:

`python run_full_model.py -si -t std -s all -r -l all -ce loss -p biomass_soil -tcd 30 -ln "Run all tiles"`

Run three tiles in biomass_swap sensitivity analysis; don't upload intermediates (forces saving of intermediate outputs);
run model_extent stage; don't continue after that stage (no run-through); get carbon pools at time of loss; emissions from biomass and soil;
compare aggregated outputs to specified file (although not used in this specific launch because only the first step runs):

`python run_full_model.py -nu -t biomass_swap -s model_extent -r false -d 20229999 -l 00N_000E,00N_110E,40N_90W -ce loss -p biomass_soil -tcd 30 -sagg s3://gfw2-data/climate/carbon_model/0_04deg_output_aggregation/biomass_soil/standard/20200914/net_flux_Mt_CO2e_biomass_soil_per_year_tcd30_0_4deg_modelv1_2_0_std_20200914.tif -ln "Multi-tile test"`


### Sensitivity analysis
Several variations of the model are included; these are the sensitivity variants, as they use different inputs or parameters. 
They can be run by changing the `--model-type` (`-t`) argument from `std` to an option found in `constants_and_names.py`. 
Each sensitivity analysis variant starts at a different stage in the model and runs to the final stage,
except that sensitivity analyses do not include the creation of the supplementary outputs (per pixel tiles, forest extent tiles).
Some use all tiles and some use a smaller extent.

| Sensitivity analysis | Description | Extent | Starting stage | 
| -------- | ----------- | ------ | ------ |
| `std` | Standard model | Global | `mp_model_extent.py` |
| `maxgain` | Maximum number of years of gain (removals) for gain-only and loss-and-gain pixels | Global | `gain_year_count_all_forest_types.py` |
| `no_shifting_ag` | Shifting agriculture driver is replaced with commodity-driven deforestation driver | Global | `mp_calculate_gross_emissions.py` |
| `convert_to_grassland` | Forest is assumed to be converted to grassland instead of cropland in the emissions model| Global | `mp_calculate_gross_emissions.py` |
| `biomass_swap` | Uses Saatchi 1-km AGB map instead of Baccini 30-m map for starting carbon densities | Extent of Saatchi map, which is generally the tropics| `mp_model_extent.py` |
| `US_removals` | Uses IPCC default removal factors for the US instead of US-specific removal factors from USFS FIA | Continental US | `mp_annual_gain_rate_AGC_BGC_all_forest_types.py` |
| `no_primary_gain` | Primary forests and IFLs are assumed to not have any removals| Global | `mp_forest_age_category_IPCC.py` |
| `legal_Amazon_loss` | Uses Brazil's PRODES annual deforestation system instead of Hansen loss | Legal Amazon| `mp_model_extent.py` |
| `Mekong_loss` | Uses Hansen loss v2.0 (multiple loss in same pixel). NOTE: Not used for flux model v1.2.0, so this is not currently supported. | Mekong region | N/A |


### Updating the model with new tree cover loss
For the current general configuration of the model, these are the changes that need to be made to update the
model with a new year of tree cover loss data. In the order in which the changes would be needed for rerunning the model:

1) Update the model version variable `version` in `constants_and_names.py`.

2) Change the tree cover loss tile source to the new tree cover loss tiles in `constants_and_names.py`.
Change the tree cover loss tile pattern in `constants_and_names.py`.

3) Change the number of loss years variable `loss_years` in `constants_and_names.py`.

4) In `constants.h` (emissions/cpp_util/), change the number of model years (`int model_years`) and the loss tile pattern (`char lossyear[]`).

5) In `equations.cpp` (emissions/cpp_util/), change the number of model years (`int model_years`). 

6) Make sure that changes in forest age category produced by `mp_forest_age_category_IPCC.py` 
   and the number of gain years produced by `mp_gain_year_count_all_forest_types.py` still make sense.

7) Obtain and pre-process the updated drivers of tree cover loss model in `mp_prep_other_inputs.py` 
   (comment out everything except the drivers lines). Note that the drivers map probably needs to be reprojected to WGS84 
   and resampled (0.005x0.005 deg) in ArcMap or similar before processing into 0.00025x0.00025 deg 10x10 tiles using this script.

8) Create a new year of burned area data using `mp_burn_year.py` (multiple changes to script needed, and potentially 
   some reworking if the burned area ftp site has changed its structure or download protocol). 
   Further instructions are at the top of `burn_date/mp_burn_year.py`.

Strictly speaking, if only the drivers, burn year, and tree cover loss are being updated, the model only needs to be 
run from forest_age_category_IPCC onwards (loss affects IPCC age category but model extent isn't affected by
any of these inputs).
However, for completeness, I suggest running all stages of the model from model_extent onwards for an update so that
model outputs from all stages have the same version in their metadata and the same dates of output as the model stages
that are actually being changed. A full model run (all tiles, all stages) takes about 18 hours on an r5d.24xlarge 
EC2 instance with 3.7 TB of storage and 96 processors.


### Other modifications to the model
It is recommended that any changes to the model be tested in a local Docker instance before running on an EC2 instance.
I like to output files to test folders on s3 with dates 20229999 because that is clearly not a real run date. 
A standard development route is: 

1) Make changes to a single model script and run using the single processor option on a single tile (easiest for debugging) in local Docker.

2) Run single script on a few representative tiles using a single processor in local Docker.

3) Run single script on a few representative tiles using multiple processor option in local Docker.

4) Run the master script on a few representative tiles using multiple processor option in local Docker to 
   confirm that changes work when using master script.

5) Run single script on a few representative tiles using multiple processors on EC2 instance (need to commit and push changes to GitHub first).

6) Run master script on all tiles using multiple processors on EC2 instance. 
   If the changes likely affected memory usage, make sure to watch memory with `htop` to make sure that too much memory isn't required. 
   If too much memory is needed, reduce the number of processors being called in the script. 

Depending on the complexity of the changes being made, some of these steps can be ommitted. Or if only a few tiles are 
being modeled (for a small country), only steps 1-4 need to be done.  

### Dependencies
Theoretically, this model should run anywhere that the correct Docker container can be started 
and there is access to the AWS s3 bucket or all inputs are in the correct folder in the Docker container. 
The Docker container should be self-sufficient in that it is configured to include the right Python packages, C++ compiler, GDAL, etc.
It is described in `Dockerfile`, with Python requirements (installed during Docker creation) in `requirements.txt`.
On an AWS EC2 instance, I have only run it on r5d instance types but it might be able to run on others.
At the least, it needs a certain type of memory configuration on the EC2 instance (at least one large SSD volume, I believe). 
Otherwise, I do not know the limitations and constraints on running this model in an EC2 instance. 

### Contact information
David Gibbs: david.gibbs@wri.org

Nancy Harris: nancy.harris@wri.org

Global Forest Watch, World Resources Institute, Washington, D.C.
