## Global forest carbon flux framework

### Purpose and scope
This framework maps gross greenhouse gas emissions from forests, 
gross carbon removals (sequestration) by forests, and the difference between them (net flux), all between 2001 and 2023. 
Gross emissions includes CO2, NH4, and N20 and all carbon pools (aboveground biomass, belowground biomass, 
dead wood, litter, and soil), and gross removals includes removals into aboveground and belowground biomass carbon. 
Although the framework is run for all tree canopy densities in 2000 (per Hansen et al. 2013), it is most relevant to
pixels with canopy density >30% in 2000 or pixels which subsequently had tree cover gain (per Potapov et al. 2022).
In addition to natural terrestrial forests, it also covers planted forests in most of the world, mangroves, and non-mangrove natural forests.
The framework essentially spatially applies IPCC national greenhouse gas inventory rules (2016 guidelines) for forests.
It covers only forests converted to non-forests, non-forests converted to forests and forests remaining forests (no other land 
use transitions). The framework is described and published in [Harris et al. (2021) Nature Climate Change
"Global maps of twenty-first century forest carbon fluxes"](https://www.nature.com/articles/s41558-020-00976-6).
Although the original manuscript covered 2001-2019, the same methods were used to update the framework to include 2023, 
with a few changes to some input layers and constants. You can read about the changes since publication 
[here](https://www.globalforestwatch.org/blog/data-and-research/whats-new-carbon-flux-monitoring).

### Inputs
Well over twenty inputs are needed for this framework. Most are spatial, but some are tabular.
All spatial data are converted to 10x10 degree raster tiles at 0.00025x0.00025 degree resolution 
(approximately 30x30 m at the equator) before ingestion. 
Spatial data include annual tree cover loss, biomass densities in 2000, drivers of tree cover loss, 
ecozones, tree cover extent in 2000, elevation, etc. 
Many inputs can be processed the same way (e.g., many rasters can be processed using the same `gdal` function) but some need special treatment.
The input processing scripts are in the `data_prep` folder and are mostly run in `mp_prep_other_inputs_annual.py` or
`mp_prep_other_inputs_one_off.py`. 
The tabular data are generally annual biomass removal (i.e. 
sequestration) factors (e.g., mangroves, planted forests, natural forests), which are then applied to spatial data. 
Different inputs are needed for different steps in the framework. 

Inputs can either be downloaded from AWS s3 storage or used if found locally in the folder `/usr/local/tiles/` in the Docker container
in which the framework runs (see below for more on the Docker container).
The framework looks for files locally before downloading them in order to reduce run time. 
The framework can still be run without AWS credentials; inputs will be downloaded from s3 but outputs will not be uploaded to s3.
In that case, outputs will only be stored locally.

A complete list of inputs, including changes made to the framework since the original publication, can be found 
[here](http://gfw2-data.s3.amazonaws.com/climate/carbon_model/Table_S3_data_sources__updated_20230406.pdf).

### Outputs
There are three key outputs produced: gross GHG emissions, gross removals, and net flux, all summed per pixel for 2001-2023. 
These are produced at two resolutions: 0.00025x0.00025 degrees 
(approximately 30x30 m at the equator) in 10x10 degree rasters (to make outputs a 
manageable size), and 0.04x0.04 degrees (approximately 4x4km at the equator) as global rasters for static maps.

Framework runs also automatically generate a .txt log. This log includes nearly everything that is output in the console.
This log is useful for documenting framework runs and checking for mistakes/errors in retrospect, 
although it does not capture errors that terminate runs.
For example, users can examine it to see if the correct input tiles were downloaded or if the intended tiles were used when running the framework.  

Output rasters and logs are uploaded to s3 unless the `--no-upload` flag (`-nu`) is activated as a command line argument
or no AWS s3 credentials are supplied to the Docker container.
This is good for local test runs or versions of the framework that are independent of s3 
(that is, inputs are stored locally and not on s3, and the user does not have a connection to s3 storage or s3 credentials).

#### 30-m output rasters

The 30-m outputs are used for zonal statistics (i.e. emissions, removals, or net flux in polygons of interest)
and mapping on the Global Forest Watch web platform or at small scales (where 30-m pixels can be distinguished). 
Individual emissions pixels can be assigned specific years based on Hansen loss during further analyses 
but removals and net flux are cumulative over the entire framework run and cannot be assigned specific years. 
This 30-m output is in megagrams (Mg) CO2e/ha 2001-2023 (i.e. densities) and includes all tree cover densities ("full extent"):
`((TCD2000>0 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0)`.
However, the framework is designed to be used specifically for forests, so the framework creates three derivative 30-m
outputs for each key output (gross emissions, gross removals, net flux) as well (only for the standard version, not for sensitivity analyses).
To that end, the "forest extent" rasters also have pre-2000 oil palm plantations in Indonesia and Malaysia removed
from them because carbon emissions and removals in those pixels would represent agricultural/tree crop emissions,
not forest/forest loss. 

1) Mg CO2e per pixel values for the full extent (all tree cover densities): 
   `((TCD2000>0 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0)`
2) Mg CO2e per hectare values for forest pixels only (colloquially, TCD>30 or Hansen gain pixels): 
   `(((TCD2000>30 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0) NOT IN pre-2000 plantations)`
3) Mg CO2e per pixel values for forest pixels only (colloquially, TCD>30 or Hansen gain pixels):  
   `(((TCD2000>30 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0) NOT IN pre-2000 plantations)`

The per hectare outputs are used for making pixel-level maps (essentially showing emission and removal factors), 
while the per pixel outputs are used for getting total values within areas because the values
of those pixels can be summed within areas of interest. The per pixel maps are calculated by `per hectare * pixel area/10000`.
(The pixels of the per hectare outputs should not be summed but they can be averaged in areas of interest.)
Statistics from this framework should always be based on the "forest extent" rasters, not the "full extent" rasters.
The full extent outputs should generally not be used but are created by the framework in case they are needed.

In addition to these three key outputs, there are many intermediate output rasters from the framework,
some of which may be useful for QC, analyses by area of interest, or other purposes. 
All of these are at 0.00025x0.00025 degree resolution and reported as per hectare values (as opposed to per pixel values), if applicable. 
Intermediate outputs include the annual aboveground and belowground biomass removal rates
for all kinds of forests, the type of removal factor applied to each pixel, the carbon pool densities in 2000, 
carbon pool densities in the year of tree cover loss, and the number of years in which removals occurred. 

Almost all framework output have metadata associated with them, 
viewable using the `gdalinfo` command line utility (https://gdal.org/programs/gdalinfo.html). 
Metadata includes units, date created, framework version, geographic extent, and more. Unfortunately, the metadata are not viewable 
when looking at file properties in ArcMap
or in the versions of these files downloadable from the Global Forest Watch Open Data Portal (https://data.globalforestwatch.org/).

#### 4-km output rasters

The 4-km outputs are used for static large-scale maps, like in publications and presentations. 
The units are Mt CO2e/pixel/year (in order to show absolute values). They are created using the "forest extent" 
per pixel 30-m rasters, not the "full extent" 30-m rasters. They should not be used for analysis. 

#### A note on signs

Although gross emissions are traditionally given positive (+) values and
gross removals are traditionally given negative (-) values, 
the 30-m gross removals rasters are positive, while the 4-km gross removals rasters are negative. 
Net flux at both scales can be positive or negative depending on the balance of emissions and removals in the area of interest
(negative for net sink, positive for net source).


### Running the framework
The framework runs from the command line inside a Linux Docker container. 
Once you have Docker configured on your system (download from Docker website), 
have cloned this repository (on the command line in the folder you want to clone to, `git clone https://github.com/wri/carbon-budget`), 
and have configured access to AWS (if desired), you will be able to run the framework. 
You can run the framework anywhere that the Docker container can be launched. That includes local computers (good for 
running test areas) and AWS ec2 instances (good for larger areas/global runs). 

There are two ways to run the framework: as a series of individual scripts, or from a master script, which runs the individual scripts sequentially.
Which one to use depends on what you are trying to do. 
Generally, the individual scripts (which correspond to specific framework stages) are
more appropriate for development and testing, while the master script is better for running
the main part of the framework from start to finish in one go. 
Run globally, both options iterate through a list of ~275 10 x 10 degree tiles. (Different framework stages have different numbers of tiles.)
Run all tiles in the framework extent fully through one framework stage before starting on the next stage. 
(The master script does this automatically.) If a user wants to run the framework on just one or a few tiles, 
that can be done through a command line argument (`--tile-id-list` or `-l`). 
If individual tiles are listed, only those will be run. This is a natural system for testing or for
running the framework for smaller areas. You can see the tile boundaries in `pixel_area_tile_footprints.zip` in this repo.
For example, to run the framework for Madagascar, only tiles 10S_040E, 10S_050E, and 20S_040E need to be run and the
command line argument would be `-l 10S_040E,10S_050E,20S_040E`. 

#### Building the Docker container

You can do the following on the command line in the same folder as the repository on your system.
This will enter the command line in the Docker container

For runs on a local computer, use `docker-compose` so that the Docker is mapped to your computer's drives.
In my setup, `C:/GIS/Carbon_model/test_tiles/docker_output/` on my computer is mapped to `/usr/local/tiles` in
the Docker container in `docker-compose.yaml`. If running on another computer, you will need to change the local 
folder being mapped in `docker-compose.yaml` to match your computer's directory structure. 
I do this for development and testing. 
If you want the framework to be able to download from and upload to s3, you will also need to provide 
your own AWS secret key and access key as environment variables (`-e`) in the `docker-compose run` command:

`docker-compose build`

`docker-compose run --rm -e AWS_SECRET_ACCESS_KEY=... -e AWS_ACCESS_KEY_ID=... carbon-budget`

If you don't have AWS credentials, you can still run the framework in the docker container but uploads will 
not occur. In this situation, you need all the basic input files for all tiles in the docker folder `/usr/local/tiles/`
on your computer:

`docker-compose build`

`docker-compose run --rm carbon-budget`

For runs on an AWS r5d ec2 instance (for full framework runs), use `docker build`. 
You need to supply AWS credentials for the framework to work because otherwise you won't be able to get 
output tiles off of the spot machine and you will lose your outputs when you terminate the spot machine.

`docker build . -t gfw/carbon-budget`

`docker run --rm -it -e AWS_SECRET_ACCESS_KEY=... -e AWS_ACCESS_KEY_ID=... gfw/carbon-budget`

Before doing a framework run, confirm that the dates of the relevant input and output s3 folders are correct in `constants_and_names.py`. 
Depending on what exactly the user is running, the user may have to change lots of dates in the s3 folders or change none.
Unfortunately, I can't really give better guidance than that; it really depends on what part of the framework is being run and how.
(I want to make the situations under which users change folder dates more consistent eventually.)

The framework can be run either using multiple processors or one processor. The former is for large scale framework runs,
while the latter is for framework development or running on small-ish countries that use only a few tiles. 
The user can limit use to just one processor with the `-sp` command line flag. 
One important thing to note is that if a user tries to use too many processors, the system will run out of memory and
can crash (particularly on AWS ec2 instances). Thus, it is important not to use too many processors at once.
Generally, the limitation in running the framework is the amount of memory available on the system rather than the number of processors.
Each script has been somewhat calibrated to use a safe number of processors for an r5d.24xlarge EC2 instance,
and often the number of processors being used is 1/2 or 1/3 of the actual number available.
If the tiles were smaller (e.g., 1x1 degree), more processors could be used but then there'd also be more tiles to process, so I'm not sure that would be any faster.
Users can track memory usage in real time using the `htop` command line utility in the Docker container. 


#### Individual scripts
The flux framework is comprised of many separate scripts (or stages), each of which can be run separately and
has its own inputs and output(s). There are several data preparation
scripts, several for the removals (sequestration/gain) framework, a few to generate carbon pools, one for calculating
gross emissions, one for calculating net flux, one for creating derivative outputs 
(aggregating key results into coarser resolution rasters for mapping and creating per-pixel and forest-extent outputs). 
Each script really has two parts: its `mp_` (multiprocessing) part and the part that actually does the calculations
on each 10x10 degree tile.
The `mp_` scripts (e.g., `mp_create_model_extent.py`) are the ones that are run. They download input files,
do any needed preprocessing, change output folder names as needed, list the tiles that are going to be run, etc.,
then initiate the actual work done on each tile in the script without the `mp_` prefix.
The order in which the individual stages must be run is very specific; many scripts depend on
the outputs of other scripts. Looking at the files that must be downloaded for the 
script to run will show what files must already be created and therefore what scripts must have already been
run. Alternatively, you can look at the top of `run_full_model.py` to see the order in which framework stages are run. 
The date component of the output directory on s3 generally must be changed in `constants_and_names.py`
for each output file. 

Stages are run from the project folder as Python modules: `/usr/local/app# python -m [folder.script] [arguments]`

For example: 

Extent stage: `/usr/local/app# python -m data_prep.mp_model_extent -l 00N_000E -t std -nu`

Carbon pool creation stage: `/usr/local/app# python -m carbon_pools.mp_create_carbon_pools -l 00N_000E,10S_050W -t std -ce loss -d 20239999`

##### Running the emissions stage
The gross emissions script is the only part of the framework that uses C++. Thus, the appropriate version of the C++ 
emissions file must be compiled for emissions to run. 
There are a few different versions of the emissions C++ script: one for the standard version and a few other for
sensitivity analyses. 
`mp_calculate_gross_emissions.py` will compile the correct C++ files (for all carbon pools and for soil only) 
each time it is run, so the C++ files do not need to be compiled manually. 
However, for completeness, the command for compiling the C++ script is (subbing in the actual file name): 

`c++ /usr/local/app/emissions/cpp_util/calc_gross_emissions_[VERSION].cpp -o /usr/local/app/emissions/cpp_util/calc_gross_emissions_[VERSION].exe -lgdal`

For the standard framework and the sensitivity analyses that don't specifically affect emissions, it is:

`c++ /usr/local/app/emissions/cpp_util/calc_gross_emissions_generic.cpp -o /usr/local/app/emissions/cpp_util/calc_gross_emissions_generic.exe -lgdal`

`mp_calculate_gross_emissions.py` can also be used to calculate emissions from soil only. 
This is set by the `-p` argument: `biomass_soil` or `soil_only`.  

Emissions stage: `/usr/local/app# python -m emissions.mp_calculate_gross_emissions -l 30N_090W,10S_010E -t std -p biomass_soil -d 20239999`

#### Master script 
The master script runs through all of the non-preparatory scripts in the framework: some removal factor creation, gross removals, carbon
pool generation, gross emissions for biomass+soil, gross emissions for soil only, 
net flux, aggregation, and derivative output creation. 
It includes all the arguments needed to run every script. 
Thus, the table below also explains the potential arguments for the individual framework stages. 
The user can control what framework components are run to some extent and set the date part of 
the output directories. The order in which the arguments are used does not matter (does not need to match the table below).
Preparatory scripts like creating soil carbon tiles or mangrove tiles are not included in the master script because
they are run very infrequently. 

| Argument | Short argument | Required/Optional | Relevant stage | Description | 
| -------- | ----- | ----------- | ------- | ------ |
| `model-type` | `-t` | Required | All | Standard version (`std`) or a sensitivity analysis. Refer to `constants_and_names.py` for valid list of sensitivity analyses. |
| `stages` | `-s` | Required | All | The framework stage at which the run should start. `all` will run the following stages in this order: model_extent, forest_age_category_IPCC, annual_removals_IPCC, annual_removals_all_forest_types, gain_year_count, gross_removals_all_forest_types, carbon_pools, gross_emissions_biomass_soil, gross_emissions_soil_only, net_flux, create_derivative_outputs |
| `tile-id-list` | `-l` | Required | All | List of tile ids to use in the framework. Should be of form `00N_110E` or `00N_110E,00N_120E` or `all` |
| `run-through` | `-r` | Optional | All | If activated, run stage provided in `stages` argument and all following stages. Otherwise, run only stage in `stages` argument. Activated with flag. |
| `run-date` | `-d` | Optional | All | Date of run. Must be format YYYYMMDD. This sets the output folder in s3. |
| `no-upload` | `-nu` | Optional | All | No files are uploaded to s3 during or after framework run (including logs and framework outputs). Use for testing to save time. When AWS credentials are not available, upload is automatically disabled and this flag does not have to be manually activated. |
| `single-processor` | `-sp` | Optional | All | Tile processing will be done without `multiprocessing` module whenever possible, i.e. no parallel processing. Use for testing. |
| `log-note` | `-ln`| Optional | All | Adds text to the beginning of the log |
| `carbon-pool-extent` | `-ce` | Optional | Carbon pool creation | Extent over which carbon pools should be calculated: loss or 2000 or loss,2000 or 2000,loss |
| `std-net-flux-aggreg` | `-std` | Optional | Aggregation | The s3 standard framework net flux aggregated tif, for comparison with the sensitivity analysis map. |
| `save-intermdiates` | `-si`| Optional | `run_full_model.py` | Intermediate outputs are not deleted within `run_full_model.py`. Use for local framework runs. If uploading to s3 is not enabled, intermediate files are automatically saved. |
| `mangroves` | `-ma` | Optional | `run_full_model.py` | Create mangrove removal factor tiles as the first stage. Activate with flag. |
| `us-rates` | `-us` | Optional | `run_full_model.py` | Create US-specific removal factor tiles as the first stage (or second stage, if mangroves are enabled). Activate with flag. |

These are some sample commands for running the flux framework in various configurations. You wouldn't necessarily want to use all of these;
they simply illustrate different configurations for the command line arguments. 
Like the individual framework stages, the full framework run script is also run from the project folder with the `-m` flag.

Run: standard version; save intermediate outputs; run framework from annual_removals_IPCC;
upload to folder with date 20239999; run 00N_000E; get carbon pools at time of loss; add a log note;
use multiprocessing (implicit because no `-sp` flag); only run listed stage (implicit because no -r flag)

`python -m run_full_model -t std -si -s annual_removals_IPCC -d 20239999 -l 00N_000E -ce loss -ln "00N_000E test"`

Run: standard version; save intermediate outputs; run framework from annual_removals_IPCC; run all subsequent framework stages;
do not upload outputs to s3; run 00N_000E; get carbon pools at time of loss; add a log note; 
use multiprocessing (implicit because no -sp flag)

`python -m run_full_model -t std -si -s annual_removals_IPCC -r -nu -l 00N_000E -ce loss -ln "00N_000E test"`

Run: standard version; save intermediate outputs; run framework from the beginning; run all framework stages;
upload to folder with date 20239999; run 00N_000E; get carbon pools at time of loss; add a log note;
use multiprocessing (implicit because no -sp flag)

`python -m run_full_model -t std -si -s all -r -d 20239999 -l 00N_000E -ce loss -ln "00N_000E test"`

Run: standard version; save intermediate outputs; run framework from the beginning; run all framework stages;
upload to folder with date 20239999; run 00N_000E, 10N_110E, and 50N_080W; get carbon pools at time of loss; 
add a log note; use multiprocessing (implicit because no -sp flag)

`python -m run_full_model -t std -si -s all -r -d 20239999 -l 00N_000E,10N_110E,50N_080W -ce loss -ln "00N_000E test"`

Run: standard version; run framework from the beginning; run all framework stages;
upload to folder with date 20239999; run 00N_000E and 00N_010E; get carbon pools at time of loss; 
use singleprocessing; add a log note; do not save intermediate outputs (implicit because no -si flag)

`python -m run_full_model -t std -s all -r -nu -d 20239999 -l 00N_000E,00N_010E -ce loss -sp -ln "Two tile test"`

FULL STANDARD FRAMEWORK RUN: standard framework; save intermediate outputs; run framework from the beginning; run all framework stages;
run all tiles; get carbon pools at time of loss; add a log note;
upload outputs to s3 with dates specified in `constants_and_names.py` (implicit because no -nu flag); 
use multiprocessing (implicit because no -sp flag)

`python -m run_full_model -t std -si -s all -r -l all -ce loss -ln "Running all tiles"`

### Sensitivity analysis
NOT SUPPORTED AT THIS TIME.

Several variations of the framework are included; these are the sensitivity variants, as they use different inputs or parameters. 
They can be run by changing the `--model-type` (`-t`) argument from `std` to an option found in `constants_and_names.py`. 
Each sensitivity analysis variant starts at a different stage in the framework and runs to the final stage,
except that sensitivity analyses do not include the creation of the supplementary outputs (per pixel tiles, forest extent tiles).
Some use all tiles and some use a smaller extent.

| Sensitivity analysis | Description | Extent | Starting stage | 
| -------- | ----------- | ------ | ------ |
| `std` | Standard framework | Global | `mp_model_extent.py` |
| `maxgain` | Maximum number of years of gain (removals) for gain-only and loss-and-gain pixels | Global | `gain_year_count_all_forest_types.py` |
| `no_shifting_ag` | Shifting agriculture driver is replaced with commodity-driven deforestation driver | Global | `mp_calculate_gross_emissions.py` |
| `convert_to_grassland` | Forest is assumed to be converted to grassland instead of cropland in the emissions framework| Global | `mp_calculate_gross_emissions.py` |
| `biomass_swap` | Uses Saatchi 1-km AGB map instead of Baccini 30-m map for starting carbon densities | Extent of Saatchi map, which is generally the tropics| `mp_model_extent.py` |
| `US_removals` | Uses IPCC default removal factors for the US instead of US-specific removal factors from USFS FIA | Continental US | `mp_annual_gain_rate_AGC_BGC_all_forest_types.py` |
| `no_primary_gain` | Primary forests and IFLs are assumed to not have any removals| Global | `mp_forest_age_category_IPCC.py` |
| `legal_Amazon_loss` | Uses Brazil's PRODES annual deforestation system instead of Hansen loss | Legal Amazon| `mp_model_extent.py` |
| `Mekong_loss` | Uses Hansen loss v2.0 (multiple loss in same pixel). NOTE: Not used for flux framework v1.2.0, so this is not currently supported. | Mekong region | N/A |


### Updating the framework with new tree cover loss
For the current general configuration of the framework, these are the changes that need to be made to update the
framework with a new year of tree cover loss data. In the order in which the changes would be needed for rerunning the framework:

1) Update the framework version variable `version` in `constants_and_names.py`.

2) Change the tree cover loss tile source to the new tree cover loss tiles in `constants_and_names.py`.
Change the tree cover loss tile pattern in `constants_and_names.py`.

3) Change the number of loss years variable `loss_years` in `constants_and_names.py`.

4) In `constants.h` (emissions/cpp_util/), change the number of framework years (`int model_years`) 
   and the loss tile pattern (`char lossyear[]`).

5) In `equations.cpp` (emissions/cpp_util/), change the number of framework years (`int model_years`). 

6) Obtain and pre-process the updated drivers of tree cover loss framework and tree cover loss from fires 
   using `mp_prep_other_inputs_annual.py`. Note that the drivers map probably needs to be reprojected to WGS84 
   and resampled (0.005x0.005 deg) in ArcMap or similar before processing into 0.00025x0.00025 deg 10x10 tiles using this script. 
   `mp_prep_other_inputs_annual.py` has some additional notes about that. You can choose which set of tiles to pre-process 
   by providing the following options for the process argument (-p):
   - tcld: Pre-processes drivers of tree cover loss tiles 
   - tclf: Pre-processes tree cover loss due to fires tiles 
   - all: Pre-processes both drivers of tree cover loss and tree cover loss due to fires tiles

7) Make sure that changes in forest age category produced by `mp_forest_age_category_IPCC.py` 
   and the number of gain years produced by `mp_gain_year_count_all_forest_types.py` still make sense.

Strictly speaking, if only the drivers, tree cover loss from fires, and tree cover loss are being updated, 
the framework only needs to be run from forest_age_category_IPCC onwards (loss affects IPCC age category).
However, for completeness, I suggest running all stages of the framework from model_extent onwards for an update so that
framework outputs from all stages have the same version in their metadata and the same dates of output as the framework stages
that are actually being changed. A full framework run (all tiles, all stages) takes about 18 hours on an r5d.24xlarge 
EC2 instance with 3.7 TB of storage and 96 processors.


### Other modifications to the framework
It is recommended that any changes to the framework be tested in a local Docker instance before running on an ec2 instance.
I like to output files to test folders on s3 with dates 20239999 because that is clearly not a real run date. 
A standard development route is: 

1) Make changes to a single framework script and run using the single processor option on a single tile (easiest for debugging) in local Docker.

2) Run single script on a few representative tiles using a single processor in local Docker.

3) Run single script on a few representative tiles using multiple processor option in local Docker.

4) Run the master script on a few representative tiles using multiple processor option in local Docker to 
   confirm that changes work when using master script.

5) Run single script on a few representative tiles using multiple processors on ec2 instance (need to commit and push changes to GitHub first).

6) Run master script on all tiles using multiple processors on EC2 instance. 
   If the changes likely affected memory usage, make sure to watch memory with `htop` to make sure that too much memory isn't required. 
   If too much memory is needed, reduce the number of processors being called in the script. 

Depending on the complexity of the changes being made, some of these steps can be ommitted. Or if only a few tiles are 
being modeled (for a small country), only steps 1-4 need to be done.  

### Running framework tests
There is an incipient testing component using `pytest`. It is currently only available for the deadwood and litter
carbon pool creation step of the framework but can be expanded to other aspects of the framework. 
Tests can be run from the project folder with the command `pytest`. 
You can get more verbose output with `pytest -s`.
To run tests that just have a certain flag (e.g., `rasterio`), you can do `pytest -m rasterio -s`.


### Dependencies
Theoretically, this framework should run anywhere that the correct Docker container can be started 
and there is access to the AWS s3 bucket or all inputs are in the correct folder in the Docker container. 
The Docker container should be self-sufficient in that it is configured to include the right Python packages, C++ compiler, GDAL, etc.
It is described in `Dockerfile`, with Python requirements (installed during Docker creation) in `requirements.txt`.
On an AWS ec2 instance, I have only run it on r5d instance types but it might be able to run on others.
At the least, it needs a certain type of memory configuration on the ec2 instance (at least one large SSD volume, I believe). 
Otherwise, I do not know the limitations and constraints on running this framework in an ec2 instance. 

### Contact information
David Gibbs: david.gibbs@wri.org

Melissa Rose: melissa.rose@wri.org

Nancy Harris: nancy.harris@wri.org

Global Forest Watch, World Resources Institute, Washington, D.C.
