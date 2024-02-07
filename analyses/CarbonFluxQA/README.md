### Purpose

    The purpose of this tool is to automate the zonal statistics portion of the QAQC process for the
    Annual Carbon Flux Model (Harris, 2021) Update. Each year, the Carbon Flux Model is run with updated activity data
    for tree cover loss (Hansen, 2013) and auxiliary inputs. Before the data is launched on Global Forest Watch, the
    outputs are compared across platforms and methods including the GeoTrellis Tool, GFW Dashboard download spreadsheets,
    the GFW API, and ArcGIS Zonal Statistics calculations. 

### Overview

    This code has been automated to create the working directory folder structure, download all of the required data 
    from s3, and produce a summary csv with the resulting zonal statisitcs. 

### User Inputs

#### Area(s) of Interest:

    The only file(s) required by the user are the shapefile(s) for the area(s) of interest. The shapefile(s) need to be 
    located in a subfolder in the woking directory named "AOIS".

    This tool is set up to run statistics for as many areas of interest as the user provides. We used country 
    boundaries for Indonesia and Gambia from the GADM 3.6 Dataset, available for download here:
    [](https://gadm.org/download_country_v3.html).

    The Indonesia boundary is IND.14.13 and the Gambia boundary is GMB.2. 

    These inputs will need to be updated if and when GFW switches to a newer version of GADM. 
 
| Dataset             | Directory                 | Description                             |
|---------------------|---------------------------|-----------------------------------------|
| Area(s) of Interest | {working_directory}/AOIS/ | Shapefiles for the area(s) of interest. |


#### User-Specified Parameters: 
    You must update the constants_and_names.py file with the path to your working_directory. This is the folder which 
    contains your areas of interest and where all of the data and results will be saved. There are a number of other 
    arguments in the constants_and_names.py file that users have the option to update. A description of each argument is 
    detailed below:  

| Argument                | Description                                                                  | Type       |
|-------------------------|------------------------------------------------------------------------------|------------|
| working_directory       | Directory which contains the AOIS subfolder.                                 | String     |
| overwrite_arcgis_output | Whether or not you want to overwrite previous arcpy outputs.                 | Boolean    |
| loss_years              | Number of years of tree cover loss in the TCL dataset.                       | Integer    |
| model_run_date          | s3 folder where per-pixel outputs from most recent model run are located.    | String     |
| tile_list               | List of 10 x 10 degree tiles that overlap with all aoi(s).                   | List       |
| tile_dictionary         | Dictionary that matches each country to their overlapping tiles.             | Dictionary |
| extent                  | Which tile set(s) to download for zonal stats (options: forest, full, both). | String     |
| tcd_threshold           | List of tree cover density thresholds to mask by.                            | List       |
| gain                    | Whether to include tree cover gain pixels in masks.                          | Boolean    |
| save_intermediates      | Whether to save intermediate masks (useful for troubleshooting).             | Boolean    |


### Datasets

#### Carbon Flux Model Data:

    Three separate outputs from the Carbon Flux Model, each with two different extents, are used as inputs in 
    this tool. This is a total of six different possible inputs. Inputs include gross emissions (all gasses), 
    gross removals (CO2), and net flux (CO2e). All are in inputs Mg / pixel. You have the option to calculate 
    zonal statistics according to tile extent: forest extent only, full extent only, or both extents.

| AOI | Extent | Type            | Units         | Tile     |
|-----|--------|-----------------|---------------|----------|
| IDN | Forest | Gross Emissions | Mg CO2e/pixel | 00N_110E |
| IDN | Forest | Gross Removals  | Mg CO2/pixel  | 00N_110E |
| IDN | Forest | Net Flux        | Mg CO2e/pixel | 00N_110E |
| IDN | Full   | Gross Emissions | Mg CO2e/pixel | 00N_110E |
| IDN | Full   | Gross Removals  | Mg CO2/pixel  | 00N_110E |
| IDN | Full   | Net Flux        | Mg CO2e/pixel | 00N_110E |
| GMB | Forest | Gross Emissions | Mg CO2/pixel  | 20N_020W |
| GMB | Forest | Gross Removals  | Mg CO2e/pixel | 20N_020W |
| GMB | Forest | Net Flux        | Mg CO2/pixel  | 20N_020W |
| GMB | Full   | Gross Emissions | Mg CO2e/pixel | 20N_020W |
| GMB | Full   | Gross Removals  | Mg CO2/pixel  | 20N_020W |
| GMB | Full   | Net Flux        | Mg CO2e/pixel | 20N_020W |


#### Auxiliary Datasets:

    Other auxiliary inputs for this tool include:

| Dataset              | Use Description                                                                          |
|----------------------|------------------------------------------------------------------------------------------|
| Tree Cover Gain      | Used to create tree cover gain mask.                                                     |
| Above Ground Biomass | Used to filter tree cover gain mask to only pixels that contain biomass.                 |
| Tree Cover Density   | Used to create density threshold mask.                                                   |
| Mangrove Extent      | Used to create Mangrove mask. Areas of mangrove included in mask.                        |
| Pre-2000 Plantations | Used to create Pre-2000 plantations mask. Pre-2000 plantations masked from calculations. |
| Tree Cover Loss      | Used to calculate annual emissions.                                                      |


### Outputs:

    The final outputs include one csv file summarizing results for each entry described in the "Carbon Flux Model 
    Inputs" table. Additionally, separate csv files for annual emissions are produced. 

### Code Summary

#### calculate_zonal_statistics
    This file is for running the code in its entirety. This script will execute all functions in the repository 
    consecutively and produce output csvs.

#### constants_and_names
    This file stores all of the input arguments provided to the functions. Any changes to the arguments in this file 
    will be applies to all scripts in the repository. 

#### funcs
    This file stores all of the functions used in the tool. Any edits to functions would be made in this file.

#### components

    This folder houses individual scripts for running separate functions. These can be useful for running particular 
    functions separately and testing edits/ troubleshootins. Each function is described below. 

##### 01 Download Files
    This script creates the folder structure (other than the AOI folder) and downloads all of the required datasets from 
    s3 using the paths provided in the the constant_and_names file. You will need to set your AWS_ACCESS_KEY and 
    AWS_SECRET_ACCESS_KEY in your environment variables for this step to work (assuming you have s3 copy permissions).

##### 02 Create Masks
    This script uses data on tree cover density, tree cover gain, mangrove extent, WHRC biomass, and pre-2000 plantations
    to replicate the masks that are used in GFW data processing. This facilitates direct comparison with results from the GFW 
    dashboard, geotrellis client, and GFW API. The script creates masks based on criteria for each input dataset and saves these
    masks in a sub directory. These masks are used later as extent inputs in the Zonal Statistics Masked script.

##### 03 Zonal Stats Masked
    This script calculates zonal statistics for each area of interest and carbon dataset combination and applies each  
    mask saved in the Mask/ Mask directory. The number of masks depend on the number of tcd_threshold values you indicated 
    and wheter or not you set the save_intermediate flag to True. At minimum, this will include final masks for each 
    tcd_threshold value and at maximum this will be the number of tcd_threshold values multiplied by the number of 
    intermediate masks (this varies depending on whether or not the area of interest includes mangroves and/ or 
    pre-2000 plantations). 

##### 04 Zonal Stats Annualized
    This script calculates annual emissions in each area of interest using the tree cover loss dataset. 

##### 05 Zonal Stats Cleaned
    This script utilizes pandas to compile the results of all analyses and export them into a user-friendly csv file.

### Running the Code
    To run the code, you will need to set up a workspace with the AOI inputs organized into the correct directory, 
    update the user inputs section of the constants_and_names.py file, and provide your AWS_ACCESS_KEY and 
    AWS_SECRET_ACCESS_KEY in your environment variables. 

    This code is built on arcpy, which will require a valid ArcGIS license to run.

### Other Notes
    Updates in progress include...
    
    - Currently, the annual zonal stats do not sum to the total emissions using the TCL dataset from s3, 
    but they do when using the previous TCL clipped rasters. For now, reach out to Erin Glen or Melissa Rose for 
    the previous TCL clipped rasters. 

#### Contact Info
    Erin Glen - erin.glen@wri.org
    Melissa Rose - melissa.rose@wri.org