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
has its own inputs and output(s). Combined, these comprise the flux model. 

##### Master script 


#### Contact information

David Gibbs

Global Forest Watch, World Resources Institute, Washington, D.C.

david.gibbs@wri.org