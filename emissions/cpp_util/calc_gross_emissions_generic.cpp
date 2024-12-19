// Implements the calculation of gross emissions from biomass and soil and stores the outputs as rasters.
// This is essentially one large decision tree that iterates pixel by pixel across each Hansen tile.
// The first split is whether the pixel has carbon and loss.
// NOTE: The way I've set up the model, all carbon pixels have loss (i.e. only carbon pixels with loss are input to the model).
// The next split is which driver of tree cover loss the pixel falls in.
// The next split is whether the pixel is on peat, followed by whether there was fire.
// The splits after that depend on the particular tree, but include ecozone (boreal/temperate/tropical), IFL, and plantation status.
// Each series of splits ends with a particular equation that calculates gross emissions in that case.
// The equations sometimes rely on constants, which are calculated for each pixel and are based on properties of
// underlying pixels (e.g., ecozone, climate zone, IFL status) (calculated in equations.cpp).
// Each end point of the decision tree gets its own code, so that it's easier to tell what branch of the decision tree
// each pixel came from. That makes checking the results easier, too.
// These codes are summarized in carbon-budget/emissions/node_codes.txt
// Because emissions are separately output for CO2, CH4, and N2O, each model endpoint has an emissions raster for each GHG.
// CH4 and N2O are summed to create a non-CO2 emissions raster and all three GHGs are summed to create a total emissions (all gases) for each pixel.
// Compile with:
// c++ /usr/local/app/emissions/cpp_util/calc_gross_emissions_generic.cpp -o /usr/local/app/emissions/cpp_util/calc_gross_emissions_generic.exe -lgdal


#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <fstream>
#include <sys/stat.h>
#include <math.h>
#include <algorithm>
#include <string.h>
#include <stdint.h>
#include <sstream>
#include <iomanip>

#include <gdal/gdal_priv.h>
#include <gdal/cpl_conv.h>
#include <gdal/ogr_spatialref.h>

// These provide constants for the emissions equations and universal constants
#include "flu_val.cpp"
#include "equations.cpp"
#include "constants.h"

using namespace std;

//to compile:  c++ calc_gross_emissions.cpp -o calc_gross_emissions.exe -lgdal
int main(int argc, char* argv[])
{
// If code is run other than <program name> <tile id> , it will raise this error.
if (argc != 4){cout << "Use <program name> <tile id><sensit_type><folder>" << endl; return 1;}

// Input arguments
string tile_id = argv[1];    // The tile id comes from the second argument. The first argument is the name of this code.
string sensit_type = argv[2];   // For standard model or sensitivity analyses that use the standard emissions model.
                             // Used to name the input carbon pool tiles and output gross emissions tiles.
string infolder = argv[3];     // The folder which has all the input files

cout << "Gross emissions C++ infolder:" << infolder << endl;

// Model constants
int model_years;    // How many loss years are in the model
model_years = constants::model_years;
string model_years_str;
model_years_str = to_string(model_years);

int CH4_equiv;      // The CO2 equivalency (global warming potential) of CH4
CH4_equiv = constants::CH4_equiv;

int N2O_equiv;      // The CO2 equivalency (global warming potential) of N2O
N2O_equiv = constants::N2O_equiv;

float C_to_CO2;       // The conversion of carbon to CO2
C_to_CO2 = constants::C_to_CO2;

float biomass_to_c;    // Fraction of carbon in biomass
biomass_to_c = constants::biomass_to_c;

int tropical;       // The ecozone code for the tropics
tropical = constants::tropical;

int temperate;      // The ecozone code for the temperate zone
temperate = constants::temperate;

int boreal;         // The ecozone code for the boreal zone
boreal = constants::boreal;

int soil_emis_period;      // The number of years over which soil emissions are calculated (separate from model years)
soil_emis_period = constants::soil_emis_period;

float shift_cult_flu; // F_lu for shifting cultivation (fraction of soil C not emitted over 20 years)
shift_cult_flu = constants::shift_cult_flu;

float settlements_flu; // F_lu for settlements and infrastructure (fraction of soil C not emitted over 20 years)
settlements_flu = constants::settlements_flu;

float hard_commod_flu; // F_lu for hard_commodities (fraction of soil C not emitted over 20 years)
hard_commod_flu = constants::hard_commod_flu;


// Input files
// Carbon pools
// Carbon pools default to the standard model names
string agc_name = infolder + tile_id + constants::AGC_emis_year + ".tif";
string bgc_name = infolder + tile_id + constants::BGC_emis_year + ".tif";
string dead_name = infolder + tile_id + constants::deadwood_C_emis_year + ".tif";
string litter_name = infolder + tile_id + constants::litter_C_emis_year + ".tif";
string soil_name = infolder + tile_id + constants::soil_C_emis_year + ".tif";

if (sensit_type != "std") {
    agc_name = infolder + tile_id + constants::AGC_emis_year + "_" + sensit_type +".tif";
    bgc_name = infolder + tile_id + constants::BGC_emis_year + "_" + sensit_type +".tif";
    dead_name = infolder + tile_id + constants::deadwood_C_emis_year + "_" + sensit_type +".tif";
    litter_name = infolder + tile_id + constants::litter_C_emis_year + "_" + sensit_type +".tif";
    soil_name = infolder + tile_id + constants::soil_C_emis_year + "_" + sensit_type +".tif";
}

// Other inputs
string loss_name = infolder + constants::lossyear + tile_id + ".tif";

if (sensit_type == "legal_Amazon_loss") {
    loss_name = infolder + tile_id + constants::legal_Amazon_loss;
}

string burn_name = infolder + tile_id + constants::burnyear;
string ecozone_name = infolder + tile_id + constants::fao_ecozones;
string climate_name = infolder + tile_id + constants::climate_zones;
string drivermodel_name = infolder + tile_id + constants::tcl_drivers;
string peat_name = infolder + tile_id + constants::peat_mask;
string ifl_primary_name = infolder + tile_id + constants::ifl_primary;
string plant_name = infolder + tile_id + constants::plantation_type;

// Output files: tonnes CO2/ha for each output and the node for the decision tree that determines emissions
// Output files default to the standard model names
string out_name_all_gases_all_drivers = tile_id + constants::all_gases_all_drivers_emis + model_years_str + ".tif";
string out_name_CO2_only_all_drivers = tile_id + constants::CO2_only_all_drivers_emis + model_years_str + ".tif";
string out_name_non_CO2_all_drivers = tile_id + constants::non_CO2_all_drivers_emis + model_years_str + ".tif";
string out_name_CH4_only_all_drivers = tile_id + constants::CH4_only_all_drivers_emis + model_years_str + ".tif";
string out_name_N2O_only_all_drivers = tile_id + constants::N2O_only_all_drivers_emis + model_years_str + ".tif";
string out_name_node_code = tile_id + constants::decision_tree_all_drivers_emis + model_years_str + ".tif";

if (sensit_type != "std") {
    out_name_all_gases_all_drivers = tile_id + constants::all_gases_all_drivers_emis + model_years_str + "_" + sensit_type + ".tif";
    out_name_CO2_only_all_drivers = tile_id + constants::CO2_only_all_drivers_emis + model_years_str + "_" + sensit_type + ".tif";
    out_name_non_CO2_all_drivers = tile_id + constants::non_CO2_all_drivers_emis + model_years_str + "_" + sensit_type + ".tif";
    out_name_CH4_only_all_drivers = tile_id + constants::CH4_only_all_drivers_emis + model_years_str + "_" + sensit_type + ".tif";
    out_name_N2O_only_all_drivers = tile_id + constants::N2O_only_all_drivers_emis + model_years_str + "_" + sensit_type + ".tif";
    out_name_node_code = tile_id + constants::decision_tree_all_drivers_emis + model_years_str + "_" + sensit_type + ".tif";
}

//// Print input and output tile names
//cout << "AGC full tile:" << agc_name << endl;
//cout << "BGC full tile:" << bgc_name << endl;
//cout << "deadwood full tile:" << dead_name << endl;
//cout << "litter full tile:" << litter_name << endl;
//cout << "soil full tile:" << soil_name << endl;
//
//cout << "burn tile:" << burn_name << endl;
//cout << "ecozone tile:" << ecozone_name << endl;
//cout << "climate zone tile:" << climate_name << endl;
//cout << "driver tile:" << drivermodel_name << endl;
//cout << "peat tile:" << peat_name << endl;
//cout << "ifl tile:" << ifl_primary_name << endl;
//cout << "plantation tile:" << plant_name << endl;


// Setting up the variables to hold the pixel location in x/y values
int x, y;
int xsize, ysize;
double GeoTransform[6]; // Fetch the affine transformation coefficients
double ulx, uly; double pixelsize;

// Initialize GDAL for reading.
// Each of these "INBAND" are later associated with the string variables defined above.
GDALAllRegister();
GDALDataset  *INGDAL_AGC; GDALRasterBand  *INBAND_AGC;
GDALDataset  *INGDAL_BGC; GDALRasterBand  *INBAND_BGC;
GDALDataset  *INGDAL_DRIVERMODEL; GDALRasterBand  *INBAND_DRIVERMODEL;
GDALDataset  *INGDAL_LOSS; GDALRasterBand  *INBAND_LOSS;
GDALDataset  *INGDAL_PEAT; GDALRasterBand  *INBAND_PEAT;
GDALDataset  *INGDAL_BURN; GDALRasterBand  *INBAND_BURN;
GDALDataset  *INGDAL_IFL_PRIMARY; GDALRasterBand  *INBAND_IFL_PRIMARY;
GDALDataset  *INGDAL_ECOZONE; GDALRasterBand  *INBAND_ECOZONE;
GDALDataset  *INGDAL_CLIMATE; GDALRasterBand  *INBAND_CLIMATE;
GDALDataset  *INGDAL_DEAD; GDALRasterBand  *INBAND_DEAD;
GDALDataset  *INGDAL_LITTER; GDALRasterBand  *INBAND_LITTER;
GDALDataset  *INGDAL_SOIL; GDALRasterBand  *INBAND_SOIL;
GDALDataset  *INGDAL_PLANT; GDALRasterBand  *INBAND_PLANT;

//open file (string variables defined above) and assign it extent and projection
INGDAL_AGC = (GDALDataset *) GDALOpen(agc_name.c_str(), GA_ReadOnly );
INBAND_AGC = INGDAL_AGC->GetRasterBand(1);

INGDAL_BGC = (GDALDataset *) GDALOpen(bgc_name.c_str(), GA_ReadOnly );
INBAND_BGC = INGDAL_BGC->GetRasterBand(1);

INGDAL_DRIVERMODEL = (GDALDataset *) GDALOpen(drivermodel_name.c_str(), GA_ReadOnly );
INBAND_DRIVERMODEL = INGDAL_DRIVERMODEL->GetRasterBand(1);

INGDAL_LOSS = (GDALDataset *) GDALOpen(loss_name.c_str(), GA_ReadOnly );
INBAND_LOSS = INGDAL_LOSS->GetRasterBand(1);

INGDAL_PEAT = (GDALDataset *) GDALOpen(peat_name.c_str(), GA_ReadOnly );
INBAND_PEAT = INGDAL_PEAT->GetRasterBand(1);

INGDAL_BURN = (GDALDataset *) GDALOpen(burn_name.c_str(), GA_ReadOnly );
INBAND_BURN = INGDAL_BURN->GetRasterBand(1);

INGDAL_IFL_PRIMARY = (GDALDataset *) GDALOpen(ifl_primary_name.c_str(), GA_ReadOnly );
INBAND_IFL_PRIMARY = INGDAL_IFL_PRIMARY->GetRasterBand(1);

INGDAL_ECOZONE = (GDALDataset *) GDALOpen(ecozone_name.c_str(), GA_ReadOnly );
INBAND_ECOZONE = INGDAL_ECOZONE->GetRasterBand(1);

INGDAL_CLIMATE = (GDALDataset *) GDALOpen(climate_name.c_str(), GA_ReadOnly );
INBAND_CLIMATE = INGDAL_CLIMATE->GetRasterBand(1);

INGDAL_DEAD = (GDALDataset *) GDALOpen(dead_name.c_str(), GA_ReadOnly );
INBAND_DEAD = INGDAL_DEAD->GetRasterBand(1);

INGDAL_LITTER = (GDALDataset *) GDALOpen(litter_name.c_str(), GA_ReadOnly );
INBAND_LITTER = INGDAL_LITTER->GetRasterBand(1);

INGDAL_SOIL = (GDALDataset *) GDALOpen(soil_name.c_str(), GA_ReadOnly );
INBAND_SOIL = INGDAL_SOIL->GetRasterBand(1);

INGDAL_PLANT = (GDALDataset *) GDALOpen(plant_name.c_str(), GA_ReadOnly );
INBAND_PLANT = INGDAL_PLANT->GetRasterBand(1);

// The rest of the code runs on the size of INBAND_DRIVERMODEL. This can be changed.
xsize=INBAND_AGC->GetXSize();
ysize=INBAND_AGC->GetYSize();
INGDAL_AGC->GetGeoTransform(GeoTransform);

ulx=GeoTransform[0];
uly=GeoTransform[3];
pixelsize=GeoTransform[1];

// // Manually change this to test the script on a small part of the raster. This starts at top left of the tile.
//xsize = 40000;
//ysize = 1100;

// Print the raster size and resolution. Should be 40,000 x 40,000 and pixel size 0.00025.
cout << "Gross emissions generic model C++ parameters: " << xsize <<", "<< ysize <<", "<< ulx <<", "<< uly << ", "<< pixelsize << endl;

// Initialize GDAL for writing
GDALDriver *OUTDRIVER;
GDALDataset *OUTGDAL_ALLDRIVERS_ALLGASSES;  // All drivers, all gases
GDALDataset *OUTGDAL_ALLDRIVERS_CO2ONLY;  // All drivers, CO2 only
GDALDataset *OUTGDAL_ALLDRIVERS_NONCO2;  // All drivers, non-CO2 (methane + nitrous oxide)
GDALDataset *OUTGDAL_ALLDRIVERS_CH4ONLY;  // All drivers, methane
GDALDataset *OUTGDAL_ALLDRIVERS_N2OONLY;  // All drivers, nitrous oxide
GDALDataset *OUTGDAL_NODE_CODE;  // Decision tree node

GDALRasterBand *OUTBAND_ALLDRIVERS_ALLGASSES;
GDALRasterBand *OUTBAND_ALLDRIVERS_CO2ONLY;
GDALRasterBand *OUTBAND_ALLDRIVERS_NONCO2;
GDALRasterBand *OUTBAND_ALLDRIVERS_CH4ONLY;
GDALRasterBand *OUTBAND_ALLDRIVERS_N2OONLY;
GDALRasterBand *OUTBAND_NODE_CODE;

OGRSpatialReference oSRS;
char *OUTPRJ = NULL;
char **papszOptions = NULL;
papszOptions = CSLSetNameValue( papszOptions, "COMPRESS", "DEFLATE" );
OUTDRIVER = GetGDALDriverManager()->GetDriverByName("GTIFF");
if( OUTDRIVER == NULL ) {cout << "no driver" << endl; exit( 1 );};
oSRS.SetWellKnownGeogCS( "WGS84" );
oSRS.exportToWkt( &OUTPRJ );
double adfGeoTransform[6] = { ulx, pixelsize, 0, uly, 0, -1*pixelsize };

// All gases, all drivers combined
OUTGDAL_ALLDRIVERS_ALLGASSES = OUTDRIVER->Create( out_name_all_gases_all_drivers.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL_ALLDRIVERS_ALLGASSES->SetGeoTransform(adfGeoTransform); OUTGDAL_ALLDRIVERS_ALLGASSES->SetProjection(OUTPRJ);
OUTBAND_ALLDRIVERS_ALLGASSES = OUTGDAL_ALLDRIVERS_ALLGASSES->GetRasterBand(1);
OUTBAND_ALLDRIVERS_ALLGASSES->SetNoDataValue(0);

// CO2 only, all drivers combined
OUTGDAL_ALLDRIVERS_CO2ONLY = OUTDRIVER->Create( out_name_CO2_only_all_drivers.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL_ALLDRIVERS_CO2ONLY->SetGeoTransform(adfGeoTransform); OUTGDAL_ALLDRIVERS_CO2ONLY->SetProjection(OUTPRJ);
OUTBAND_ALLDRIVERS_CO2ONLY = OUTGDAL_ALLDRIVERS_CO2ONLY->GetRasterBand(1);
OUTBAND_ALLDRIVERS_CO2ONLY->SetNoDataValue(0);

// Non-CO2, all drivers combined
OUTGDAL_ALLDRIVERS_NONCO2 = OUTDRIVER->Create( out_name_non_CO2_all_drivers.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL_ALLDRIVERS_NONCO2->SetGeoTransform(adfGeoTransform); OUTGDAL_ALLDRIVERS_NONCO2->SetProjection(OUTPRJ);
OUTBAND_ALLDRIVERS_NONCO2 = OUTGDAL_ALLDRIVERS_NONCO2->GetRasterBand(1);
OUTBAND_ALLDRIVERS_NONCO2->SetNoDataValue(0);

// CH4 only, all drivers combined
OUTGDAL_ALLDRIVERS_CH4ONLY = OUTDRIVER->Create( out_name_CH4_only_all_drivers.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL_ALLDRIVERS_CH4ONLY->SetGeoTransform(adfGeoTransform); OUTGDAL_ALLDRIVERS_CH4ONLY->SetProjection(OUTPRJ);
OUTBAND_ALLDRIVERS_CH4ONLY = OUTGDAL_ALLDRIVERS_CH4ONLY->GetRasterBand(1);
OUTBAND_ALLDRIVERS_CH4ONLY->SetNoDataValue(0);

// N2O only, all drivers combined
OUTGDAL_ALLDRIVERS_N2OONLY = OUTDRIVER->Create( out_name_N2O_only_all_drivers.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL_ALLDRIVERS_N2OONLY->SetGeoTransform(adfGeoTransform); OUTGDAL_ALLDRIVERS_N2OONLY->SetProjection(OUTPRJ);
OUTBAND_ALLDRIVERS_N2OONLY = OUTGDAL_ALLDRIVERS_N2OONLY->GetRasterBand(1);
OUTBAND_ALLDRIVERS_N2OONLY->SetNoDataValue(0);

// Decision tree node
OUTGDAL_NODE_CODE = OUTDRIVER->Create( out_name_node_code.c_str(), xsize, ysize, 1, GDT_UInt16, papszOptions );
OUTGDAL_NODE_CODE->SetGeoTransform(adfGeoTransform); OUTGDAL_NODE_CODE->SetProjection(OUTPRJ);
OUTBAND_NODE_CODE = OUTGDAL_NODE_CODE->GetRasterBand(1);
OUTBAND_NODE_CODE->SetNoDataValue(0);


// Read/write data
// Inputs
float agc_data[xsize];
float bgc_data[xsize];
float loss_data[xsize];
float peat_data[xsize];
float drivermodel_data[xsize];
float burn_data[xsize];
float ecozone_data[xsize];
float soil_data[xsize];
float climate_data[xsize];
float dead_data[xsize];
float litter_data[xsize];
float ifl_primary_data[xsize];
float plant_data[xsize];

// Outputs
float out_data_permanent_agriculture[xsize];        // 1
float out_data_hard_commodities[xsize];             // 2
float out_data_shifting_cultivation[xsize];         // 3
float out_data_forest_management[xsize];            // 4
float out_data_wildfire[xsize];                     // 5
float out_data_settlements[xsize];                  // 6
float out_data_other_disturbances[xsize];           // 7
float out_data_no_driver[xsize];                    // Null

float out_data_alldrivers_allgasses[xsize];
float out_data_alldrivers_CO2only[xsize];
float out_data_alldrivers_nonCO2[xsize];
float out_data_alldrivers_CH4only[xsize];
float out_data_alldrivers_N2Oonly[xsize];
short int out_data_node_code[xsize];

// Loop over the y coordinates, then the x coordinates
for (y=0; y<ysize; y++)
{

// The following RasterIO reads (and the RasterIO writes at the end) produced compile warnings about unused results
// (warning: ignoring return value of 'CPLErr GDALRasterBand::RasterIO(GDALRWFlag, int, int, int, int, void*, int, int, GDALDataType, GSpacing, GSpacing, GDALRasterIOExtraArg*)', declared with attribute warn_unused_result [-Wunused-result]).
// I asked how to handle or silence the warnings at https://stackoverflow.com/questions/72410931/how-to-handle-warn-unused-result-wunused-result/72410978#72410978.
// The code below handles the warnings by directing them to arguments, which are then checked.
// For cerr instead of std::err: https://www.geeksforgeeks.org/cerr-standard-error-stream-object-in-cpp/

// Error code returned by each line saved as their own argument
CPLErr errcodeIn1 = INBAND_AGC->RasterIO(GF_Read, 0, y, xsize, 1, agc_data, xsize, 1, GDT_Float32, 0, 0);
CPLErr errcodeIn2 = INBAND_BGC->RasterIO(GF_Read, 0, y, xsize, 1, bgc_data, xsize, 1, GDT_Float32, 0, 0);
CPLErr errcodeIn3 = INBAND_DRIVERMODEL->RasterIO(GF_Read, 0, y, xsize, 1, drivermodel_data, xsize, 1, GDT_Float32, 0, 0);
CPLErr errcodeIn4 = INBAND_LOSS->RasterIO(GF_Read, 0, y, xsize, 1, loss_data, xsize, 1, GDT_Float32, 0, 0);
CPLErr errcodeIn5 = INBAND_PEAT->RasterIO(GF_Read, 0, y, xsize, 1, peat_data, xsize, 1, GDT_Float32, 0, 0);
CPLErr errcodeIn6 = INBAND_BURN->RasterIO(GF_Read, 0, y, xsize, 1, burn_data, xsize, 1, GDT_Float32, 0, 0);
CPLErr errcodeIn7 = INBAND_IFL_PRIMARY->RasterIO(GF_Read, 0, y, xsize, 1, ifl_primary_data, xsize, 1, GDT_Float32, 0, 0);
CPLErr errcodeIn8 = INBAND_ECOZONE->RasterIO(GF_Read, 0, y, xsize, 1, ecozone_data, xsize, 1, GDT_Float32, 0, 0);
CPLErr errcodeIn9 = INBAND_CLIMATE->RasterIO(GF_Read, 0, y, xsize, 1, climate_data, xsize, 1, GDT_Float32, 0, 0);
CPLErr errcodeIn10 = INBAND_DEAD->RasterIO(GF_Read, 0, y, xsize, 1, dead_data, xsize, 1, GDT_Float32, 0, 0);
CPLErr errcodeIn11 = INBAND_LITTER->RasterIO(GF_Read, 0, y, xsize, 1, litter_data, xsize, 1, GDT_Float32, 0, 0);
CPLErr errcodeIn12 = INBAND_SOIL->RasterIO(GF_Read, 0, y, xsize, 1, soil_data, xsize, 1, GDT_Float32, 0, 0);
CPLErr errcodeIn13 = INBAND_PLANT->RasterIO(GF_Read, 0, y, xsize, 1, plant_data, xsize, 1, GDT_Float32, 0, 0);

// Number of input files
int inSize = 13;

// Array of error codes returned from each input
CPLErr errcodeInArray [inSize] = {errcodeIn1, errcodeIn2, errcodeIn3, errcodeIn4, errcodeIn5, errcodeIn6, errcodeIn7,
errcodeIn8, errcodeIn9, errcodeIn10, errcodeIn11, errcodeIn12, errcodeIn13};

// Iterates through the input error codes to make sure that the error code is acceptable
int j;

for (j=0; j<inSize; j++)
{
    if (errcodeInArray[j] != 0) {
        cerr << "rasterIO failed!\n";
        exit(1);
    }
}

for(x=0; x<xsize; x++)

    // Everything from here down analyzes one pixel at a time
	{

        // Initializes each output raster at 0 (nodata value)
		float outdata_permanent_agriculture_allgases = 0;   // permanent agriculture, all gases
		float outdata_permanent_agriculture_CO2only = 0;  // permanent agriculture, CO2 only
		float outdata_permanent_agriculture_nonCO2 = 0;  // permanent agriculture, non-CO2
		float outdata_permanent_agriculture_CH4only = 0;  // permanent agriculture, CH4 only
		float outdata_permanent_agriculture_N2Oonly = 0;  // permanent agriculture, N2O only

		float outdata_hard_commodities_allgases = 0;   // hard commodities, all gases
		float outdata_hard_commodities_CO2only = 0;  // hard commodities, CO2 only
		float outdata_hard_commodities_nonCO2 = 0;  // hard commodities, non-CO2
		float outdata_hard_commodities_CH4only = 0;  // hard commodities, CH4 only
		float outdata_hard_commodities_N2Oonly = 0;  // hard commodities, N2O only
		
		float outdata_shifting_cultivation_allgases = 0;   // shifting cultivation, all gases
		float outdata_shifting_cultivation_CO2only = 0;  // shifting cultivation, CO2 only
		float outdata_shifting_cultivation_nonCO2 = 0;  // shifting cultivation, non-CO2
		float outdata_shifting_cultivation_CH4only = 0;  // shifting cultivation, CH4 only
		float outdata_shifting_cultivation_N2Oonly = 0;  // shifting cultivation, N2O only
		
		float outdata_forest_management_allgases = 0;   // forest management, all gases
		float outdata_forest_management_CO2only = 0;  // forest management, CO2 only
		float outdata_forest_management_nonCO2 = 0;  // forest management, non-CO2
		float outdata_forest_management_CH4only = 0;  // forest management, CH4 only
		float outdata_forest_management_N2Oonly = 0;  // forest management, N2O only
		
		float outdata_wildfire_allgases = 0;   // wildfire, all gases
		float outdata_wildfire_CO2only = 0;  // wildfire, CO2 only
		float outdata_wildfire_nonCO2 = 0;  // wildfire, non-CO2
		float outdata_wildfire_CH4only = 0;  // wildfire, CH4 only
		float outdata_wildfire_N2Oonly = 0;  // wildfire, N2O only
		
		float outdata_settlements_allgases = 0;   // settlement and infrastructure, all gases
		float outdata_settlements_CO2only = 0;  // settlement and infrastructure, CO2 only
		float outdata_settlements_nonCO2 = 0;  // settlement and infrastructure, non-CO2
		float outdata_settlements_CH4only = 0;  // settlement and infrastructure, CH4 only
		float outdata_settlements_N2Oonly = 0;  // settlement and infrastructure, N2O only

		float outdata_other_disturbances_allgases = 0;   // other natural disturbances, all gases
		float outdata_other_disturbances_CO2only = 0;  // other natural disturbances, CO2 only
		float outdata_other_disturbances_nonCO2 = 0;  // other natural disturbances, non-CO2
		float outdata_other_disturbances_CH4only = 0;  // other natural disturbances, CH4 only
		float outdata_other_disturbances_N2Oonly = 0;  // other natural disturbances, N2O only

		float outdata_no_driver_allgases = 0;   // no driver, all gases
		float outdata_no_driver_CO2only = 0;  // no driver, CO2 only
		float outdata_no_driver_nonCO2 = 0;  // no driver, non-CO2
		float outdata_no_driver_CH4only = 0;  // no driver, CH4 only
		float outdata_no_driver_N2Oonly = 0;  // no driver, N2O only

		float outdata_alldrivers_allgases = 0;  // all drivers, all gases
		float outdata_alldrivers_CO2only = 0;  // all drivers, CO2 only
		float outdata_alldrivers_nonCO2 = 0;  // all drivers, non-CO2
		float outdata_alldrivers_CH4only = 0;  // all drivers, CH4 only
		float outdata_alldrivers_N2Oonly = 0;  // all drivers, N2O only
		
		short int outdata_node_code = 0;  // flowchart node

        // Only evaluates pixels that have loss and carbon. By definition, all pixels with carbon are in the model extent.
		if (loss_data[x] > 0 && agc_data[x] > 0)
        {

            // From equations.cpp, a function called def_variables, we get back several constants
            // based on several input rasters for that pixel. These are later used for calculating emissions.

            // def_variables kept returning the same values for all pixels in a tile as the first pixel in the tile regardless of the inputs to the function;
            // it was as if the returned values for the first pixel evaluated couldn't be overwritten.
            // The first answer here told me how to solve that: https://stackoverflow.com/questions/51609816/return-float-array-from-a-function-c
            float q[9];
            def_variables(&q[0], ecozone_data[x], drivermodel_data[x], ifl_primary_data[x], climate_data[x], plant_data[x], loss_data[x]);

			// The constants needed for calculating emissions
			float Cf = q[0];            // Combustion factor
			float Gef_CO2 = q[1];       // Emissions factor for CO2
			float Gef_CH4 = q[2];       // Emissions factor for CH4
			float Gef_N2O = q[3];       // Emissions factor for N2O
			float peatburn_CO2_only = q[4];      // Emissions from burning peat, CO2 emissions only
			float peatburn_CH4_only = q[5];       // Emissions from burning peat, CH4 emissions only (there are no N2O emissions from burning peat)
    		float peat_drain_total_CO2_only = q[6];      // Emissions from draining peat, CO2 emissions only
    		float peat_drain_total_CH4_only = q[7];      // Emissions from draining peat, CH4 emissions only
    		float peat_drain_total_N2O_only = q[8];      // Emissions from draining peat, N2O emissions only

            // Define and calculate several values used later
			float non_soil_c;
			non_soil_c = agc_data[x] + bgc_data[x] + dead_data[x] + litter_data[x];

			float non_soil_bgc_c;
			non_soil_bgc_c = agc_data[x] + dead_data[x] + litter_data[x]; //used for CH4 and N2O fire emissions for forestry, wildfire, other, and no driver

			float above_below_c;
			above_below_c = agc_data[x] + bgc_data[x];

			float Biomass_tCO2e_nofire_CO2_only;     // Emissions from biomass on pixels without fire- only emits CO2 (no non-CO2 option)
			float Biomass_tCO2e_yesfire_CO2_only;    // Emissions from biomass on pixels with fire- only the CO2
			float Biomass_tCO2e_yesfire_CH4_only;   // Emissions from biomass on pixels with fire- only CH4 emissions
			float Biomass_tCO2e_yesfire_N2O_only;   // Emissions from biomass on pixels with fire- only N2O emissions
			float minsoil;                           // Emissions from mineral soil- all CO2
			float flu;                               // Emissions fraction from mineral soil

		    // Each driver is an output raster and has its own emissions model. 
		    // outdata_node_code is the code for each combination of outputs (defined in carbon-budget/emissions/node_codes.txt)

			// Emissions model for permanent agriculture
			if (drivermodel_data[x] == 1)
			{
				// For each driver, these values (or a subset of them) are necessary for calculating emissions.
				Biomass_tCO2e_nofire_CO2_only = non_soil_c * C_to_CO2;
				Biomass_tCO2e_yesfire_CO2_only = non_soil_c * C_to_CO2;
				Biomass_tCO2e_yesfire_CH4_only = ((non_soil_c / biomass_to_c) * Cf * Gef_CH4 * pow(10,-3) * CH4_equiv);
				Biomass_tCO2e_yesfire_N2O_only = ((non_soil_c / biomass_to_c) * Cf * Gef_N2O * pow(10,-3) * N2O_equiv);
				flu = flu_val(climate_data[x], ecozone_data[x]);
				minsoil = ((soil_data[x]-(soil_data[x] * flu))/soil_emis_period) * (model_years-loss_data[x]);

				if (peat_data[x] > 0) // permanent ag, peat
				{
					if (burn_data[x] > 0) // permanent ag, peat, burned
					{
						outdata_permanent_agriculture_CO2only = Biomass_tCO2e_yesfire_CO2_only + peat_drain_total_CO2_only + peatburn_CO2_only;
						outdata_permanent_agriculture_CH4only = Biomass_tCO2e_yesfire_CH4_only + peat_drain_total_CH4_only + peatburn_CH4_only;
						outdata_permanent_agriculture_N2Oonly = Biomass_tCO2e_yesfire_N2O_only + peat_drain_total_N2O_only;
						outdata_node_code = 10;
					}
					if (burn_data[x] == 0) // permanent ag, peat, not burned
					{
						if (ecozone_data[x] == tropical) // permanent ag, peat, not burned, tropical
						{
						    if (plant_data[x] >= 1) // permanent ag, peat, not burned, tropical, plantation
						    {
						    	outdata_permanent_agriculture_CO2only = Biomass_tCO2e_nofire_CO2_only + peat_drain_total_CO2_only;
						        outdata_permanent_agriculture_CH4only = 0 + peat_drain_total_CH4_only;
						        outdata_permanent_agriculture_N2Oonly = 0 + peat_drain_total_N2O_only;
						        outdata_node_code = 11;
						    }
						    if (plant_data[x] == 0)     // permanent ag, peat, not burned, tropical, not plantation
						    {
						        outdata_permanent_agriculture_CO2only = Biomass_tCO2e_nofire_CO2_only;
						        outdata_permanent_agriculture_CH4only = 0;
                                outdata_permanent_agriculture_N2Oonly = 0;
						        outdata_node_code = 111;
						    }
						}
                        if ((ecozone_data[x] == boreal) || (ecozone_data[x] == temperate))      // permanent ag, peat, not burned, temperate/boreal
						{
						    outdata_permanent_agriculture_CO2only = Biomass_tCO2e_nofire_CO2_only + peat_drain_total_CO2_only;
						    outdata_permanent_agriculture_CH4only = 0 + peat_drain_total_CH4_only;
							outdata_permanent_agriculture_N2Oonly = 0 + peat_drain_total_N2O_only;
						    outdata_node_code = 12;
						}
					}
				}
				if (peat_data[x] == 0) // permanent ag, not peat
				{
					if (burn_data[x] > 0) // permanent ag, not peat, burned
					{
						if (ecozone_data[x] == tropical)   // permanent ag, not peat, burned, tropical
						{
                            if (ifl_primary_data[x] == 1)   // permanent ag, not peat, burned, tropical, IFL
                            {
                                if (plant_data[x] >= 1)     // permanent ag, not peat, burned, tropical, IFL, plantation
						        {
						            outdata_permanent_agriculture_CO2only = Biomass_tCO2e_yesfire_CO2_only;
						            outdata_permanent_agriculture_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							        outdata_permanent_agriculture_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						            outdata_node_code = 13;
						        }
						        if (plant_data[x] == 0)     // permanent ag, not peat, burned, tropical, IFL, not plantation
						        {
						            outdata_permanent_agriculture_CO2only = Biomass_tCO2e_yesfire_CO2_only + minsoil;
						            outdata_permanent_agriculture_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							        outdata_permanent_agriculture_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						            outdata_node_code = 131;
						        }
						    }
						    if (ifl_primary_data[x] == 0)   // permanent ag, not peat, burned, tropical, not IFL
						    {
                                if (plant_data[x] >= 1)     // permanent ag, not peat, burned, tropical, not IFL, plantation
						        {
						            outdata_permanent_agriculture_CO2only = Biomass_tCO2e_yesfire_CO2_only;
						            outdata_permanent_agriculture_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							        outdata_permanent_agriculture_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						            outdata_node_code = 14;
 						        }
						        if (plant_data[x] == 0)     // permanent ag, not peat, burned, tropical, not IFL, not plantation
						        {
						            outdata_permanent_agriculture_CO2only = Biomass_tCO2e_yesfire_CO2_only + minsoil;
						            outdata_permanent_agriculture_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							        outdata_permanent_agriculture_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						            outdata_node_code = 141;
						        }
                            }
						}
						if (ecozone_data[x] == boreal)   // permanent ag, not peat, burned, boreal
						{
                            outdata_permanent_agriculture_CO2only = Biomass_tCO2e_yesfire_CO2_only + minsoil;
                            outdata_permanent_agriculture_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							outdata_permanent_agriculture_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						    outdata_node_code = 15;
						}
						if (ecozone_data[x] == temperate)   // permanent ag, not peat, burned, temperate
						{
						    if (plant_data[x] >= 1)     // permanent ag, not peat, burned, temperate, plantation
						    {
						        outdata_permanent_agriculture_CO2only = Biomass_tCO2e_yesfire_CO2_only;
						        outdata_permanent_agriculture_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							    outdata_permanent_agriculture_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						        outdata_node_code = 16;
						    }
						    if (plant_data[x] == 0)     // permanent ag, not peat, burned, temperate, not plantation
						    {
						        outdata_permanent_agriculture_CO2only = Biomass_tCO2e_yesfire_CO2_only + minsoil;
						        outdata_permanent_agriculture_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							    outdata_permanent_agriculture_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						        outdata_node_code = 161;
						    }
						}
					}
					if (burn_data[x] == 0) // permanent ag, not peat, not burned
					{
						if (ecozone_data[x] == tropical)   // permanent ag, not peat, not burned, tropical
						{
						    if (plant_data[x] >= 1)     // permanent ag, not peat, not burned, tropical, plantation
						    {
						        outdata_permanent_agriculture_CO2only = Biomass_tCO2e_nofire_CO2_only;
						        outdata_permanent_agriculture_CH4only = 0;
							    outdata_permanent_agriculture_N2Oonly = 0;
						        outdata_node_code = 17;
						    }
						    if (plant_data[x] == 0)     // permanent ag, not peat, not burned, tropical, not plantation
						    {
						        outdata_permanent_agriculture_CO2only = Biomass_tCO2e_nofire_CO2_only + minsoil;
						        outdata_permanent_agriculture_CH4only = 0;
							    outdata_permanent_agriculture_N2Oonly = 0;
						        outdata_node_code = 171;
						    }
						}
						if (ecozone_data[x] == boreal)   // permanent ag, not peat, not burned, boreal
						{
                            outdata_permanent_agriculture_CO2only = Biomass_tCO2e_nofire_CO2_only + minsoil;
                            outdata_permanent_agriculture_CH4only = 0;
							outdata_permanent_agriculture_N2Oonly = 0;
                            outdata_node_code = 18;
						}
						if (ecozone_data[x] == temperate)   // permanent ag, not peat, not burned, temperate
						{
						    if (plant_data[x] >= 1)     // permanent ag, not peat, not burned, temperate, plantation
						    {
						        outdata_permanent_agriculture_CO2only = Biomass_tCO2e_nofire_CO2_only;
						        outdata_permanent_agriculture_CH4only = 0;
							    outdata_permanent_agriculture_N2Oonly = 0;
						        outdata_node_code = 19;
						    }
						    if (plant_data[x] == 0)     // permanent ag, not peat, not burned, temperate, not plantation
						    {
						        outdata_permanent_agriculture_CO2only = Biomass_tCO2e_nofire_CO2_only + minsoil;
						        outdata_permanent_agriculture_CH4only = 0;
							    outdata_permanent_agriculture_N2Oonly = 0;
						        outdata_node_code = 191;

						        ////QC code to get the values of the relevant variables at a particular pixel of interest (based on its values rather than its coordinates)
                                //double total;
                                //total = Biomass_tCO2e_yesfire_CO2_only + peat_drain_total_CO2_only + peatburn_CO2_only + Biomass_tCO2e_yesfire_CH4_only + Biomass_tCO2e_yesfire_N2O_only + peat_drain_total_CH4_only + peat_drain_total_N2O_only + peatburn_CH4_only;
                                //if ((total < 715) && (total > 714) && (agc_data[x] = 26.25) && (soil_data[x] = 216) && (dead_data[x] = 1.44) && (litter_data[x] = 0.5328) && (burn_data[x] = 6))
                                //{
                                //    cout << "total: " << total << endl;
                                //    cout << "Biomass_tCO2e_yesfire_CO2_only: " << Biomass_tCO2e_yesfire_CO2_only << endl;
                                //    cout << "Biomass_tCO2e_yesfire_CH4_only: " << Biomass_tCO2e_yesfire_CH4_only << endl;
                                //    cout << "Biomass_tCO2e_yesfire_N2O_only: " << Biomass_tCO2e_yesfire_N2O_only << endl;
                                //    cout << "peat_drain_total_CO2_only: " << peat_drain_total_CO2_only << endl;
                                //    cout << "peat_drain_total_CH4_only: " << peat_drain_total_CH4_only << endl;
                                //    cout << "peat_drain_total_N2O_only: " << peat_drain_total_N2O_only << endl;
                                //    cout << "peatburn_CO2_only: " << peatburn_CO2_only << endl;
                                //    cout << "peatburn_CH4_only: " << peatburn_CH4_only << endl;
                                //    cout << "agc_data[x]: " << agc_data[x] << endl;
                                //    cout << "Cf: " << Cf << endl;
                                //    cout << "Gef_CO2: " << Gef_CO2 << endl;
                                //    cout << "Gef_CH4: " << Gef_CH4 << endl;
                                //    cout << "Gef_N2O: " << Gef_N2O << endl;
                                //    cout << "" << endl;
                                //}
						    }
						}
					}
				}
				outdata_permanent_agriculture_allgases = outdata_permanent_agriculture_CO2only + outdata_permanent_agriculture_CH4only + outdata_permanent_agriculture_N2Oonly;
				outdata_permanent_agriculture_nonCO2 = outdata_permanent_agriculture_CH4only + outdata_permanent_agriculture_N2Oonly;
			}

			// Emissions model for hard commodities
			else if (drivermodel_data[x] == 2)
			{
				// For each driver, these values (or a subset of them) are necessary for calculating emissions.
				Biomass_tCO2e_nofire_CO2_only = non_soil_c * C_to_CO2;
				Biomass_tCO2e_yesfire_CO2_only = (non_soil_c * C_to_CO2);
				Biomass_tCO2e_yesfire_CH4_only = ((non_soil_c / biomass_to_c) * Cf * Gef_CH4 * pow(10,-3) * CH4_equiv);
				Biomass_tCO2e_yesfire_N2O_only = ((non_soil_c / biomass_to_c) * Cf * Gef_N2O * pow(10,-3) * N2O_equiv);
				minsoil = ((soil_data[x]-(soil_data[x] * hard_commod_flu))/soil_emis_period) * (model_years-loss_data[x]);

				if (peat_data[x] > 0) // hard commodities, peat
				{
					if (burn_data[x] > 0) // hard commodities, peat, burned
					{
						outdata_hard_commodities_CO2only = Biomass_tCO2e_yesfire_CO2_only + peat_drain_total_CO2_only + peatburn_CO2_only;
						outdata_hard_commodities_CH4only = Biomass_tCO2e_yesfire_CH4_only + peat_drain_total_CH4_only + peatburn_CH4_only;
						outdata_hard_commodities_N2Oonly = Biomass_tCO2e_yesfire_N2O_only + peat_drain_total_N2O_only;
						outdata_node_code = 20;
					}
					if (burn_data[x] == 0) // hard commodities, peat, not burned
					{
						if (ecozone_data[x] == tropical) // hard commodities, peat, not burned, tropical
						{
						    if (plant_data[x] >= 1) // hard commodities, peat, not burned, tropical, plantation
						    {
						    	outdata_hard_commodities_CO2only = Biomass_tCO2e_nofire_CO2_only + peat_drain_total_CO2_only;
						        outdata_hard_commodities_CH4only = 0 + peat_drain_total_CH4_only;
						        outdata_hard_commodities_N2Oonly = 0 + peat_drain_total_N2O_only;
						        outdata_node_code = 21;
						    }
						    if (plant_data[x] == 0)     // hard commodities, peat, not burned, tropical, not plantation
						    {
						        outdata_hard_commodities_CO2only = Biomass_tCO2e_nofire_CO2_only;
						        outdata_hard_commodities_CH4only = 0;
                                outdata_hard_commodities_N2Oonly = 0;
						        outdata_node_code = 211;
						    }
						}
                        if ((ecozone_data[x] == boreal) || (ecozone_data[x] == temperate))      // hard commodities, peat, not burned, temperate/boreal
						{
						    outdata_hard_commodities_CO2only = Biomass_tCO2e_nofire_CO2_only + peat_drain_total_CO2_only;
						    outdata_hard_commodities_CH4only = 0 + peat_drain_total_CH4_only;
							outdata_hard_commodities_N2Oonly = 0 + peat_drain_total_N2O_only;
						    outdata_node_code = 22;
						}
					}
				}
				if (peat_data[x] == 0) // hard commodities, not peat
				{
					if (burn_data[x] > 0) // hard commodities, not peat, burned
					{
						if (ecozone_data[x] == tropical)   // hard commodities, not peat, burned, tropical
						{
                            if (ifl_primary_data[x] == 1)   // hard commodities, not peat, burned, tropical, IFL
                            {
                                if (plant_data[x] >= 1)     // hard commodities, not peat, burned, tropical, IFL, plantation
						        {
						            outdata_hard_commodities_CO2only = Biomass_tCO2e_yesfire_CO2_only;
						            outdata_hard_commodities_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							        outdata_hard_commodities_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						            outdata_node_code = 23;
						        }
						        if (plant_data[x] == 0)     // hard commodities, not peat, burned, tropical, IFL, not plantation
						        {
						            outdata_hard_commodities_CO2only = Biomass_tCO2e_yesfire_CO2_only + minsoil;
						            outdata_hard_commodities_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							        outdata_hard_commodities_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						            outdata_node_code = 231;
						        }
						    }
						    if (ifl_primary_data[x] == 0)   // hard commodities, not peat, burned, tropical, not IFL
						    {
                                if (plant_data[x] >= 1)     // hard commodities, not peat, burned, tropical, not IFL, plantation
						        {
						            outdata_hard_commodities_CO2only = Biomass_tCO2e_yesfire_CO2_only;
						            outdata_hard_commodities_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							        outdata_hard_commodities_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						            outdata_node_code = 24;
 						        }
						        if (plant_data[x] == 0)     // hard commodities, not peat, burned, tropical, not IFL, not plantation
						        {
						            outdata_hard_commodities_CO2only = Biomass_tCO2e_yesfire_CO2_only + minsoil;
						            outdata_hard_commodities_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							        outdata_hard_commodities_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						            outdata_node_code = 241;
						        }
                            }
						}
						if (ecozone_data[x] == boreal)   // hard commodities, not peat, burned, boreal
						{
                            outdata_hard_commodities_CO2only = Biomass_tCO2e_yesfire_CO2_only + minsoil;
                            outdata_hard_commodities_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							outdata_hard_commodities_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						    outdata_node_code = 25;
						}
						if (ecozone_data[x] == temperate)   // hard commodities, not peat, burned, temperate
						{
						    if (plant_data[x] >= 1)     // hard commodities, not peat, burned, temperate, plantation
						    {
						        outdata_hard_commodities_CO2only = Biomass_tCO2e_yesfire_CO2_only;
						        outdata_hard_commodities_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							    outdata_hard_commodities_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						        outdata_node_code = 26;
						    }
						    if (plant_data[x] == 0)     // hard commodities, not peat, burned, temperate, not plantation
						    {
						        outdata_hard_commodities_CO2only = Biomass_tCO2e_yesfire_CO2_only + minsoil;
						        outdata_hard_commodities_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							    outdata_hard_commodities_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						        outdata_node_code = 261;
						    }
						}
					}
					if (burn_data[x] == 0) // hard commodities, not peat, not burned
					{
						if (ecozone_data[x] == tropical)   // hard commodities, not peat, not burned, tropical
						{
						    if (plant_data[x] >= 1)     // hard commodities, not peat, not burned, tropical, plantation
						    {
						        outdata_hard_commodities_CO2only = Biomass_tCO2e_nofire_CO2_only;
						        outdata_hard_commodities_CH4only = 0;
							    outdata_hard_commodities_N2Oonly = 0;
						        outdata_node_code = 27;
						    }
						    if (plant_data[x] == 0)     // hard commodities, not peat, not burned, tropical, not plantation
						    {
						        outdata_hard_commodities_CO2only = Biomass_tCO2e_nofire_CO2_only + minsoil;
						        outdata_hard_commodities_CH4only = 0;
							    outdata_hard_commodities_N2Oonly = 0;
						        outdata_node_code = 271;
						    }
						}
						if (ecozone_data[x] == boreal)   // hard commodities, not peat, not burned, boreal
						{
                            outdata_hard_commodities_CO2only = Biomass_tCO2e_nofire_CO2_only + minsoil;
                            outdata_hard_commodities_CH4only = 0;
							outdata_hard_commodities_N2Oonly = 0;
                            outdata_node_code = 28;
						}
						if (ecozone_data[x] == temperate)   // hard commodities, not peat, not burned, temperate
						{
						    if (plant_data[x] >= 1)     // hard commodities, not peat, not burned, temperate, plantation
						    {
						        outdata_hard_commodities_CO2only = Biomass_tCO2e_nofire_CO2_only;
						        outdata_hard_commodities_CH4only = 0;
							    outdata_hard_commodities_N2Oonly = 0;
						        outdata_node_code = 29;
						    }
						    if (plant_data[x] == 0)     // hard commodities, not peat, not burned, temperate, not plantation
						    {
						        outdata_hard_commodities_CO2only = Biomass_tCO2e_nofire_CO2_only + minsoil;
						        outdata_hard_commodities_CH4only = 0;
							    outdata_hard_commodities_N2Oonly = 0;
						        outdata_node_code = 291;
						    }
						}
					}
				}
				outdata_hard_commodities_allgases = outdata_hard_commodities_CO2only + outdata_hard_commodities_CH4only + outdata_hard_commodities_N2Oonly;
				outdata_hard_commodities_nonCO2 = outdata_hard_commodities_CH4only + outdata_hard_commodities_N2Oonly;
			}

			// Emissions model for shifting cultivation (only difference is flu val)
			else if (drivermodel_data[x] == 3)
			{
				Biomass_tCO2e_nofire_CO2_only = non_soil_c * C_to_CO2;
				Biomass_tCO2e_yesfire_CO2_only = (non_soil_c * C_to_CO2);
				Biomass_tCO2e_yesfire_CH4_only = ((non_soil_c / biomass_to_c) * Cf * Gef_CH4 * pow(10,-3) * CH4_equiv);
				Biomass_tCO2e_yesfire_N2O_only = ((non_soil_c / biomass_to_c) * Cf * Gef_N2O * pow(10,-3) * N2O_equiv);
				minsoil = ((soil_data[x]-(soil_data[x] * shift_cult_flu))/soil_emis_period) * (model_years-loss_data[x]);

				if (peat_data[x] > 0) // shifting cultivation, peat
				{
					if (burn_data[x] > 0) // shifting cultivation, peat, burned
					{
						if ((ecozone_data[x] == boreal) || (ecozone_data[x] == temperate))      // shifting cultivation, peat, burned, temperate/boreal
						{
						    outdata_shifting_cultivation_CO2only = Biomass_tCO2e_yesfire_CO2_only + peatburn_CO2_only;
						    outdata_shifting_cultivation_CH4only = Biomass_tCO2e_yesfire_CH4_only + peatburn_CH4_only;
							outdata_shifting_cultivation_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						    outdata_node_code = 30;
						}
						if (ecozone_data[x] == tropical)      // shifting cultivation, peat, burned, tropical
						{
						    outdata_shifting_cultivation_CO2only = Biomass_tCO2e_yesfire_CO2_only + peat_drain_total_CO2_only + peatburn_CO2_only;
						    outdata_shifting_cultivation_CH4only = Biomass_tCO2e_yesfire_CH4_only + peat_drain_total_CH4_only + peatburn_CH4_only;
							outdata_shifting_cultivation_N2Oonly = Biomass_tCO2e_yesfire_N2O_only + peat_drain_total_N2O_only;
						    outdata_node_code = 31;
						}
					}
					if (burn_data[x] == 0)// shifting cultivation, peat, not burned
					{
						if ((ecozone_data[x] == boreal) || (ecozone_data[x] == temperate))      // shifting cultivation, peat, not burned, temperate/boreal
						{
						    outdata_shifting_cultivation_CO2only = Biomass_tCO2e_nofire_CO2_only;
						    outdata_shifting_cultivation_CH4only = 0;
							outdata_shifting_cultivation_N2Oonly = 0;
						    outdata_node_code = 32;
						}
						if (ecozone_data[x] == tropical)      // shifting cultivation, peat, not burned, tropical
						{
						    if (plant_data[x] >= 1)     // shifting cultivation, peat, not burned, tropical, plantation
						    {
						        outdata_shifting_cultivation_CO2only = Biomass_tCO2e_nofire_CO2_only + peat_drain_total_CO2_only;
						        outdata_shifting_cultivation_CH4only = 0 + peat_drain_total_CH4_only;
							    outdata_shifting_cultivation_N2Oonly = 0 + peat_drain_total_N2O_only;
						        outdata_node_code = 33;
						    }
						    if (plant_data[x] == 0)     // shifting cultivation, peat, not burned, tropical, not plantation
						    {
						        outdata_shifting_cultivation_CO2only = Biomass_tCO2e_nofire_CO2_only;
						        outdata_shifting_cultivation_CH4only = 0;
							    outdata_shifting_cultivation_N2Oonly = 0;
						        outdata_node_code = 331;
						    }
						}
					}
				}
				if (peat_data[x] == 0)// shifting cultivation, not peat
				{
					if (burn_data[x] > 0) // shifting cultivation, not peat, burned
					{
						if (ecozone_data[x] == tropical)   // shifting cultivation, not peat, burned, tropical
						{
                            if (ifl_primary_data[x] == 1)   // shifting cultivation, not peat, burned, tropical, IFL
                            {
                                if (plant_data[x] >= 1)     // shifting cultivation, not peat, burned, tropical, IFL, plantation
						        {
						            outdata_shifting_cultivation_CO2only = Biomass_tCO2e_yesfire_CO2_only;
						            outdata_shifting_cultivation_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							        outdata_shifting_cultivation_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						            outdata_node_code = 34;
						        }
						        if (plant_data[x] == 0)     // shifting cultivation, not peat, burned, tropical, IFL, not plantation
						        {
						            outdata_shifting_cultivation_CO2only = Biomass_tCO2e_yesfire_CO2_only + minsoil;
						            outdata_shifting_cultivation_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							        outdata_shifting_cultivation_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						            outdata_node_code = 341;
						        }
						    }
						    if (ifl_primary_data[x] == 0)   // shifting cultivation, not peat, burned, tropical, not IFL
						    {
                                if (plant_data[x] >= 1)     // shifting cultivation, not peat, burned, tropical, not IFL, plantation
						        {
						            outdata_shifting_cultivation_CO2only = Biomass_tCO2e_yesfire_CO2_only;
						            outdata_shifting_cultivation_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							        outdata_shifting_cultivation_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						            outdata_node_code = 35;
						        }
						        if (plant_data[x] == 0)     // shifting cultivation, not peat, burned, tropical, not IFL, not plantation
						        {
						            outdata_shifting_cultivation_CO2only = Biomass_tCO2e_yesfire_CO2_only + minsoil;
						            outdata_shifting_cultivation_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							        outdata_shifting_cultivation_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						            outdata_node_code = 351;
						        }
                            }
						}
						if (ecozone_data[x] == boreal)   // shifting cultivation, not peat, burned, boreal
						{
                            outdata_shifting_cultivation_CO2only = Biomass_tCO2e_yesfire_CO2_only + minsoil;
                            outdata_shifting_cultivation_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							outdata_shifting_cultivation_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						    outdata_node_code = 36;
						}
						if (ecozone_data[x] == temperate)   // shifting cultivation, not peat, burned, temperate
						{
						    if (plant_data[x] >= 1)     // shifting cultivation, not peat, burned, temperate, plantation
						    {
						        outdata_shifting_cultivation_CO2only = Biomass_tCO2e_yesfire_CO2_only;
						        outdata_shifting_cultivation_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							    outdata_shifting_cultivation_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						        outdata_node_code = 37;
						    }
						    if (plant_data[x] == 0)     // shifting cultivation, not peat, burned, temperate, not plantation
						    {
						        outdata_shifting_cultivation_CO2only = Biomass_tCO2e_yesfire_CO2_only + minsoil;
						        outdata_shifting_cultivation_CH4only = Biomass_tCO2e_yesfire_CH4_only;
							    outdata_shifting_cultivation_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						        outdata_node_code = 371;
						    }
						}
					}
					if (burn_data[x] == 0) // shifting cultivation, not peat, not burned
					{
						if (ecozone_data[x] == tropical)   // shifting cultivation, not peat, not burned, tropical
						{
						    if (plant_data[x] >= 1)     // shifting cultivation, not peat, not burned, tropical, plantation
						    {
						        outdata_shifting_cultivation_CO2only = Biomass_tCO2e_nofire_CO2_only;
						        outdata_shifting_cultivation_CH4only = 0;
							    outdata_shifting_cultivation_N2Oonly = 0;
						        outdata_node_code = 38;
						    }
						    if (plant_data[x] == 0)     // shifting cultivation, not peat, not burned, tropical, not plantation
						    {
						        outdata_shifting_cultivation_CO2only = Biomass_tCO2e_nofire_CO2_only + minsoil;
						        outdata_shifting_cultivation_CH4only = 0;
							    outdata_shifting_cultivation_N2Oonly = 0;
						        outdata_node_code = 381;

//                                //QC code to get the values of the relevant variables at a particular pixel of interest (based on its values rather than its coordinates)
//                                double total;
//                                total = Biomass_tCO2e_nofire_CO2_only + minsoil;
//                                double minsoil_pt1;
//                                minsoil_pt1 = ((soil_data[x]-(soil_data[x] * shift_cult_flu))/soil_emis_period);
//                                double minsoil_pt2;
//                                minsoil_pt2 = (model_years-loss_data[x]);
////                              if ((total < 781) && (total > 780) && (agc_data[x] < 155) && (agc_data[x] > 154) && (loss_data[x] = 3) && (soil_data[x] = 135) && (drivermodel_data[x] == 3))
////                                if ((x > 3525) && (x < 3530) && (total < 781) && (total > 780) && (agc_data[x] = 154.354538) && (loss_data[x] = 3) && (soil_data[x] = 135) && (drivermodel_data[x] == 3))
//                                if ((x > 3526) && (x < 3528) && (y > 1555) && (y < 1559) && (drivermodel_data[x] == 3) && (loss_data[x] = 3))
//                                {
//                                    cout << "x: " << x << endl;
//                                    cout << "y: " << y << endl;
//                                    cout << "agc_data: " << agc_data[x] << endl;
//                                    cout << "bgc_data: " << bgc_data[x] << endl;
//                                    cout << "deadwood_data: " << dead_data[x] << endl;
//                                    cout << "litter_data: " << litter_data[x] << endl;
//                                    cout << "non_soil_C: " << non_soil_c << endl;
//                                    cout << "C_to_CO2: " << C_to_CO2 << endl;
//                                    cout << "Biomass_tCO2e_nofire_CO2_only: " << Biomass_tCO2e_nofire_CO2_only << endl;
//                                    cout << "soil_data: " << soil_data[x] << endl;
//                                    cout << "shift_cult_flu: " << shift_cult_flu << endl;
//                                    cout << "minsoil_first_half: " << minsoil_pt1 << endl;
//                                    cout << "loss_year: " << loss_data[x] << endl;
//                                    cout << "minsoil_second_half: " << (model_years-loss_data[x]) << endl;
//                                    cout << "minsoil: " << minsoil << endl;
//                                    cout << "total: " << total << endl;
//                                    cout << endl;
//                                }
						    }
						}
						if (ecozone_data[x] == boreal)   // shifting cultivation, not peat, not burned, boreal
						{
                            outdata_shifting_cultivation_CO2only = Biomass_tCO2e_nofire_CO2_only + minsoil;
                            outdata_shifting_cultivation_CH4only = 0;
						    outdata_shifting_cultivation_N2Oonly = 0;
                            outdata_node_code = 39;
						}
						if (ecozone_data[x] == temperate)   // shifting cultivation, not peat, not burned, temperate
						{
						    if (plant_data[x] >= 1)     // shifting cultivation, not peat, not burned, temperate, plantation
						    {
						        outdata_shifting_cultivation_CO2only = Biomass_tCO2e_nofire_CO2_only;
						        outdata_shifting_cultivation_CH4only = 0;
						        outdata_shifting_cultivation_N2Oonly = 0;
						        outdata_node_code = 391;
						    }
						    if (plant_data[x] == 0)     // shifting cultivation, not peat, not burned, temperate, not plantation
						    {
						        outdata_shifting_cultivation_CO2only = Biomass_tCO2e_nofire_CO2_only + minsoil;
						        outdata_shifting_cultivation_CH4only = 0;
						        outdata_shifting_cultivation_N2Oonly = 0;
						        outdata_node_code = 392;
						    }
						}
					}
				}
			    outdata_shifting_cultivation_allgases = outdata_shifting_cultivation_CO2only + outdata_shifting_cultivation_CH4only + outdata_shifting_cultivation_N2Oonly;
			    outdata_shifting_cultivation_nonCO2 = outdata_shifting_cultivation_CH4only + outdata_shifting_cultivation_N2Oonly;
			}

			// Emissions model for forest management
			else if (drivermodel_data[x] == 4)
			{
				Biomass_tCO2e_nofire_CO2_only = above_below_c * C_to_CO2;
				Biomass_tCO2e_yesfire_CO2_only = ((agc_data[x] / biomass_to_c) * Cf * Gef_CO2 * pow(10, -3));
                Biomass_tCO2e_yesfire_CH4_only = ((non_soil_bgc_c / biomass_to_c) * Cf * Gef_CH4 * pow(10, -3) * CH4_equiv);
				Biomass_tCO2e_yesfire_N2O_only = ((non_soil_bgc_c / biomass_to_c) * Cf * Gef_N2O * pow(10, -3) * N2O_equiv);

				if (peat_data[x] > 0) // forest management, peat
				{
					if (burn_data[x] > 0 ) // forest management, peat, burned
					{
						outdata_forest_management_CO2only = Biomass_tCO2e_yesfire_CO2_only + peat_drain_total_CO2_only + peatburn_CO2_only;
						outdata_forest_management_CH4only = Biomass_tCO2e_yesfire_CH4_only + peat_drain_total_CH4_only + peatburn_CH4_only;
						outdata_forest_management_N2Oonly = Biomass_tCO2e_yesfire_N2O_only + peat_drain_total_N2O_only;
						outdata_node_code = 40;
					}
					if (burn_data[x] == 0 )  // forest management, peat, not burned
					{
						if ((ecozone_data[x] == boreal) || (ecozone_data[x] == temperate))  // forest management, peat, not burned, temperate/boreal
						{
							outdata_forest_management_CO2only = Biomass_tCO2e_nofire_CO2_only;
							outdata_forest_management_CH4only = 0;
						    outdata_forest_management_N2Oonly = 0;
							outdata_node_code = 41;
						}
						if (ecozone_data[x] == tropical)// forest management, peat, not burned, tropical
						{
							if (plant_data[x] > 0)  // forest management, peat, not burned, tropical, plantation
							{
								outdata_forest_management_CO2only = Biomass_tCO2e_nofire_CO2_only + peat_drain_total_CO2_only;
								outdata_forest_management_CH4only = 0 + peat_drain_total_CH4_only;
								outdata_forest_management_N2Oonly = 0 + peat_drain_total_N2O_only;
								outdata_node_code = 42;
							}
							if (plant_data[x] == 0)  // forest management, peat, not burned, tropical, not plantation
							{
								outdata_forest_management_CO2only = Biomass_tCO2e_nofire_CO2_only;
								outdata_forest_management_CH4only = 0;
								outdata_forest_management_N2Oonly = 0;
								outdata_node_code = 421;
							}
						}
					}
				}
				else  // forest management, not peat
				{
					if (burn_data[x] > 0) // forest management, not peat, burned
					{
						outdata_forest_management_CO2only = Biomass_tCO2e_yesfire_CO2_only;
						outdata_forest_management_CH4only = Biomass_tCO2e_yesfire_CH4_only;
						outdata_forest_management_N2Oonly= Biomass_tCO2e_yesfire_N2O_only;
						outdata_node_code = 43;
					}
					if (burn_data[x] == 0) // forest management, not peat, not burned
					{
						outdata_forest_management_CO2only = Biomass_tCO2e_nofire_CO2_only;
						outdata_forest_management_CH4only = 0;
						outdata_forest_management_N2Oonly = 0;
						outdata_node_code = 44;
					}
				}
				outdata_forest_management_allgases = outdata_forest_management_CO2only + outdata_forest_management_CH4only + outdata_forest_management_N2Oonly;
				outdata_forest_management_nonCO2 = outdata_forest_management_CH4only + outdata_forest_management_N2Oonly;
			}

		    // Emissions model for wildfires
		    else if (drivermodel_data[x] == 5)
			{
				Biomass_tCO2e_nofire_CO2_only = above_below_c * C_to_CO2;
				Biomass_tCO2e_yesfire_CO2_only = ((agc_data[x] / biomass_to_c) * Cf * Gef_CO2 * pow(10, -3));
                Biomass_tCO2e_yesfire_CH4_only = ((non_soil_bgc_c / biomass_to_c) * Cf * Gef_CH4 * pow(10, -3) * CH4_equiv);
				Biomass_tCO2e_yesfire_N2O_only = ((non_soil_bgc_c / biomass_to_c) * Cf * Gef_N2O * pow(10, -3) * N2O_equiv);

				if (peat_data[x] > 0) // wildfire, peat
				{
					if (burn_data[x] > 0) // wildfire, peat, burned
					{
						outdata_wildfire_CO2only = Biomass_tCO2e_yesfire_CO2_only + peat_drain_total_CO2_only + peatburn_CO2_only;
						outdata_wildfire_CH4only = Biomass_tCO2e_yesfire_CH4_only + peat_drain_total_CH4_only + peatburn_CH4_only;
						outdata_wildfire_N2Oonly = Biomass_tCO2e_yesfire_N2O_only + peat_drain_total_N2O_only;
						outdata_node_code = 50;
					}
					if (burn_data[x] == 0) // wildfire, peat, not burned
					{
						if ((ecozone_data[x] == boreal) || (ecozone_data[x] == temperate)) // wildfire, peat, not burned, temperate/boreal
						{
							outdata_wildfire_CO2only = Biomass_tCO2e_nofire_CO2_only;
							outdata_wildfire_CH4only = 0;
						    outdata_wildfire_N2Oonly = 0;
							outdata_node_code = 51;
						}
						if (ecozone_data[x] == tropical) // wildfire, peat, not burned, tropical
						{
					        if (plant_data[x] > 0)  // wildfire, peat, not burned, tropical, plantation
							{
								outdata_wildfire_CO2only = Biomass_tCO2e_nofire_CO2_only + peat_drain_total_CO2_only;
								outdata_wildfire_CH4only = 0 + peat_drain_total_CH4_only;
						        outdata_wildfire_N2Oonly = 0 + peat_drain_total_N2O_only;
								outdata_node_code = 52;
							}
							if (plant_data[x] == 0)  // wildfire, peat, not burned, tropical, not plantation
							{
								outdata_wildfire_CO2only = Biomass_tCO2e_nofire_CO2_only;
								outdata_wildfire_CH4only = 0;
						        outdata_wildfire_N2Oonly = 0;
								outdata_node_code = 521;
							}
						}
					}
				}
				else  // wildfire, not peat
				{
					if (burn_data[x] > 0)  // wildfire, not peat, burned
					{
						outdata_wildfire_CO2only = Biomass_tCO2e_yesfire_CO2_only;
						outdata_wildfire_CH4only = Biomass_tCO2e_yesfire_CH4_only;
						outdata_wildfire_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						outdata_node_code = 53;
					}
					else  // wildfire, not peat, not burned
					{
						outdata_wildfire_CO2only = Biomass_tCO2e_nofire_CO2_only;
						outdata_wildfire_CH4only = 0;
						outdata_wildfire_N2Oonly = 0;
						outdata_node_code = 54;
					}
				}
				outdata_wildfire_allgases = outdata_wildfire_CO2only + outdata_wildfire_CH4only + outdata_wildfire_N2Oonly;
				outdata_wildfire_nonCO2 = outdata_wildfire_CH4only + outdata_wildfire_N2Oonly;
			}

		    // Emissions model for settlements & infrastructure
		    else if (drivermodel_data[x] == 6)
			{
				Biomass_tCO2e_nofire_CO2_only = non_soil_c * C_to_CO2;
				Biomass_tCO2e_yesfire_CO2_only = (non_soil_c * C_to_CO2);
				Biomass_tCO2e_yesfire_CH4_only = ((non_soil_c / biomass_to_c) * Cf * Gef_CH4 * pow(10,-3) * CH4_equiv);
				Biomass_tCO2e_yesfire_N2O_only = ((non_soil_c / biomass_to_c) * Cf * Gef_N2O * pow(10,-3) * N2O_equiv);
				minsoil = ((soil_data[x]-(soil_data[x] * settlements_flu))/soil_emis_period) * (model_years-loss_data[x]);

                if (peat_data[x] > 0) // settlements & infrastructure, peat
				{
					if (burn_data[x] > 0) // settlements & infrastructure, peat, burned
					{
						outdata_settlements_CO2only = Biomass_tCO2e_yesfire_CO2_only + peat_drain_total_CO2_only + peatburn_CO2_only;
						outdata_settlements_CH4only = Biomass_tCO2e_yesfire_CH4_only + peat_drain_total_CH4_only + peatburn_CH4_only;
						outdata_settlements_N2Oonly = Biomass_tCO2e_yesfire_N2O_only + peat_drain_total_N2O_only;
						outdata_node_code = 60;
					}
					if (burn_data[x] == 0) // settlements & infrastructure, peat, not burned
					{
						if (ecozone_data[x] == tropical) // settlements & infrastructure, peat, not burned, tropical
						{
						    if (plant_data[x] >= 1) // settlements & infrastructure, peat, not burned, tropical, plantation
						    {
						    	outdata_settlements_CO2only = Biomass_tCO2e_nofire_CO2_only + peat_drain_total_CO2_only;
						        outdata_settlements_CH4only = 0 + peat_drain_total_CH4_only;
						        outdata_settlements_N2Oonly = 0 + peat_drain_total_N2O_only;
						        outdata_node_code = 61;
						    }
						    if (plant_data[x] == 0)     // settlements & infrastructure, peat, not burned, tropical, not plantation
						    {
						        outdata_settlements_CO2only = Biomass_tCO2e_nofire_CO2_only;
						        outdata_settlements_CH4only = 0;
						        outdata_settlements_N2Oonly = 0;
						        outdata_node_code = 611;
						    }
						}
                        if ((ecozone_data[x] == boreal) || (ecozone_data[x] == temperate))      // settlements & infrastructure, peat, not burned, temperate/boreal
						{
						    outdata_settlements_CO2only = Biomass_tCO2e_nofire_CO2_only + peat_drain_total_CO2_only;
						    outdata_settlements_CH4only = 0 + peat_drain_total_CH4_only;
						    outdata_settlements_N2Oonly = 0 + peat_drain_total_N2O_only;
						    outdata_node_code = 62;
						}
					}
				}
				if (peat_data[x] == 0)// settlements & infrastructure, not peat
				{
					if (burn_data[x] > 0) // settlements & infrastructure, not peat, burned
					{
						if (ecozone_data[x] == tropical)   // settlements & infrastructure, not peat, burned, tropical
						{
                            if (ifl_primary_data[x] == 1)   // settlements & infrastructure, not peat, burned, tropical, IFL
                            {
                                if (plant_data[x] >= 1)     // settlements & infrastructure, not peat, burned, tropical, IFL, plantation
						        {
						            outdata_settlements_CO2only = Biomass_tCO2e_yesfire_CO2_only;
						            outdata_settlements_CH4only = Biomass_tCO2e_yesfire_CH4_only;
						            outdata_settlements_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						            outdata_node_code = 63;
						        }
						        if (plant_data[x] == 0)     // settlements & infrastructure, not peat, burned, tropical, IFL, not plantation
						        {
						            outdata_settlements_CO2only = Biomass_tCO2e_yesfire_CO2_only + minsoil;
						            outdata_settlements_CH4only = Biomass_tCO2e_yesfire_CH4_only;
						            outdata_settlements_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						            outdata_node_code = 631;
						        }
						    }
						    if (ifl_primary_data[x] == 0)   // settlements & infrastructure, not peat, burned, tropical, not IFL
						    {
                                if (plant_data[x] >= 1)     // settlements & infrastructure, not peat, burned, tropical, not IFL, plantation
						        {
						            outdata_settlements_CO2only = Biomass_tCO2e_yesfire_CO2_only;
						            outdata_settlements_CH4only = Biomass_tCO2e_yesfire_CH4_only;
						            outdata_settlements_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						            outdata_node_code = 64;
						        }
						        if (plant_data[x] == 0)     // settlements & infrastructure, not peat, burned, tropical, not IFL, not plantation
						        {
						            outdata_settlements_CO2only = Biomass_tCO2e_yesfire_CO2_only + minsoil;
						            outdata_settlements_CH4only = Biomass_tCO2e_yesfire_CH4_only;
						            outdata_settlements_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						            outdata_node_code = 641;
						        }
                            }
						}
						if (ecozone_data[x] == boreal)   // settlements & infrastructure, not peat, burned, boreal
						{
                            outdata_settlements_CO2only = Biomass_tCO2e_yesfire_CO2_only + minsoil;
                            outdata_settlements_CH4only = Biomass_tCO2e_yesfire_CH4_only;
						    outdata_settlements_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						    outdata_node_code = 65;
						}
						if (ecozone_data[x] == temperate)   // settlements & infrastructure, not peat, burned, temperate
						{
						    if (plant_data[x] >= 1)     // settlements & infrastructure, not peat, burned, temperate, plantation
						    {
						        outdata_settlements_CO2only = Biomass_tCO2e_yesfire_CO2_only;
						        outdata_settlements_CH4only = Biomass_tCO2e_yesfire_CH4_only;
						        outdata_settlements_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						        outdata_node_code = 66;
						    }
						    if (plant_data[x] == 0)     // settlements & infrastructure, not peat, burned, temperate, not plantation
						    {
						        outdata_settlements_CO2only = Biomass_tCO2e_yesfire_CO2_only + minsoil;
						        outdata_settlements_CH4only = Biomass_tCO2e_yesfire_CH4_only;
						        outdata_settlements_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						        outdata_node_code = 661;
						    }
						}
					}
					if (burn_data[x] == 0) // settlements & infrastructure, not peat, not burned
					{
						if (ecozone_data[x] == tropical)   // settlements & infrastructure, not peat, not burned, tropical
						{
						    if (plant_data[x] >= 1)     // settlements & infrastructure, not peat, not burned, tropical, plantation
						    {
						        outdata_settlements_CO2only = Biomass_tCO2e_nofire_CO2_only;
						        outdata_settlements_CH4only = 0;
						        outdata_settlements_N2Oonly = 0;
						        outdata_node_code = 67;
						    }
						    if (plant_data[x] == 0)     // settlements & infrastructure, not peat, not burned, tropical, not plantation
						    {
						        outdata_settlements_CO2only = Biomass_tCO2e_nofire_CO2_only + minsoil;
						        outdata_settlements_CH4only = 0;
						        outdata_settlements_N2Oonly = 0;
						        outdata_node_code = 671;
						    }
						}
						if (ecozone_data[x] == boreal)   // settlements & infrastructure, not peat, not burned, boreal
						{
                            outdata_settlements_CO2only = Biomass_tCO2e_nofire_CO2_only + minsoil;
                            outdata_settlements_CH4only = 0;
						    outdata_settlements_N2Oonly = 0;
                            outdata_node_code = 68;
						}
						if (ecozone_data[x] == temperate)   // settlements & infrastructure, not peat, not burned, temperate
						{
						    if (plant_data[x] >= 1)     // settlements & infrastructure, not peat, not burned, temperate, plantation
						    {
						        outdata_settlements_CO2only = Biomass_tCO2e_nofire_CO2_only;
						        outdata_settlements_CH4only = 0;
						        outdata_settlements_N2Oonly = 0;
						        outdata_node_code = 69;
						    }
						    if (plant_data[x] == 0)     // settlements & infrastructure, not peat, not burned, temperate, not plantation
						    {
						        outdata_settlements_CO2only = Biomass_tCO2e_nofire_CO2_only + minsoil;
						        outdata_settlements_CH4only = 0;
						        outdata_settlements_N2Oonly = 0;
						        outdata_node_code = 691;
						    }
						}
					}
				}
				outdata_settlements_allgases = outdata_settlements_CO2only + outdata_settlements_CH4only + outdata_settlements_N2Oonly;
				outdata_settlements_nonCO2 = outdata_settlements_CH4only + outdata_settlements_N2Oonly;
			}

			// Emissions model for other natural disturbances
			else if (drivermodel_data[x] == 7)
			{
				Biomass_tCO2e_nofire_CO2_only = above_below_c * C_to_CO2;
				Biomass_tCO2e_yesfire_CO2_only = ((agc_data[x] / biomass_to_c) * Cf * Gef_CO2 * pow(10, -3));
                Biomass_tCO2e_yesfire_CH4_only = ((non_soil_bgc_c / biomass_to_c) * Cf * Gef_CH4 * pow(10, -3) * CH4_equiv);
				Biomass_tCO2e_yesfire_N2O_only = ((non_soil_bgc_c / biomass_to_c) * Cf * Gef_N2O * pow(10, -3) * N2O_equiv);

				if (peat_data[x] > 0) // other natural disturbances, peat
				{
					if (burn_data[x] > 0 ) // other natural disturbances, peat, burned
					{
						outdata_other_disturbances_CO2only = Biomass_tCO2e_yesfire_CO2_only + peat_drain_total_CO2_only + peatburn_CO2_only;
						outdata_other_disturbances_CH4only = Biomass_tCO2e_yesfire_CH4_only + peat_drain_total_CH4_only + peatburn_CH4_only;
						outdata_other_disturbances_N2Oonly = Biomass_tCO2e_yesfire_N2O_only + peat_drain_total_N2O_only;
						outdata_node_code = 70;
					}
					if (burn_data[x] == 0 )  // other natural disturbances, peat, not burned
					{
						if ((ecozone_data[x] == boreal) || (ecozone_data[x] == temperate))  // other natural disturbances, peat, not burned, temperate/boreal
						{
							outdata_other_disturbances_CO2only = Biomass_tCO2e_nofire_CO2_only;
							outdata_other_disturbances_CH4only = 0;
						    outdata_other_disturbances_N2Oonly = 0;
							outdata_node_code = 71;
						}
						if (ecozone_data[x] == tropical)// other natural disturbances, peat, not burned, tropical
						{
							if (plant_data[x] > 0)  // other natural disturbances, peat, not burned, tropical, plantation
							{
								outdata_other_disturbances_CO2only = Biomass_tCO2e_nofire_CO2_only + peat_drain_total_CO2_only;
								outdata_other_disturbances_CH4only = 0 + peat_drain_total_CH4_only;
								outdata_other_disturbances_N2Oonly = 0 + peat_drain_total_N2O_only;
								outdata_node_code = 72;
							}
							if (plant_data[x] == 0)  // other natural disturbances, peat, not burned, tropical, not plantation
							{
								outdata_other_disturbances_CO2only = Biomass_tCO2e_nofire_CO2_only;
								outdata_other_disturbances_CH4only = 0;
								outdata_other_disturbances_N2Oonly = 0;
								outdata_node_code = 721;
							}
						}
					}
				}
				else  // other natural disturbances, not peat
				{
					if (burn_data[x] > 0) // other natural disturbances, not peat, burned
					{
						outdata_other_disturbances_CO2only = Biomass_tCO2e_yesfire_CO2_only;
						outdata_other_disturbances_CH4only = Biomass_tCO2e_yesfire_CH4_only;
						outdata_other_disturbances_N2Oonly= Biomass_tCO2e_yesfire_N2O_only;
						outdata_node_code = 73;
					}
					if (burn_data[x] == 0) // other natural disturbances, not peat, not burned
					{
						outdata_other_disturbances_CO2only = Biomass_tCO2e_nofire_CO2_only;
						outdata_other_disturbances_CH4only = 0;
						outdata_other_disturbances_N2Oonly = 0;
						outdata_node_code = 74;
					}
				}
				outdata_other_disturbances_allgases = outdata_other_disturbances_CO2only + outdata_other_disturbances_CH4only + outdata_other_disturbances_N2Oonly;
				outdata_other_disturbances_nonCO2 = outdata_other_disturbances_CH4only + outdata_other_disturbances_N2Oonly;
			}

		    // Emissions for where there is no driver model.
		    // Radost said to make it the same as other natural disturbances
		    else
			{
				Biomass_tCO2e_nofire_CO2_only = above_below_c * C_to_CO2;
				Biomass_tCO2e_yesfire_CO2_only = ((agc_data[x] / biomass_to_c) * Cf * Gef_CO2 * pow(10, -3));
                Biomass_tCO2e_yesfire_CH4_only = ((non_soil_bgc_c / biomass_to_c) * Cf * Gef_CH4 * pow(10, -3) * CH4_equiv);
				Biomass_tCO2e_yesfire_N2O_only = ((non_soil_bgc_c / biomass_to_c) * Cf * Gef_N2O * pow(10, -3) * N2O_equiv);

				if (peat_data[x] > 0) // No driver, peat
				{
					if (burn_data[x] > 0 ) // No driver, peat, burned
					{
						outdata_no_driver_CO2only = Biomass_tCO2e_yesfire_CO2_only + peat_drain_total_CO2_only + peatburn_CO2_only;
						outdata_no_driver_CH4only = Biomass_tCO2e_yesfire_CH4_only + peat_drain_total_CH4_only + peatburn_CH4_only;
						outdata_no_driver_N2Oonly = Biomass_tCO2e_yesfire_N2O_only + peat_drain_total_N2O_only;
						outdata_node_code = 80;
					}
					if (burn_data[x] == 0 )  // No driver, peat, not burned
					{
						if ((ecozone_data[x] == boreal) || (ecozone_data[x] == temperate))  // No driver, peat, not burned, temperate/boreal
						{
							outdata_no_driver_CO2only = Biomass_tCO2e_nofire_CO2_only;
							outdata_no_driver_CH4only = 0;
							outdata_no_driver_N2Oonly = 0;
							outdata_node_code = 81;
						}
						if (ecozone_data[x] == tropical)// No driver, peat, not burned, tropical
						{
							if (plant_data[x] > 0)  // No driver, peat, not burned, tropical, plantation
							{
								outdata_no_driver_CO2only = Biomass_tCO2e_nofire_CO2_only + peat_drain_total_CO2_only;
								outdata_no_driver_CH4only = 0 + peat_drain_total_CH4_only;
							    outdata_no_driver_N2Oonly = 0 + peat_drain_total_N2O_only;
								outdata_node_code = 82;
							}
							if (plant_data[x] == 0)  // No driver, peat, not burned, tropical, not plantation
							{
								outdata_no_driver_CO2only = Biomass_tCO2e_nofire_CO2_only;
								outdata_no_driver_CH4only = 0;
							    outdata_no_driver_N2Oonly = 0;
								outdata_node_code = 821;
							}
						}
					}
				}
				else
				{
					if (burn_data[x] > 0) // No driver, not peat, burned
					{
						outdata_no_driver_CO2only = Biomass_tCO2e_yesfire_CO2_only;
						outdata_no_driver_CH4only = Biomass_tCO2e_yesfire_CH4_only;
					    outdata_no_driver_N2Oonly = Biomass_tCO2e_yesfire_N2O_only;
						outdata_node_code = 83;
					}
					if (burn_data[x] == 0) // No driver, not peat, not burned
					{
						outdata_no_driver_CO2only = Biomass_tCO2e_nofire_CO2_only;
						outdata_no_driver_CH4only = 0;
						outdata_no_driver_N2Oonly = 0;
						outdata_node_code = 84;
					}
				}
				outdata_no_driver_allgases = outdata_no_driver_CO2only + outdata_no_driver_CH4only + outdata_no_driver_N2Oonly;
				outdata_no_driver_nonCO2 = outdata_no_driver_CH4only + outdata_no_driver_N2Oonly;
			}

			// Write the value to the correct raster
			if (drivermodel_data[x] == 1)  // permanent ag
			{
				out_data_permanent_agriculture[x] = outdata_permanent_agriculture_allgases;
				out_data_hard_commodities[x] = 0;
				out_data_shifting_cultivation[x] = 0;
				out_data_forest_management[x] = 0;
				out_data_wildfire[x] = 0;
				out_data_settlements[x] = 0;
				out_data_other_disturbances[x] = 0;
				out_data_no_driver[x] = 0;
			}
			else if (drivermodel_data[x] == 2)  // hard commodities
			{
				out_data_permanent_agriculture[x] = 0;
				out_data_hard_commodities[x] = outdata_hard_commodities_allgases;
				out_data_shifting_cultivation[x] = 0;
				out_data_forest_management[x] = 0;
				out_data_wildfire[x] = 0;
				out_data_settlements[x] = 0;
				out_data_other_disturbances[x] = 0;
				out_data_no_driver[x] = 0;
			}
			else if (drivermodel_data[x] == 3)  // shifting cultivation
			{
				out_data_permanent_agriculture[x] = 0;
				out_data_hard_commodities[x] = 0;
				out_data_shifting_cultivation[x] = outdata_shifting_cultivation_allgases;
				out_data_forest_management[x] = 0;
				out_data_wildfire[x] = 0;
				out_data_settlements[x] = 0;
				out_data_other_disturbances[x] = 0;
				out_data_no_driver[x] = 0;
			}
			else if (drivermodel_data[x] == 4)  // forest management
			{
				out_data_permanent_agriculture[x] = 0;
				out_data_hard_commodities[x] = 0;
				out_data_shifting_cultivation[x] = 0;
				out_data_forest_management[x] = outdata_forest_management_allgases;
				out_data_wildfire[x] = 0;
				out_data_settlements[x] = 0;
				out_data_other_disturbances[x] = 0;
				out_data_no_driver[x] = 0;
			}
			else if (drivermodel_data[x] == 5)  // wildfire
			{
				out_data_permanent_agriculture[x] = 0;
				out_data_hard_commodities[x] = 0;
				out_data_shifting_cultivation[x] = 0;
				out_data_forest_management[x] = 0;
				out_data_wildfire[x] = outdata_wildfire_allgases;
				out_data_settlements[x] = 0;
				out_data_other_disturbances[x] = 0;
				out_data_no_driver[x] = 0;
			}
			else if (drivermodel_data[x] == 6)  // settlements & infrastructure
			{
				out_data_permanent_agriculture[x] = 0;
				out_data_hard_commodities[x] = 0;
				out_data_shifting_cultivation[x] = 0;
				out_data_forest_management[x] = 0;
				out_data_wildfire[x] = 0;
				out_data_settlements[x] = outdata_settlements_allgases;
				out_data_other_disturbances[x] = 0;
				out_data_no_driver[x] = 0;
			}
			else if (drivermodel_data[x] == 7)  // other natural disturbances
			{
				out_data_permanent_agriculture[x] = 0;
				out_data_hard_commodities[x] = 0;
				out_data_shifting_cultivation[x] = 0;
				out_data_forest_management[x] = 0;
				out_data_wildfire[x] = 0;
				out_data_settlements[x] = 0;
				out_data_other_disturbances[x] = outdata_other_disturbances_allgases;
				out_data_no_driver[x] = 0;
			}
			else                                // No driver
			{
				out_data_permanent_agriculture[x] = 0;
				out_data_hard_commodities[x] = 0;
				out_data_shifting_cultivation[x] = 0;
				out_data_forest_management[x] = 0;
				out_data_wildfire[x] = 0;
				out_data_settlements[x] = 0;
				out_data_other_disturbances[x] = 0;
				out_data_no_driver[x] = outdata_no_driver_allgases;
			}
				// Decision tree end node value stored in its raster
				out_data_node_code[x] = outdata_node_code;


				// Add up all drivers for a combined raster. Each pixel only has one driver
				outdata_alldrivers_allgases = outdata_permanent_agriculture_allgases + outdata_hard_commodities_allgases + outdata_shifting_cultivation_allgases + outdata_forest_management_allgases + outdata_wildfire_allgases + outdata_settlements_allgases + outdata_other_disturbances_allgases + outdata_no_driver_allgases;
				outdata_alldrivers_CO2only = outdata_permanent_agriculture_CO2only + outdata_hard_commodities_CO2only + outdata_shifting_cultivation_CO2only + outdata_forest_management_CO2only + outdata_wildfire_CO2only + outdata_settlements_CO2only + outdata_other_disturbances_CO2only + outdata_no_driver_CO2only;
				outdata_alldrivers_nonCO2 = outdata_permanent_agriculture_nonCO2 + outdata_hard_commodities_nonCO2 +outdata_shifting_cultivation_nonCO2 + outdata_forest_management_nonCO2 + outdata_wildfire_nonCO2 + outdata_settlements_nonCO2 + outdata_other_disturbances_nonCO2 + outdata_no_driver_nonCO2;
                outdata_alldrivers_CH4only = outdata_permanent_agriculture_CH4only + outdata_hard_commodities_CH4only +outdata_shifting_cultivation_CH4only + outdata_forest_management_CH4only + outdata_wildfire_CH4only + outdata_settlements_CH4only + outdata_other_disturbances_CH4only + outdata_no_driver_CH4only;
                outdata_alldrivers_N2Oonly = outdata_permanent_agriculture_N2Oonly + outdata_hard_commodities_N2Oonly +outdata_shifting_cultivation_N2Oonly + outdata_forest_management_N2Oonly + outdata_wildfire_N2Oonly + outdata_settlements_N2Oonly + outdata_other_disturbances_N2Oonly + outdata_no_driver_N2Oonly;

				if (outdata_alldrivers_allgases == 0)
				{
					out_data_alldrivers_allgasses[x] = 0;
					out_data_alldrivers_CO2only[x] = 0;
					out_data_alldrivers_nonCO2[x] = 0;
					out_data_alldrivers_CH4only[x] = 0;
					out_data_alldrivers_N2Oonly[x] = 0;
				}
				else{
					out_data_alldrivers_allgasses[x] = outdata_alldrivers_allgases;
					out_data_alldrivers_CO2only[x] = outdata_alldrivers_CO2only;
					out_data_alldrivers_nonCO2[x] = outdata_alldrivers_nonCO2;
					out_data_alldrivers_CH4only[x] = outdata_alldrivers_CH4only;
					out_data_alldrivers_N2Oonly[x] = outdata_alldrivers_N2Oonly;
				}
		}

		// If pixel is not on loss and carbon, all output rasters get 0
		else
		{
			out_data_permanent_agriculture[x] = 0;
			out_data_hard_commodities[x] = 0;
			out_data_shifting_cultivation[x] = 0;
			out_data_forest_management[x] = 0;
			out_data_wildfire[x] = 0;
			out_data_settlements[x] = 0;
			out_data_other_disturbances[x] = 0;
			out_data_no_driver[x] = 0;
			out_data_alldrivers_allgasses[x] = 0;
			out_data_alldrivers_CO2only[x] = 0;
			out_data_alldrivers_nonCO2[x] = 0;
			out_data_alldrivers_CH4only[x] = 0;
			out_data_alldrivers_N2Oonly[x] = 0;
			out_data_node_code[x] = 0;
		}
    }

// The following RasterIO writes (and the RasterIO reads at the start) produced compile warnings about unused results
// (warning: ignoring return value of 'CPLErr GDALRasterBand::RasterIO(GDALRWFlag, int, int, int, int, void*, int, int, GDALDataType, GSpacing, GSpacing, GDALRasterIOExtraArg*)', declared with attribute warn_unused_result [-Wunused-result]).
// I asked how to handle or silence the warnings at https://stackoverflow.com/questions/72410931/how-to-handle-warn-unused-result-wunused-result/72410978#72410978.
// The code below handles the warnings by directing them to arguments, which are then checked.
// For cerr instead of std::err: https://www.geeksforgeeks.org/cerr-standard-error-stream-object-in-cpp/

// Error code returned by each line saved as their own argument
CPLErr errcodeOut_alldrivers_allgasses = OUTBAND_ALLDRIVERS_ALLGASSES->RasterIO( GF_Write, 0, y, xsize, 1, out_data_alldrivers_allgasses, xsize, 1, GDT_Float32, 0, 0 );
CPLErr errcodeOut_alldrivers_CO2only = OUTBAND_ALLDRIVERS_CO2ONLY->RasterIO( GF_Write, 0, y, xsize, 1, out_data_alldrivers_CO2only, xsize, 1, GDT_Float32, 0, 0 );
CPLErr errcodeOut_alldrivers_nonCO2 = OUTBAND_ALLDRIVERS_NONCO2->RasterIO( GF_Write, 0, y, xsize, 1, out_data_alldrivers_nonCO2, xsize, 1, GDT_Float32, 0, 0 );
CPLErr errcodeOut_alldrivers_CH4only = OUTBAND_ALLDRIVERS_CH4ONLY->RasterIO( GF_Write, 0, y, xsize, 1, out_data_alldrivers_CH4only, xsize, 1, GDT_Float32, 0, 0 );
CPLErr errcodeOut_alldrivers_N2Oonly = OUTBAND_ALLDRIVERS_N2OONLY->RasterIO( GF_Write, 0, y, xsize, 1, out_data_alldrivers_N2Oonly, xsize, 1, GDT_Float32, 0, 0 );
CPLErr errcodeOut_node_code = OUTBAND_NODE_CODE->RasterIO( GF_Write, 0, y, xsize, 1, out_data_node_code, xsize, 1, GDT_UInt16, 0, 0 );

// Number of output files
int outSize = 6;

// Array of error codes returned from each output
CPLErr errcodeOutArray [outSize] = {errcodeOut_alldrivers_allgasses, errcodeOut_alldrivers_CO2only, errcodeOut_alldrivers_nonCO2, errcodeOut_alldrivers_CH4only, errcodeOut_alldrivers_N2Oonly, errcodeOut_node_code};

// Iterates through the output error codes to make sure that the error code is acceptable
int k;

for (k=0; k<outSize; k++)
{
    if (errcodeOutArray[k] != 0) {
        cerr << "rasterIO failed!\n";
        exit(1);
    }
}
}

GDALClose(INGDAL_AGC);
GDALClose((GDALDatasetH)OUTGDAL_ALLDRIVERS_ALLGASSES);
GDALClose((GDALDatasetH)OUTGDAL_ALLDRIVERS_CO2ONLY);
GDALClose((GDALDatasetH)OUTGDAL_ALLDRIVERS_NONCO2);
GDALClose((GDALDatasetH)OUTGDAL_ALLDRIVERS_CH4ONLY);
GDALClose((GDALDatasetH)OUTGDAL_ALLDRIVERS_N2OONLY);
GDALClose((GDALDatasetH)OUTGDAL_NODE_CODE);
return 0;
}