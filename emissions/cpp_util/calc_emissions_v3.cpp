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

// these are the functions we call when doing complicated calculations
#include "flu_val.cpp"
#include "equations.cpp"

using namespace std;

//to compile:  c++ calc_emissions_v3.cpp -o calc_emissions_v3.exe -lgdal
// to compile on MINGW: g++ calc_emissions_v3.cpp -o calc_emissions_v3.exe -I /usr/local/include -L /usr/local/lib -lgdal
int main(int argc, char* argv[])
{
// If code is run other than <program name> <tile id> , it will raise this error.
if (argc != 2){cout << "Use <program name> <tile id>" << endl; return 1;}

// Input arguments
string agb_name=argv[1];
string tile_id = argv[1]; // The tile id comes from the second argument. The first argument is the name of this code.

string infolder = "cpp_util/";

// Model constants
int CH4_equiv;      // The CO2 equivalency (global warming potential) of CH4
CH4_equiv = 28;

int N2O_equiv;      // The CO2 equivalency (global warming potential) of N2O
N2O_equiv = 265;

int C_to_CO2;       // The conversion of carbon to CO2
C_to_CO2 = 44/12;

int model_years;    // How many loss years are in the model
model_years = 15;

int tropical;       // The ecozone code for the tropics
tropical = 1;

int temperate;      // The ecozone code for the temperate zone
temperate = 3;

int boreal;         // The ecozone code for the boreal zone
boreal = 2;

// Input files
// Carbon pools
string agc_name = infolder + tile_id + "_t_AGC_ha_emis_year.tif";
string bgc_name = infolder + tile_id + "_t_BGC_ha_emis_year.tif";
string dead_name = infolder + tile_id + "_t_deadwood_C_ha_emis_year_2000.tif";
string litter_name = infolder + tile_id + "_t_litter_C_ha_emis_year_2000.tif";
string soil_name = infolder + tile_id + "_t_soil_C_ha_emis_year_2000.tif";

// Other inputs
string loss_name = infolder + tile_id + "_loss_pre_2000_plant_masked.tif";
string burn_name = infolder + tile_id + "_burnyear.tif";
string ecozone_name = infolder + tile_id + "_fao_ecozones_bor_tem_tro_processed.tif";
string climate_name = infolder + tile_id + "_climate_zone_processed.tif";
string drivermodel_name = infolder + tile_id + "_tree_cover_loss_driver_processed.tif";
string peat_name = infolder + tile_id + "_peat_mask_processed.tif";
string ifl_name = infolder + tile_id + "_res_ifl_2000.tif";
string plant_name = infolder + tile_id + "_plantation_type_oilpalm_woodfiber_other_unmasked.tif";

// Output files: tonnes CO2/ha for each tree cover loss driver, their total, and the node for the decision tree
// that determines emissions
string out_name1= tile_id + "_commodity_t_CO2_ha_gross_emis_year.tif";
string out_name2 = tile_id + "_shifting_ag_t_CO2_ha_gross_emis_year.tif";
string out_name3 = tile_id + "_forestry_t_CO2_ha_gross_emis_year.tif";
string out_name4 =  tile_id + "_wildfire_t_CO2_ha_gross_emis_year.tif";
string out_name5 = tile_id + "_urbanization_t_CO2_ha_gross_emis_year.tif";
string out_name6 = tile_id + "_no_driver_t_CO2_ha_gross_emis_year.tif";
string out_name10 = tile_id + "_all_drivers_t_CO2_ha_gross_emis_year.tif";
string out_name20 = tile_id + "_decision_tree_nodes_gross_emis.tif";

// Setting up the variables to hold the pixel location in x/y values
int x, y;
int xsize, ysize;
double GeoTransform[6]; // Fetch the affine transformation coefficients
double ulx, uly; double pixelsize;

// Initialize GDAL for reading.
// Each of these "INBAND" are later associated with the string variables defined above.
GDALAllRegister();
GDALDataset  *INGDAL1; GDALRasterBand  *INBAND1;
GDALDataset  *INGDAL2; GDALRasterBand  *INBAND2;
GDALDataset  *INGDAL3; GDALRasterBand  *INBAND3;
GDALDataset  *INGDAL4; GDALRasterBand  *INBAND4;
GDALDataset  *INGDAL5; GDALRasterBand  *INBAND5;
GDALDataset  *INGDAL6; GDALRasterBand  *INBAND6;
GDALDataset  *INGDAL7; GDALRasterBand  *INBAND7;
GDALDataset  *INGDAL8; GDALRasterBand  *INBAND8;
GDALDataset  *INGDAL9; GDALRasterBand  *INBAND9;
GDALDataset  *INGDAL10; GDALRasterBand  *INBAND10;
GDALDataset  *INGDAL11; GDALRasterBand  *INBAND11;
GDALDataset  *INGDAL12; GDALRasterBand  *INBAND12;
GDALDataset  *INGDAL13; GDALRasterBand  *INBAND13;

//open file (string variables defined above) and assign it extent and projection
INGDAL1 = (GDALDataset *) GDALOpen(agc_name.c_str(), GA_ReadOnly );
INBAND1 = INGDAL1->GetRasterBand(1);

INGDAL2 = (GDALDataset *) GDALOpen(bgc_name.c_str(), GA_ReadOnly );
INBAND2 = INGDAL2->GetRasterBand(1);

INGDAL3 = (GDALDataset *) GDALOpen(drivermodel_name.c_str(), GA_ReadOnly );
INBAND3 = INGDAL3->GetRasterBand(1);

INGDAL4 = (GDALDataset *) GDALOpen(loss_name.c_str(), GA_ReadOnly );
INBAND4 = INGDAL4->GetRasterBand(1);

INGDAL5 = (GDALDataset *) GDALOpen(peat_name.c_str(), GA_ReadOnly );
INBAND5 = INGDAL5->GetRasterBand(1);

INGDAL6 = (GDALDataset *) GDALOpen(burn_name.c_str(), GA_ReadOnly );
INBAND6 = INGDAL6->GetRasterBand(1);

INGDAL7 = (GDALDataset *) GDALOpen(ifl_name.c_str(), GA_ReadOnly );
INBAND7 = INGDAL7->GetRasterBand(1);

INGDAL8 = (GDALDataset *) GDALOpen(ecozone_name.c_str(), GA_ReadOnly );
INBAND8 = INGDAL8->GetRasterBand(1);

INGDAL9 = (GDALDataset *) GDALOpen(climate_name.c_str(), GA_ReadOnly );
INBAND9 = INGDAL9->GetRasterBand(1);

INGDAL10 = (GDALDataset *) GDALOpen(dead_name.c_str(), GA_ReadOnly );
INBAND10 = INGDAL10->GetRasterBand(1);

INGDAL11 = (GDALDataset *) GDALOpen(litter_name.c_str(), GA_ReadOnly );
INBAND11 = INGDAL11->GetRasterBand(1);

INGDAL12 = (GDALDataset *) GDALOpen(soil_name.c_str(), GA_ReadOnly );
INBAND12 = INGDAL12->GetRasterBand(1);

INGDAL13 = (GDALDataset *) GDALOpen(plant_name.c_str(), GA_ReadOnly );
INBAND13 = INGDAL13->GetRasterBand(1);

// The rest of the code runs on the size of INBAND3. This can be changed.
xsize=INBAND3->GetXSize();
ysize=INBAND3->GetYSize();
INGDAL1->GetGeoTransform(GeoTransform);

ulx=GeoTransform[0];
uly=GeoTransform[3];
pixelsize=GeoTransform[1];

// // Manually change this to test the script on a small part of the raster. This starts at top left of the tile.
//xsize = 4300;
//ysize = 4300;

// Print the raster size and resolution. Should be 40,000 x 40,000 and pixel size 0.00025.
cout << xsize <<", "<< ysize <<", "<< ulx <<", "<< uly << ", "<< pixelsize << endl;

// Initialize GDAL for writing
GDALDriver *OUTDRIVER;
GDALDataset *OUTGDAL1;  // Commodities
GDALDataset *OUTGDAL2;  // Shifting ag
GDALDataset *OUTGDAL3;  // Forestry
GDALDataset *OUTGDAL4;  // Wildfire
GDALDataset *OUTGDAL5;  // Urbanization
GDALDataset *OUTGDAL6;  // No driver
GDALDataset *OUTGDAL10; // All emissions combined
GDALDataset *OUTGDAL20; // Decision tree node

GDALRasterBand *OUTBAND1;
GDALRasterBand *OUTBAND2;
GDALRasterBand *OUTBAND3;
GDALRasterBand *OUTBAND4;
GDALRasterBand *OUTBAND5;
GDALRasterBand *OUTBAND6;
GDALRasterBand *OUTBAND10;
GDALRasterBand *OUTBAND20;

OGRSpatialReference oSRS;
char *OUTPRJ = NULL;
char **papszOptions = NULL;
papszOptions = CSLSetNameValue( papszOptions, "COMPRESS", "LZW" );
OUTDRIVER = GetGDALDriverManager()->GetDriverByName("GTIFF");
if( OUTDRIVER == NULL ) {cout << "no driver" << endl; exit( 1 );};
oSRS.SetWellKnownGeogCS( "WGS84" );
oSRS.exportToWkt( &OUTPRJ );
double adfGeoTransform[6] = { ulx, pixelsize, 0, uly, 0, -1*pixelsize };

// Commoditiy gross emissions
OUTGDAL1 = OUTDRIVER->Create( out_name1.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL1->SetGeoTransform(adfGeoTransform); OUTGDAL1->SetProjection(OUTPRJ);
OUTBAND1 = OUTGDAL1->GetRasterBand(1);
OUTBAND1->SetNoDataValue(0);

// Shifting ag gross emissions
OUTGDAL2 = OUTDRIVER->Create( out_name2.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL2->SetGeoTransform(adfGeoTransform); OUTGDAL2->SetProjection(OUTPRJ);
OUTBAND2 = OUTGDAL2->GetRasterBand(1);
OUTBAND2->SetNoDataValue(0);

// Forestry gross emissions
OUTGDAL3 = OUTDRIVER->Create( out_name3.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL3->SetGeoTransform(adfGeoTransform); OUTGDAL3->SetProjection(OUTPRJ);
OUTBAND3 = OUTGDAL3->GetRasterBand(1);
OUTBAND3->SetNoDataValue(0);

// Wildfire gross emissions
OUTGDAL4 = OUTDRIVER->Create( out_name4.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL4->SetGeoTransform(adfGeoTransform); OUTGDAL4->SetProjection(OUTPRJ);
OUTBAND4 = OUTGDAL4->GetRasterBand(1);
OUTBAND4->SetNoDataValue(0);

// Urbanization gross emissions
OUTGDAL5 = OUTDRIVER->Create( out_name5.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL5->SetGeoTransform(adfGeoTransform); OUTGDAL5->SetProjection(OUTPRJ);
OUTBAND5 = OUTGDAL5->GetRasterBand(1);
OUTBAND5->SetNoDataValue(0);

// No driver gross emissions
OUTGDAL6 = OUTDRIVER->Create( out_name6.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL6->SetGeoTransform(adfGeoTransform); OUTGDAL6->SetProjection(OUTPRJ);
OUTBAND6 = OUTGDAL6->GetRasterBand(1);
OUTBAND6->SetNoDataValue(0);

// All loss combined
OUTGDAL10 = OUTDRIVER->Create( out_name10.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL10->SetGeoTransform(adfGeoTransform); OUTGDAL10->SetProjection(OUTPRJ);
OUTBAND10 = OUTGDAL10->GetRasterBand(1);
OUTBAND10->SetNoDataValue(0);

// Decision tree node
OUTGDAL20 = OUTDRIVER->Create( out_name20.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL20->SetGeoTransform(adfGeoTransform); OUTGDAL20->SetProjection(OUTPRJ);
OUTBAND20 = OUTGDAL20->GetRasterBand(1);
OUTBAND20->SetNoDataValue(0);


// Read/write data
// Inputs
float agb_data[xsize];
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
float ifl_data[xsize];
float plant_data[xsize];

// Outputs
float out_data1[xsize];
float out_data2[xsize];
float out_data3[xsize];
float out_data4[xsize];
float out_data5[xsize];
float out_data6[xsize];
float out_data10[xsize];
float out_data20[xsize];


// Loop over the y coordinates, then the x coordinates
for (y=0; y<ysize; y++)
{

INBAND1->RasterIO(GF_Read, 0, y, xsize, 1, agc_data, xsize, 1, GDT_Float32, 0, 0);
INBAND2->RasterIO(GF_Read, 0, y, xsize, 1, bgc_data, xsize, 1, GDT_Float32, 0, 0);
INBAND3->RasterIO(GF_Read, 0, y, xsize, 1, drivermodel_data, xsize, 1, GDT_Float32, 0, 0);
INBAND4->RasterIO(GF_Read, 0, y, xsize, 1, loss_data, xsize, 1, GDT_Float32, 0, 0);
INBAND5->RasterIO(GF_Read, 0, y, xsize, 1, peat_data, xsize, 1, GDT_Float32, 0, 0);
INBAND6->RasterIO(GF_Read, 0, y, xsize, 1, burn_data, xsize, 1, GDT_Float32, 0, 0);
INBAND7->RasterIO(GF_Read, 0, y, xsize, 1, ifl_data, xsize, 1, GDT_Float32, 0, 0);
INBAND8->RasterIO(GF_Read, 0, y, xsize, 1, ecozone_data, xsize, 1, GDT_Float32, 0, 0);
INBAND9->RasterIO(GF_Read, 0, y, xsize, 1, climate_data, xsize, 1, GDT_Float32, 0, 0);
INBAND10->RasterIO(GF_Read, 0, y, xsize, 1, dead_data, xsize, 1, GDT_Float32, 0, 0);
INBAND11->RasterIO(GF_Read, 0, y, xsize, 1, litter_data, xsize, 1, GDT_Float32, 0, 0);
INBAND12->RasterIO(GF_Read, 0, y, xsize, 1, soil_data, xsize, 1, GDT_Float32, 0, 0);
INBAND13->RasterIO(GF_Read, 0, y, xsize, 1, plant_data, xsize, 1, GDT_Float32, 0, 0);

for(x=0; x<xsize; x++)

    // Everything from here down analyzes one pixel at a time
	{

        // Initializes each output raster at 0 (nodata value)
		float outdata1 = 0;  // commodities
		float outdata2 = 0;  // shifting ag.
		float outdata3 = 0;  // forestry
		float outdata4 = 0;  // wildfire
		float outdata5 = 0;  // urbanization
		float outdata6 = 0;  // no driver
		float outdata10 = 0;  // total of all drivers
		float outdata20 = 0;  // flowchart node


        // Only evaluates pixels that have loss and carbon
		if (loss_data[x] > 0 && agc_data[x] > 0)
        {
			float *vars;
			// From equations.cpp, a function called def_variables, we get back several constants
			/// based on several input rasters for that pixel. These are later used for calculating emissions.
			vars = def_variables(ecozone_data[x], drivermodel_data[x], ifl_data[x], climate_data[x], plant_data[x], loss_data[x]);

			float Cf = *(vars + 0);
			float Gef_CO2 = *(vars + 1);
			float Gef_CH4 = *(vars + 2);
			float Gef_N2O = *(vars + 3);
			float peatburn = *(vars + 4);
			float peat_drain_total = *(vars + 5);

            // Define and calculate several values used later
			float non_soil_c;
			non_soil_c = agc_data[x] + bgc_data[x] + dead_data[x] + litter_data[x];

			float above_below_c;
			above_below_c = agc_data[x] + bgc_data[x];

			float minsoil;
			float flu;
			float Biomass_tCO2e_nofire;
			float Biomass_tCO2e_yesfire;

		    // Each driver is an output raster and has its own emissions model. outdata20 is the code for each
            // combination of outputs. Defined in carbon-budget/emissions/node_codes.txt

			// Emissions model for commodity-driven deforestation
			if (drivermodel_data[x] == 1)
			{
				Biomass_tCO2e_nofire = non_soil_c * C_to_CO2;
				Biomass_tCO2e_yesfire = (non_soil_c * C_to_CO2) + ((2 * non_soil_c) * Cf * Gef_CH4 * pow(10,-3) * CH4_equiv) + ((2 * non_soil_c) * Cf * Gef_N2O * pow(10,-3) * N2O_equiv);
				flu = flu_val(climate_data[x], ecozone_data[x]);
				minsoil = ((soil_data[x]-(soil_data[x] * flu))/20) * (model_years-loss_data[x]);

				if (peat_data[x] > 0) // Commodity, peat
				{
					if (burn_data[x] > 0) // Commodity, peat, burned
					{
						outdata1 = Biomass_tCO2e_yesfire + peat_drain_total + peatburn;
						outdata20 = 10;
					}
					if (burn_data[x] == 0)// Commodity, peat, not burned
					{
						outdata1 = Biomass_tCO2e_nofire + peat_drain_total;
						outdata20 = 11;
					}
				}
				if (peat_data[x] == 0)// Commodity, not peat
				{
					if (burn_data[x] > 0) // Commodity, not peat, burned
					{
						if (ecozone_data[x] == tropical)   // Commodity, not peat, burned, tropical
						{
                            if (ifl_data[x] == 1)   // Commodity, not peat, burned, tropical, IFL
                            {
                                if (plant_data[x] >= 1)     // Commodity, not peat, burned, tropical, IFL, plantation
						        {
						            outdata1 = Biomass_tCO2e_yesfire;
						            outdata20 = 12;
						        }
						        if (plant_data[x] == 0)     // Commodity, not peat, burned, tropical, IFL, not plantation
						        {
						            outdata1 = Biomass_tCO2e_yesfire + minsoil;
						            outdata20 = 121;
						        }
						    }
						    if (ifl_data[x] == 0)   // Commodity, not peat, burned, tropical, not IFL
						    {
                                if (plant_data[x] >= 1)     // Commodity, not peat, burned, tropical, not IFL, plantation
						        {
						            outdata1 = Biomass_tCO2e_yesfire;
						            outdata20 = 13;
						        }
						        if (plant_data[x] == 0)     // Commodity, not peat, burned, tropical, not IFL, not plantation
						        {
						            outdata1 = Biomass_tCO2e_yesfire + minsoil;
						            outdata20 = 131;
						        }
                            }
						}
						if (ecozone_data[x] == boreal)   // Commodity, not peat, burned, boreal
						{
                            outdata1 = Biomass_tCO2e_yesfire + minsoil;
						    outdata20 = 14;
						}
						if (ecozone_data[x] == temperate)   // Commodity, not peat, burned, temperate
						{
						    if (plant_data[x] >= 1)     // Commodity, not peat, burned, temperate, plantation
						    {
						        outdata1 = Biomass_tCO2e_yesfire;
						        outdata20 = 15;
						    }
						    if (plant_data[x] == 0)     // Commodity, not peat, burned, temperate, no plantation
						    {
						        outdata1 = Biomass_tCO2e_yesfire + minsoil;
						        outdata20 = 151;
						    }
						}
					}
					if (burn_data[x] == 0) // Commodity, not peat, not burned
					{
						if (ecozone_data[x] == tropical)   // Commodity, not peat, not burned, tropical
						{
						    if (plant_data[x] >= 1)     // Commodity, not peat, not burned, tropical, plantation
						    {
						        outdata1 = Biomass_tCO2e_nofire;
						        outdata20 = 16;
						    }
						    if (plant_data[x] == 0)     // Commodity, not peat, not burned, tropical, no plantation
						    {
						        outdata1 = Biomass_tCO2e_nofire + minsoil;
						        outdata20 = 161;
						    }
						}
						if (ecozone_data[x] == boreal)   // Commodity, not peat, not burned, boreal
						{
                            outdata1 = Biomass_tCO2e_nofire + minsoil;
                            outdata20 = 17;
						}
						if (ecozone_data[x] == temperate)   // Commodity, not peat, not burned, temperate
						{
						    if (plant_data[x] >= 1)     // Commodity, not peat, not burned, temperate, plantation
						    {
						        outdata1 = Biomass_tCO2e_nofire;
						        outdata20 = 18;
						    }
						    if (plant_data[x] == 0)     // Commodity, not peat, not burned, temperate, no plantation
						    {
						        outdata1 = Biomass_tCO2e_nofire + minsoil;
						        outdata20 = 181;
						    }
						}
					}
				}
//				if (outdata1 < 0)
//				{
                cout << "New pixel:" << endl;
                cout << "outdata1:" << outdata1 << endl;
                cout << "Biomass_tCO2e_yesfire: " << Biomass_tCO2e_yesfire << endl;
                cout << "Biomass_tCO2e_nofire: " << Biomass_tCO2e_nofire << endl;
                cout << "flu: " << flu << endl;
                cout << "minsoil part 1: " << ((soil_data[x]-(soil_data[x] * flu))/20) << endl;
                cout << "minsoil part 2: " << (model_years-loss_data[x]) << endl;
                cout << "minsoil: " << minsoil << endl;
                cout << "soildata: "	<< soil_data[x] << endl;
                cout << "lossname: " << loss_data[x] << endl;
                cout << "" << endl;
//				}
			}

			// Emissions model for shifting agriculture (only difference is flu val)
			else if (drivermodel_data[x] == 2)
			{
				Biomass_tCO2e_nofire = non_soil_c * C_to_CO2;
				Biomass_tCO2e_yesfire = (non_soil_c * C_to_CO2) + ((2 * non_soil_c) * Cf * Gef_CH4 * pow(10,-3) * CH4_equiv) + ((2 * non_soil_c) * Cf * Gef_N2O * pow(10,-3) * N2O_equiv);
				float shiftag_flu;
				shiftag_flu = 0.72;
				minsoil = ((soil_data[x]-(soil_data[x] * shiftag_flu))/20) * (model_years-loss_data[x]);

				if (peat_data[x] > 0) // Shifting ag, peat
				{
					if (burn_data[x] > 0) // Shifting ag, peat, burned
					{
						if ((ecozone_data[x] == boreal) || (ecozone_data[x] == temperate))      // Shifting ag, peat, burned, temperate/boreal
						{
						    outdata2 = Biomass_tCO2e_yesfire + peatburn;
						    outdata20 = 20;
						}
						if (ecozone_data[x] == tropical)      // Shifting ag, peat, burned, tropical
						{
						    outdata2 = Biomass_tCO2e_yesfire + peat_drain_total + peatburn;
						    outdata20 = 21;
						}
					}
					if (burn_data[x] == 0)// Shifting ag, peat, not burned
					{
						if ((ecozone_data[x] == boreal) || (ecozone_data[x] == temperate))      // Shifting ag, peat, not burned, temperate/boreal
						{
						    outdata2 = Biomass_tCO2e_nofire;
						    outdata20 = 22;
						}
						if (ecozone_data[x] == tropical)      // Shifting ag, peat, not burned, tropical
						{
						    if (plant_data[x] >= 1)     // Shifting ag, peat, not burned, tropical, plantation
						    {
						        outdata2 = Biomass_tCO2e_nofire + peat_drain_total;
						        outdata20 = 23;
						    }
						    if (plant_data[x] == 0)     // Shifting ag, peat, not burned, tropical, not plantation
						    {
						        outdata2 = Biomass_tCO2e_nofire;
						        outdata20 = 231;
						    }
						}
					}
				}
				if (peat_data[x] == 0)// Shifting ag, not peat
				{
					if (burn_data[x] > 0) // Shifting ag, not peat, burned
					{
						if (ecozone_data[x] == tropical)   // Shifting ag, not peat, burned, tropical
						{
                            if (ifl_data[x] == 1)   // Shifting ag, not peat, burned, tropical, IFL
                            {
                                if (plant_data[x] >= 1)     // Shifting ag, not peat, burned, tropical, IFL, plantation
						        {
						            outdata2 = Biomass_tCO2e_yesfire;
						            outdata20 = 24;
						        }
						        if (plant_data[x] == 0)     // Shifting ag, not peat, burned, tropical, IFL, not plantation
						        {
						            outdata2 = Biomass_tCO2e_yesfire + minsoil;
						            outdata20 = 241;
						        }
						    }
						    if (ifl_data[x] == 0)   // Shifting ag, not peat, burned, tropical, not IFL
						    {
                                if (plant_data[x] >= 1)     // Shifting ag, not peat, burned, tropical, not IFL, plantation
						        {
						            outdata2 = Biomass_tCO2e_yesfire;
						            outdata20 = 25;
						        }
						        if (plant_data[x] == 0)     // Shifting ag, not peat, burned, tropical, not IFL, not plantation
						        {
						            outdata2 = Biomass_tCO2e_yesfire + minsoil;
						            outdata20 = 251;
						        }
                            }
						}
						if (ecozone_data[x] == boreal)   // Shifting ag, not peat, burned, boreal
						{
                            outdata2 = Biomass_tCO2e_yesfire + minsoil;
						    outdata20 = 26;
						}
						if (ecozone_data[x] == temperate)   // Shifting ag, not peat, burned, temperate
						{
						    if (plant_data[x] >= 1)     // Shifting ag, not peat, burned, temperate, plantation
						    {
						        outdata2 = Biomass_tCO2e_yesfire;
						        outdata20 = 27;
						    }
						    if (plant_data[x] == 0)     // Shifting ag, not peat, burned, temperate, no plantation
						    {
						        outdata2 = Biomass_tCO2e_yesfire + minsoil;
						        outdata20 = 271;
						    }
						}
					}
					if (burn_data[x] == 0) // Shifting ag, not peat, not burned
					{
						if (ecozone_data[x] == tropical)   // Shifting ag, not peat, not burned, tropical
						{
						    if (plant_data[x] >= 1)     // Shifting ag, not peat, not burned, tropical, plantation
						    {
						        outdata2 = Biomass_tCO2e_nofire;
						        outdata20 = 28;
						    }
						    if (plant_data[x] == 0)     // Shifting ag, not peat, not burned, tropical, no plantation
						    {
						        outdata2 = Biomass_tCO2e_nofire + minsoil;
						        outdata20 = 281;
						    }
						}
						if (ecozone_data[x] == boreal)   // Shifting ag, not peat, not burned, boreal
						{
                            outdata2 = Biomass_tCO2e_nofire + minsoil;
                            outdata20 = 29;
						}
						if (ecozone_data[x] == temperate)   // Shifting ag, not peat, not burned, temperate
						{
						    if (plant_data[x] >= 1)     // Shifting ag, not peat, not burned, temperate, plantation
						    {
						        outdata2 = Biomass_tCO2e_nofire;
						        outdata20 = 291;
						    }
						    if (plant_data[x] == 0)     // Shifting ag, not peat, not burned, temperate, no plantation
						    {
						        outdata2 = Biomass_tCO2e_nofire + minsoil;
						        outdata20 = 292;
						    }
						}
					}
				}
			}

			// Emissions model for forestry
			else if (drivermodel_data[x] == 3)
			{
				Biomass_tCO2e_nofire = above_below_c * C_to_CO2;
				Biomass_tCO2e_yesfire = ((2 * agc_data[x]) * Cf * Gef_CO2 * pow(10, -3)) + ((2 * agc_data[x]) * Cf * Gef_CH4 * pow(10, -3) * CH4_equiv) + ((2 * agc_data[x]) * Cf * Gef_N2O * pow(10, -3) * N2O_equiv);

				if (peat_data[x] > 0) // Forestry, peat
				{
					if (burn_data[x] > 0 ) // Forestry, peat, burned
					{
						outdata3 = Biomass_tCO2e_yesfire + peat_drain_total + peatburn;
						outdata20 = 30;
					}
					if (burn_data[x] == 0 )  // Forestry, peat, not burned
					{
						if ((ecozone_data[x] == boreal) || (ecozone_data[x] == temperate))  // Forestry, peat, not burned, temperate/boreal
						{
							outdata3 = Biomass_tCO2e_nofire;
							outdata20 = 31;
						}
						if (ecozone_data[x] == tropical)// Forestry, peat, not burned, tropical
						{
							if (plant_data[x] > 0)  // Forestry, peat, not burned, tropical, plantation
							{
								outdata3 = Biomass_tCO2e_nofire + peat_drain_total;
								outdata20 = 32;
							}
							if (plant_data[x] == 0)  // Forestry, peat, not burned, tropical, not plantation
							{
								outdata3 = Biomass_tCO2e_nofire;
								outdata20 = 321;
							}
						}
					}
				}
				else
				{
					if (burn_data[x] > 0) // Forestry, not peat, burned
					{
						outdata3 = Biomass_tCO2e_yesfire;
						outdata20 = 33;
					}
					if (burn_data[x] == 0) // Forestry, not peat, not burned
					{
						outdata3 = Biomass_tCO2e_nofire;
						outdata20 = 34;
					}
				}
			}

		    // Emissions model for wildfires
		    else if (drivermodel_data[x] == 4)
			{
				Biomass_tCO2e_nofire = above_below_c * C_to_CO2;
				Biomass_tCO2e_yesfire = ((2 * agc_data[x]) * Cf * Gef_CO2 * pow(10, -3)) + ((2 * agc_data[x]) * Cf * Gef_CH4 * pow(10, -3) * CH4_equiv) + ((2 * agc_data[x]) * Cf * Gef_N2O * pow(10, -3) * N2O_equiv);
				flu = flu_val(climate_data[x], ecozone_data[x]);

				if (peat_data[x] > 0) // Wildfire, peat
				{
					if (burn_data[x] > 0) // Wildfire, peat, burned
					{
						outdata4 = Biomass_tCO2e_yesfire + peat_drain_total + peatburn;
						outdata20 = 40;
					}
					if (burn_data[x] == 0) // Wildfire, peat, not burned
					{
						if ((ecozone_data[x] == boreal) || (ecozone_data[x] == temperate)) // Wildfire, peat, not burned, temperate/boreal
						{
							outdata4 = Biomass_tCO2e_nofire;
							outdata20 = 41;
						}
						if (ecozone_data[x] == tropical) // Wildfire, peat, not burned, tropical
						{
					        if (plant_data[x] > 0)  // Wildfire, peat, not burned, tropical, plantation
							{
								outdata4 = Biomass_tCO2e_nofire + peat_drain_total;
								outdata20 = 42;
							}
							if (plant_data[x] == 0)  // Wildfire, peat, not burned, tropical, not plantation
							{
								outdata4 = Biomass_tCO2e_nofire;
								outdata20 = 421;
							}
						}
					}
				}
				else  // Wildfire, not peat
				{
					if (burn_data[x] > 0)  // Wildfire, not peat, burned
					{
						outdata4 = Biomass_tCO2e_yesfire;
						outdata20 = 43;
					}
					else  // Wildfire, not peat, not burned
					{
						outdata4 = Biomass_tCO2e_nofire;
						outdata20 = 44;
					}
				}
			}

		    // Emissions model for urbanization
		    else if (drivermodel_data[x] == 5)
			{
				Biomass_tCO2e_nofire = non_soil_c * C_to_CO2;
				Biomass_tCO2e_yesfire = (non_soil_c * C_to_CO2) + ((2 * non_soil_c) * Cf * Gef_CH4 * pow(10,-3) * CH4_equiv) + ((2 * non_soil_c) * Cf * Gef_N2O * pow(10,-3) * N2O_equiv);
				float urb_flu;
				urb_flu = 0.8;
				minsoil = ((soil_data[x]-(soil_data[x] * urb_flu))/20) * (model_years-loss_data[x]);

                if (peat_data[x] > 0) // Urbanization, peat
				{
					if (burn_data[x] > 0) // Urbanization, peat, burned
					{
						outdata5 = Biomass_tCO2e_yesfire + peat_drain_total + peatburn;
						outdata20 = 50;
					}
					if (burn_data[x] == 0)// Urbanization, peat, not burned
					{
						outdata5 = Biomass_tCO2e_nofire + peat_drain_total;
						outdata20 = 51;
					}
				}
				if (peat_data[x] == 0)// Urbanization, not peat
				{
					if (burn_data[x] > 0) // Urbanization, not peat, burned
					{
						if (ecozone_data[x] == tropical)   // Urbanization, not peat, burned, tropical
						{
                            if (ifl_data[x] == 1)   // Urbanization, not peat, burned, tropical, IFL
                            {
                                if (plant_data[x] >= 1)     // Urbanization, not peat, burned, tropical, IFL, plantation
						        {
						            outdata5 = Biomass_tCO2e_yesfire;
						            outdata20 = 52;
						        }
						        if (plant_data[x] == 0)     // Urbanization, not peat, burned, tropical, IFL, not plantation
						        {
						            outdata5 = Biomass_tCO2e_yesfire + minsoil;
						            outdata20 = 521;
						        }
						    }
						    if (ifl_data[x] == 0)   // Urbanization, not peat, burned, tropical, not IFL
						    {
                                if (plant_data[x] >= 1)     // Urbanization, not peat, burned, tropical, not IFL, plantation
						        {
						            outdata5 = Biomass_tCO2e_yesfire;
						            outdata20 = 53;
						        }
						        if (plant_data[x] == 0)     // Urbanization, not peat, burned, tropical, not IFL, not plantation
						        {
						            outdata5 = Biomass_tCO2e_yesfire + minsoil;
						            outdata20 = 531;
						        }
                            }
						}
						if (ecozone_data[x] == boreal)   // Urbanization, not peat, burned, boreal
						{
                            outdata5 = Biomass_tCO2e_yesfire + minsoil;
						    outdata20 = 54;
						}
						if (ecozone_data[x] == temperate)   // Urbanization, not peat, burned, temperate
						{
						    if (plant_data[x] >= 1)     // Urbanization, not peat, burned, temperate, plantation
						    {
						        outdata5 = Biomass_tCO2e_yesfire;
						        outdata20 = 55;
						    }
						    if (plant_data[x] == 0)     // Urbanization, not peat, burned, temperate, no plantation
						    {
						        outdata5 = Biomass_tCO2e_yesfire + minsoil;
						        outdata20 = 551;
						    }
						}
					}
					if (burn_data[x] == 0) // Urbanization, not peat, not burned
					{
						if ((ecozone_data[x] == tropical))   // Urbanization, not peat, not burned, tropical
						{
						    if (plant_data[x] >= 1)     // Urbanization, not peat, not burned, tropical, plantation
						    {
						        outdata5 = Biomass_tCO2e_nofire;
						        outdata20 = 56;
						    }
						    if (plant_data[x] == 0)     // Urbanization, not peat, not burned, tropical, no plantation
						    {
						        outdata5 = Biomass_tCO2e_nofire + minsoil;
						        outdata20 = 561;
						    }
						}
						if (ecozone_data[x] == boreal)   // Urbanization, not peat, not burned, boreal
						{
                            outdata5 = Biomass_tCO2e_nofire + minsoil;
                            outdata20 = 57;
						}
						if (ecozone_data[x] == temperate)   // Urbanization, not peat, not burned, temperate
						{
						    if (plant_data[x] >= 1)     // Urbanization, not peat, not burned, temperate, plantation
						    {
						        outdata5 = Biomass_tCO2e_nofire;
						        outdata20 = 58;
						    }
						    if (plant_data[x] == 0)     // Urbanization, not peat, not burned, temperate, no plantation
						    {
						        outdata5 = Biomass_tCO2e_nofire + minsoil;
						        outdata20 = 581;
						    }
						}
					}
				}
			}

		    // Emissions for where there is no driver model.
		    // Nancy said to make this the same as forestry.
		    else
			{
				Biomass_tCO2e_nofire = above_below_c * C_to_CO2;
				Biomass_tCO2e_yesfire = ((2 * agc_data[x]) * Cf * Gef_CO2 * pow(10, -3)) + ((2 * agc_data[x]) * Cf * Gef_CH4 * pow(10, -3) * CH4_equiv) + ((2 * agc_data[x]) * Cf * Gef_N2O * pow(10, -3) * N2O_equiv);
				flu = flu_val(climate_data[x], ecozone_data[x]);

				if (peat_data[x] > 0) // No driver, peat
				{
					if (burn_data[x] > 0 ) // No driver, peat, burned
					{
						outdata6 = Biomass_tCO2e_yesfire + peat_drain_total + peatburn;
						outdata20 = 60;
					}
					if (burn_data[x] == 0 )  // No driver, peat, not burned
					{
						if ((ecozone_data[x] == boreal) || (ecozone_data[x] == temperate))  // No driver, peat, not burned, temperate/boreal
						{
							outdata6 = Biomass_tCO2e_nofire;
							outdata20 = 61;
						}
						if (ecozone_data[x] == tropical)// No driver, peat, not burned, tropical
						{
							if (plant_data[x] > 0)  // No driver, peat, not burned, tropical, plantation
							{
								outdata6 = Biomass_tCO2e_nofire + peat_drain_total;
								outdata20 = 62;
							}
							if (plant_data[x] == 0)  // No driver, peat, not burned, tropical, not plantation
							{
								outdata6 = Biomass_tCO2e_nofire;
								outdata20 = 63;
							}
						}
					}
				}
				else
				{
					if (burn_data[x] > 0) // No driver, not peat, burned
					{
						outdata6 = Biomass_tCO2e_yesfire;
						outdata20 = 64;
					}
					if (burn_data[x] == 0) // No driver, not peat, not burned
					{
						outdata6 = Biomass_tCO2e_nofire;
						outdata20 = 65;
					}
				}
			}

			// Write the value to the correct raster
			if (drivermodel_data[x] == 1)  // Commodities
			{
				out_data1[x] = outdata1;
				out_data2[x] = 0;
				out_data3[x] = 0;
				out_data4[x] = 0;
				out_data5[x] = 0;
				out_data6[x] = 0;
			}
			else if (drivermodel_data[x] == 2)  // Shifting ag
			{
				out_data1[x] = 0;
				out_data2[x] = outdata2;
				out_data3[x] = 0;
				out_data4[x] = 0;
				out_data5[x] = 0;
				out_data6[x] = 0;
			}
			else if (drivermodel_data[x] == 3)  // Forestry
			{
				out_data1[x] = 0;
				out_data2[x] = 0;
				out_data3[x] = outdata3;
				out_data4[x] = 0;
				out_data5[x] = 0;
				out_data6[x] = 0;
			}
			else if (drivermodel_data[x] == 4)  // Wildfire
			{
				out_data1[x] = 0;
				out_data2[x] = 0;
				out_data3[x] = 0;
				out_data4[x] = outdata4;
				out_data5[x] = 0;
				out_data6[x] = 0;
			}
			else if (drivermodel_data[x] == 5)  // Urbanization
			{
				out_data1[x] = 0;
				out_data2[x] = 0;
				out_data3[x] = 0;
				out_data4[x] = 0;
				out_data5[x] = outdata5;
				out_data6[x] = 0;
			}
			else                                // No driver
			{
				out_data1[x] = 0;
				out_data2[x] = 0;
				out_data3[x] = 0;
				out_data4[x] = 0;
				out_data5[x] = 0;
				out_data6[x] = outdata6;
			}
				// Decision tree end node value stored in its raster
				out_data20[x] = outdata20;


				// Add up all drivers for a combined raster. Each pixel only has one driver
				outdata10 = outdata1 + outdata2 + outdata3 + outdata4 + outdata5 + outdata6;
				if (outdata10 == 0)
				{
					out_data10[x] = 0;
				}
				else{
					out_data10[x] = outdata10;
				}
		}

		// If pixel is not on loss and carbon, all output rasters get 0
		else
		{

			out_data1[x] = 0;
			out_data2[x] = 0;
			out_data3[x] = 0;
			out_data4[x] = 0;
			out_data5[x] = 0;
			out_data6[x] = 0;
			out_data10[x] = 0;
			out_data20[x] = 0;
		}
    }

OUTBAND1->RasterIO( GF_Write, 0, y, xsize, 1, out_data1, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND2->RasterIO( GF_Write, 0, y, xsize, 1, out_data2, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND3->RasterIO( GF_Write, 0, y, xsize, 1, out_data3, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND4->RasterIO( GF_Write, 0, y, xsize, 1, out_data4, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND5->RasterIO( GF_Write, 0, y, xsize, 1, out_data5, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND6->RasterIO( GF_Write, 0, y, xsize, 1, out_data6, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND10->RasterIO( GF_Write, 0, y, xsize, 1, out_data10, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND20->RasterIO( GF_Write, 0, y, xsize, 1, out_data20, xsize, 1, GDT_Float32, 0, 0 );
}

GDALClose(INGDAL1);
GDALClose((GDALDatasetH)OUTGDAL1);
GDALClose((GDALDatasetH)OUTGDAL2);
GDALClose((GDALDatasetH)OUTGDAL3);
GDALClose((GDALDatasetH)OUTGDAL4);
GDALClose((GDALDatasetH)OUTGDAL5);
GDALClose((GDALDatasetH)OUTGDAL6);
GDALClose((GDALDatasetH)OUTGDAL10);
GDALClose((GDALDatasetH)OUTGDAL20);
return 0;
}