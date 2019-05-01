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
// if code is run other than <program name> <tile id> , will raise this error
if (argc != 2){cout << "Use <program name> <tile id>" << endl; return 1;}

// in files
string agb_name=argv[1];
string tile_id = argv[1]; // the tile id comes from the second argument. the first argument is the name of this code

string infolder = "cpp_util/";

// all these starting with "string" are naming variables
// these are all the input files
string agc_name = infolder + tile_id + "_t_AGC_ha_emis_year.tif";
string bgc_name = infolder + tile_id + "_t_BGC_ha_emis_year.tif";
string dead_name = infolder + tile_id + "_t_deadwood_C_ha_emis_year_2000.tif";
string litter_name = infolder + tile_id + "_t_litter_C_ha_emis_year_2000.tif";
string soil_name = infolder + tile_id + "_t_soil_C_ha_emis_year_2000.tif";
string loss_name = infolder + tile_id + "_loss_pre_2000_plant_masked.tif";
string burn_name = infolder + tile_id + "_burnyear.tif";
string ecozone_name = infolder + tile_id + "_fao_ecozones_bor_tem_tro_processed.tif";
string climate_name = infolder + tile_id + "_climate_zone_processed.tif";

string forestmodel_name = infolder + tile_id + "_tree_cover_loss_driver_processed.tif";
string peat_name = infolder + tile_id + "_peat_mask_processed.tif";
string ifl_name = infolder + tile_id + "_res_ifl_2000.tif";
string plant_name = infolder + tile_id + "__plantation_type_oilpalm_woodfiber_other_unmasked.tif";

// naming all of the output files
string out_name1= "outdata/" + tile_id + "_commodity_t_CO2_ha_gross_emis_year.tif";
string out_name2 = "outdata/" + tile_id + "_shifting_ag_t_CO2_ha_gross_emis_year.tif";
string out_name3 = "outdata/" + tile_id + "_forestry_t_CO2_ha_gross_emis_year.tif";
string out_name4 = "outdata/" + tile_id + "_wildfire_t_CO2_ha_gross_emis_year.tif";
string out_name5 = "outdata/" + tile_id + "_urbanization_t_CO2_ha_gross_emis_year.tif";
string out_name10 = "outdata/" + tile_id + "_all_drivers_t_CO2_ha_gross_emis_year.tif";
string out_name20 = "outdata/" + tile_id + "_decision_tree_nodes_gross_emis.tif";

// setting up the variables to hold the pixel location in x/y values
int x, y;
int xsize, ysize;
double GeoTransform[6]; // Fetch the affine transformation coefficients
double ulx, uly; double pixelsize;

//initialize GDAL for reading
// each of these "INBAND" are later associated with the string variables defined above
GDALAllRegister();
GDALDataset  *INGDAL; GDALRasterBand  *INBAND;
GDALDataset  *INGDAL2; GDALRasterBand  *INBAND2;
GDALDataset  *INGDAL3; GDALRasterBand  *INBAND3;
GDALDataset  *INGDAL4; GDALRasterBand  *INBAND4;
GDALDataset  *INGDAL5; GDALRasterBand  *INBAND5;
GDALDataset  *INGDAL6; GDALRasterBand  *INBAND6;
GDALDataset  *INGDAL8; GDALRasterBand  *INBAND8;
GDALDataset  *INGDAL9; GDALRasterBand  *INBAND9;
GDALDataset  *INGDAL10; GDALRasterBand  *INBAND10;
GDALDataset  *INGDAL11; GDALRasterBand  *INBAND11;
GDALDataset  *INGDAL12; GDALRasterBand  *INBAND12;
GDALDataset  *INGDAL13; GDALRasterBand  *INBAND13;
GDALDataset  *INGDAL14; GDALRasterBand  *INBAND14;
GDALDataset  *INGDAL15; GDALRasterBand  *INBAND15;

//open file (string variables defined above) and assign it extent and projection
INGDAL = (GDALDataset *) GDALOpen(agc_name.c_str(), GA_ReadOnly );
INBAND = INGDAL->GetRasterBand(1);

INGDAL2 = (GDALDataset *) GDALOpen(bgc_name.c_str(), GA_ReadOnly );
INBAND2 = INGDAL2->GetRasterBand(1);

INGDAL3 = (GDALDataset *) GDALOpen(forestmodel_name.c_str(), GA_ReadOnly );
INBAND3 = INGDAL3->GetRasterBand(1);

INGDAL4 = (GDALDataset *) GDALOpen(loss_name.c_str(), GA_ReadOnly );
INBAND4 = INGDAL4->GetRasterBand(1);

INGDAL5 = (GDALDataset *) GDALOpen(peat_name.c_str(), GA_ReadOnly );
INBAND5 = INGDAL5->GetRasterBand(1);

INGDAL6 = (GDALDataset *) GDALOpen(burn_name.c_str(), GA_ReadOnly );
INBAND6 = INGDAL6->GetRasterBand(1);

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

INGDAL13 = (GDALDataset *) GDALOpen(ifl_name.c_str(), GA_ReadOnly );
INBAND13 = INGDAL13->GetRasterBand(1);

INGDAL15 = (GDALDataset *) GDALOpen(plant_name.c_str(), GA_ReadOnly );
INBAND15 = INGDAL15->GetRasterBand(1);

// the rest of the code runs on the size of INBAND3. this can be changed
xsize=INBAND3->GetXSize();
ysize=INBAND3->GetYSize();
INGDAL->GetGeoTransform(GeoTransform);

ulx=GeoTransform[0];
uly=GeoTransform[3];
pixelsize=GeoTransform[1];

// if wanting to test a small corner of a raster, can manually set this. This starts at top left of raster
//xsize = 2500;
//ysize = 2500;

// print the raster size. should be 40,000 x 40,000 and pixel size 0.00025
cout << xsize <<", "<< ysize <<", "<< ulx <<", "<< uly << ", "<< pixelsize << endl;

//initialize GDAL for writing
GDALDriver *OUTDRIVER;
GDALDataset *OUTGDAL3;
GDALDataset *OUTGDAL2;
GDALDataset *OUTGDAL4;
GDALDataset *OUTGDAL1;
GDALDataset *OUTGDAL5;
GDALDataset *OUTGDAL10;
GDALDataset *OUTGDAL20;

GDALRasterBand *OUTBAND3;
GDALRasterBand *OUTBAND2;
GDALRasterBand *OUTBAND4;
GDALRasterBand *OUTBAND1;
GDALRasterBand *OUTBAND5;
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

OUTGDAL3 = OUTDRIVER->Create( out_name3.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL3->SetGeoTransform(adfGeoTransform); OUTGDAL3->SetProjection(OUTPRJ);
OUTBAND3 = OUTGDAL3->GetRasterBand(1);
OUTBAND3->SetNoDataValue(-9999);

OUTGDAL2 = OUTDRIVER->Create( out_name2.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL2->SetGeoTransform(adfGeoTransform); OUTGDAL2->SetProjection(OUTPRJ);
OUTBAND2 = OUTGDAL2->GetRasterBand(1);
OUTBAND2->SetNoDataValue(-9999);

OUTGDAL4 = OUTDRIVER->Create( out_name4.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL4->SetGeoTransform(adfGeoTransform); OUTGDAL4->SetProjection(OUTPRJ);
OUTBAND4 = OUTGDAL4->GetRasterBand(1);
OUTBAND4->SetNoDataValue(-9999);

OUTGDAL1 = OUTDRIVER->Create( out_name1.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL1->SetGeoTransform(adfGeoTransform); OUTGDAL1->SetProjection(OUTPRJ);
OUTBAND1 = OUTGDAL1->GetRasterBand(1);
OUTBAND1->SetNoDataValue(-9999);

OUTGDAL20 = OUTDRIVER->Create( out_name20.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL20->SetGeoTransform(adfGeoTransform); OUTGDAL20->SetProjection(OUTPRJ);
OUTBAND20 = OUTGDAL20->GetRasterBand(1);
OUTBAND20->SetNoDataValue(-9999);

OUTGDAL10 = OUTDRIVER->Create( out_name10.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL10->SetGeoTransform(adfGeoTransform); OUTGDAL10->SetProjection(OUTPRJ);
OUTBAND10 = OUTGDAL10->GetRasterBand(1);

OUTGDAL5 = OUTDRIVER->Create( out_name5.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL5->SetGeoTransform(adfGeoTransform); OUTGDAL5->SetProjection(OUTPRJ);
OUTBAND5 = OUTGDAL5->GetRasterBand(1);
OUTBAND5->SetNoDataValue(-9999);

//read/write data...
//In data
float agb_data[xsize];
float agc_data[xsize];
float bgc_data[xsize];
float loss_data[xsize];
float peat_data[xsize];
float forestmodel_data[xsize];
float burn_data[xsize];
float ecozone_data[xsize];
float soil_data[xsize];
float climate_data[xsize];
float dead_data[xsize];
float litter_data[xsize];
float ifl_data[xsize];
float plant_data[xsize];

//Out data
float out_data3[xsize];
float out_data2[xsize];
float out_data4[xsize];
float out_data1[xsize];
float out_data5[xsize];
float out_data10[xsize];
float out_data20[xsize];

// loop over the y coords, then the x coords
for (y=0; y<ysize; y++)
{

INBAND->RasterIO(GF_Read, 0, y, xsize, 1, agc_data, xsize, 1, GDT_Float32, 0, 0);
INBAND2->RasterIO(GF_Read, 0, y, xsize, 1, bgc_data, xsize, 1, GDT_Float32, 0, 0);
INBAND3->RasterIO(GF_Read, 0, y, xsize, 1, forestmodel_data, xsize, 1, GDT_Float32, 0, 0);
INBAND4->RasterIO(GF_Read, 0, y, xsize, 1, loss_data, xsize, 1, GDT_Float32, 0, 0);
INBAND5->RasterIO(GF_Read, 0, y, xsize, 1, peat_data, xsize, 1, GDT_Float32, 0, 0);
INBAND6->RasterIO(GF_Read, 0, y, xsize, 1, burn_data, xsize, 1, GDT_Float32, 0, 0);
INBAND8->RasterIO(GF_Read, 0, y, xsize, 1, ecozone_data, xsize, 1, GDT_Float32, 0, 0);
INBAND9->RasterIO(GF_Read, 0, y, xsize, 1, climate_data, xsize, 1, GDT_Float32, 0, 0);
INBAND10->RasterIO(GF_Read, 0, y, xsize, 1, dead_data, xsize, 1, GDT_Float32, 0, 0);
INBAND11->RasterIO(GF_Read, 0, y, xsize, 1, litter_data, xsize, 1, GDT_Float32, 0, 0);
INBAND12->RasterIO(GF_Read, 0, y, xsize, 1, soil_data, xsize, 1, GDT_Float32, 0, 0);
INBAND13->RasterIO(GF_Read, 0, y, xsize, 1, ifl_data, xsize, 1, GDT_Float32, 0, 0);
INBAND15->RasterIO(GF_Read, 0, y, xsize, 1, plant_data, xsize, 1, GDT_Float32, 0, 0);

for(x=0; x<xsize; x++)

	{

		float outdata4 = 0;  // wildfire
		float outdata2 = 0;  // shifting ag.
		float outdata1 = 0;  // deforestation/conversion
		float outdata3 = 0;  // forestry
		float outdata5 = 0;  // urbanization
		float outdata6 = 0;  // no disturbance driver
		float outdata10 = 0;  // total of all drivers
		float outdata20 = 0;  // flowchart node

		// float outdata4 = -9999;
		// float outdata2 = -9999;
		// float outdata1 = -9999;
		// float outdata3 = -9999;
		// float outdata20 = -9999;


		// change nodata to 0 bc we want to add them to create total carbon
		if (dead_data[x] == -9999)
		{
			dead_data[x] = 0;
		}
		if (litter_data[x] == -9999)
		{
				litter_data[x] = 0;
		}

		if (loss_data[x] > 0 && agc_data[x] > 0) // on loss AND carbon
		{
			float *vars;
			// in equations.cpp, a function called def_variables, we get back what the emissions factors should be,
			/// based on disturbance type and climate zone. these are later used for calculating emissions
			vars = def_variables(ecozone_data[x], forestmodel_data[x], ifl_data[x], climate_data[x], plant_data[x], loss_data[x]);

			float cf = *(vars + 0);
			float c02 = *(vars + 1);
			float ch = *(vars + 2);
			float n20 = *(vars + 3);
			float peatburn = *(vars + 4);
			float peatdrain = *(vars + 5);

            // define and calculate several values used later
			float total_c;
			total_c = agc_data[x] + bgc_data[x] + dead_data[x] +litter_data[x];

			float above_below_c;
			above_below_c = agc_data[x] + bgc_data[x];

			float minsoil;

			float flu;

			float Biomass_tCO2e_nofire;
			float Biomass_tCO2e_yesfire;

            // each disturbance type is a raster- outdata3 is forestry emissions. outdata20 is the code for each
            // combination of outputs. Defined in carbon-budget/emissions/node_codes.txt
			if (forestmodel_data[x] == 3) // forestry
			{
				Biomass_tCO2e_yesfire = (above_below_c * 3.67) + ((2 * above_below_c) * cf * ch * pow(10, -3) * 28) + ((2 * above_below_c) * cf * n20 * pow(10, -3) * 265);
				Biomass_tCO2e_nofire = (agc_data[x] + bgc_data[x]) * 3.67;

				flu = flu_val(climate_data[x], ecozone_data[x]);

				if (peat_data[x] > 0) // forestry, peat
				{
					if (burn_data[x] > 0 ) // forestry, peat, burned
					{
						outdata3 = Biomass_tCO2e_yesfire + peatdrain + peatburn;
						outdata20 = 30;
					}
					else // forestry, peat, not burned
					{
						if ((ecozone_data[x] == 2) || (ecozone_data[x] == 3))
						{
							outdata3 = Biomass_tCO2e_nofire;
							outdata20 = 31;
						}
						else
						{
							if (plant_data[x] > 0)
							{
								outdata3 = Biomass_tCO2e_nofire + peatdrain;
								outdata20 = 32;
							}
							else
							{
								outdata3 = Biomass_tCO2e_nofire;
								outdata20 = 33;
							}
						}
					}
				}
				else
				{
					if (burn_data[x] > 0) // forestry, not peat, burned
					{
						outdata3 = Biomass_tCO2e_yesfire;
						outdata20 = 34;
					}
					else // forestry, not peat, not burned
					{
						outdata3 = Biomass_tCO2e_nofire;
						outdata20 = 35;
					}
				}
			}

			else if (forestmodel_data[x] == 1)  // deforestation/conversion
			{
				Biomass_tCO2e_yesfire = (total_c * 3.67) + ((2 * total_c) * cf * ch * pow(10,-3) * 28) + ((2 * total_c) * cf * n20 * pow(10,-3) * 265);
				Biomass_tCO2e_nofire = total_c * 3.67;
				flu = flu_val(climate_data[x], ecozone_data[x]);
				minsoil = soil_data[x]-(soil_data[x] * flu);

				if (peat_data[x] > 0) // deforestation, peat
				{
					if (burn_data[x] > 0) // deforestation, peat, burned
					{
						outdata1 = Biomass_tCO2e_yesfire + peatdrain + peatburn;
						outdata20 = 11;
					}
					else // deforestation, peat, not burned
					{
						outdata1 = Biomass_tCO2e_nofire + peatdrain;
						outdata20 = 12;
					}
				}
				else // deforestation, not peat
				{
					if (burn_data[x] > 0) // deforestation, not peat, burned
					{
						outdata1 = Biomass_tCO2e_yesfire + minsoil;
						outdata20 = 13;
					}
					else // deforestation, not peat, not burned
					{
						outdata1 = Biomass_tCO2e_nofire + minsoil;
						outdata20 = 14;
					}
				}
			}

			else if (forestmodel_data[x] == 2) // shifting ag. only diff is flu val
			{
				Biomass_tCO2e_yesfire = (total_c * 3.67) + ((2 * total_c) * cf * ch * pow(10,-3) * 28) + ((2 * total_c) * cf * n20 * pow(10,-3) * 265);
				Biomass_tCO2e_nofire = total_c * 3.67;
				flu = 0.72;
				minsoil = soil_data[x]-(soil_data[x] * .72);

				if (peat_data[x] > 0) // deforestation, peat
				{
					if (burn_data[x] > 0) // deforestation, peat, burned
					{
						outdata2 = Biomass_tCO2e_yesfire + peatdrain + peatburn;
						outdata20 = 20;
					}
					else // deforestation, peat, not burned
					{
						outdata2 = Biomass_tCO2e_nofire + peatdrain;
						outdata20 = 21;
					}
				}
				else // deforestation, not peat
				{
					if (burn_data[x] > 0) // deforestation, not peat, burned
					{
						outdata2 = Biomass_tCO2e_yesfire + minsoil;
						outdata20 = 22;
					}
					else // deforestation, not peat, not burned
					{
						outdata2 = Biomass_tCO2e_nofire + minsoil;
						outdata20 = 23;
					}
				}
			}

		   else if (forestmodel_data[x] == 4) // wildfire
			{
				Biomass_tCO2e_yesfire = ((2 * above_below_c) * cf * c02 * pow(10, -3)) + ((2* above_below_c) * cf * ch * pow(10, -3) * 28) + ((2 * above_below_c) * cf * n20 * pow(10, -3) * 265);
				Biomass_tCO2e_nofire = above_below_c * 3.67;
				flu = flu_val(climate_data[x], ecozone_data[x]);

				if (peat_data[x] > 0) // wildfire, peat
				{
					if (burn_data[x] > 0) // wildfire, peat, burned
					{
						outdata4 = Biomass_tCO2e_yesfire + peatdrain + peatburn;
						outdata20 = 40;
					}

					else // wildfire, peat, not burned
					{
						if ((ecozone_data[x] == 2) || (ecozone_data[x] == 3)) // boreal or temperate
						{
							outdata4 = Biomass_tCO2e_nofire;
							outdata20 = 41;
						}
						else // tropics
						{
							outdata4 = Biomass_tCO2e_nofire + peatdrain;
							outdata20 = 42;
						}
					}
				}
				else  // wildfire, not peat
				{
					if (burn_data[x] > 0)  // wildfire, not peat, burned
					{
						outdata4 = Biomass_tCO2e_yesfire;
						outdata20 = 43;
					}
					else  // wildfire, not peat, not burned
					{
						outdata4 = Biomass_tCO2e_nofire;
						outdata20 = 44;
					}
				}
			}

		   else if (forestmodel_data[x] == 5)  // urbanization
			{
				Biomass_tCO2e_yesfire = (total_c * 3.67) + ((2 * total_c) * cf * ch * pow(10,-3) * 28) + ((2 * total_c) * cf * n20 * pow(10,-3) * 265);
				Biomass_tCO2e_nofire = total_c * 3.67;
				flu = 0.8;
				minsoil = soil_data[x]-(soil_data[x] * flu);

				if (peat_data[x] > 0) // urbanization, peat
				{
					if (burn_data[x] > 0) // urbanization, peat, burned
					{
						outdata5 = Biomass_tCO2e_yesfire + peatdrain + peatburn;
						outdata20 = 50;
					}
					else // urbanization, peat, not burned
					{
						outdata5 = Biomass_tCO2e_nofire + peatdrain;
						outdata20 = 51;
					}
				}
				else // urbanization, not peat
				{
					if (burn_data[x] > 0) // urbanization, not peat, burned
					{
						outdata5 = Biomass_tCO2e_yesfire + minsoil;
						outdata20 = 52;
					}
					else // urbanization, not peat, not burned
					{
						outdata5 = Biomass_tCO2e_nofire + minsoil;
						outdata20 = 53;
					}
				}
			}

		   else // no forest model data- make it no data except make disturbance model same as forestry, nancy said.
			{
				out_data3[x] = -9999;
				out_data2[x] = -9999;
				out_data1[x] = -9999;
				out_data4[x] = -9999;
				out_data5[x] = -9999;

				Biomass_tCO2e_yesfire = (above_below_c * 3.67) + ((2 * above_below_c) * cf * ch * pow(10, -3) * 28) + ((2 * above_below_c) * cf * n20 * pow(10, -3) * 265);

				Biomass_tCO2e_nofire = (agc_data[x] + bgc_data[x]) * 3.67;
				flu = flu_val(climate_data[x], ecozone_data[x]);

				if (peat_data[x] > 0) // no class, peat
				{
					if (burn_data[x] > 0 ) // no class, peat, burned
					{
						outdata6 = Biomass_tCO2e_yesfire + peatdrain + peatburn;
						outdata20 = 60;
					}
					else // no class, peat, not burned
					{
						if ((ecozone_data[x] == 2) || (ecozone_data[x] == 3))
						{
							outdata6 = Biomass_tCO2e_nofire;
							outdata20 = 61;
						}
						else
						{
							if (plant_data[x] > 0)
							{
								outdata6 = Biomass_tCO2e_nofire + peatdrain;
								outdata20 = 62;
							}
							else
							{
								outdata6 = Biomass_tCO2e_nofire;
								outdata20 = 63;
							}
						}
					}
				}
				else
				{
					if (burn_data[x] > 0) // no class, not peat, burned
					{
						outdata6 = Biomass_tCO2e_yesfire;
						outdata20 = 64;
					}
					else // no class, not peat, not burned
					{
						outdata6 = Biomass_tCO2e_nofire;
						outdata20 = 65;
					}
				}
			}

			// write the variable to the pixel value
			if (forestmodel_data[x] == 1)
			{
				out_data3[x] = -9999;
				out_data1[x] = outdata1;
				out_data2[x] = -9999;
				out_data4[x] = -9999;
				out_data5[x] = -9999;
			}
			else if (forestmodel_data[x] == 2)
			{
				out_data3[x] = -9999;
				out_data1[x] = -9999;
				out_data2[x] = outdata2;
				out_data4[x] = -9999;
				out_data5[x] = -9999;
			}
			else if (forestmodel_data[x] == 3)
			{
				out_data3[x] = outdata3;
				out_data1[x] = -9999;
				out_data2[x] = -9999;
				out_data4[x] = -9999;
				out_data5[x] = -9999;
			}
			else if (forestmodel_data[x] == 4)
			{
				out_data3[x] = -9999;
				out_data1[x] = -9999;
				out_data2[x] = -9999;
				out_data4[x] = outdata4;
				out_data5[x] = -9999;
			}
			else if (forestmodel_data[x] == 5)
			{
				out_data3[x] = -9999;
				out_data1[x] = -9999;
				out_data2[x] = -9999;
				out_data4[x] = -9999;
				out_data5[x] = outdata5;
			}

			else // another else statement to handle if no forest model data
			{
				out_data3[x] = -9999;
				out_data1[x] = -9999;
				out_data2[x] = -9999;
				out_data4[x] = -9999;
				out_data5[x] = -9999;
			}
				// node total raster
				out_data20[x] = outdata20;



				// add up all outputs to make merged output

				outdata10 = outdata1 + outdata3 + outdata2 + outdata4 + outdata5 + outdata6;
				if ((outdata10 == 0) || (outdata10 == -9999))
				{
					out_data10[x] = -9999;
				}
				else{
					out_data10[x] = outdata10;
				}
		}

		else // not on loss AND carbon
		{

			out_data3[x] = -9999;
			out_data2[x] = -9999;
			out_data4[x] = -9999;
			out_data1[x] = -9999;
			out_data5[x] = -9999;
			out_data10[x] = -9999;
			out_data20[x] = -9999;
		}
    }
OUTBAND3->RasterIO( GF_Write, 0, y, xsize, 1, out_data3, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND2->RasterIO( GF_Write, 0, y, xsize, 1, out_data2, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND4->RasterIO( GF_Write, 0, y, xsize, 1, out_data4, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND1->RasterIO( GF_Write, 0, y, xsize, 1, out_data1, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND5->RasterIO( GF_Write, 0, y, xsize, 1, out_data5, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND10->RasterIO( GF_Write, 0, y, xsize, 1, out_data10, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND20->RasterIO( GF_Write, 0, y, xsize, 1, out_data20, xsize, 1, GDT_Float32, 0, 0 );
}
GDALClose(INGDAL);
GDALClose((GDALDatasetH)OUTGDAL3);
GDALClose((GDALDatasetH)OUTGDAL2);
GDALClose((GDALDatasetH)OUTGDAL4);
GDALClose((GDALDatasetH)OUTGDAL1);
GDALClose((GDALDatasetH)OUTGDAL5);
GDALClose((GDALDatasetH)OUTGDAL10);
GDALClose((GDALDatasetH)OUTGDAL20);
return 0;
}
