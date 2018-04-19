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

#include <gdal_priv.h>
#include <cpl_conv.h>
#include <ogr_spatialref.h>

#include "flu_val.cpp"
#include "equations.cpp"

using namespace std;

//to compile:  c++ raster_math.cpp -o raster_math -lgdal
// to compile on MINGW: g++ calc_emissions_v2.cpp -o calc_emissions_v2.exe -I /usr/local/include -L /usr/local/lib -lgdal
int main(int argc, char* argv[])
{
//passing arguments
if (argc != 2){cout << "Use <program name> <tile id>" << endl; return 1;}

// in files
string agb_name=argv[1];
string tile_id = argv[1];
string forestmodel_name = tile_id + "_res_Goode_FinalClassification_15_50uncertain_expanded_wgs84.tif";
string bgc_name = tile_id + "_bgc.tif";
string agc_name = tile_id + "_carbon.tif";
string loss_name = tile_id + "_loss.tif";
string burn_name = tile_id + "_burnyear.tif"; 
string ecozone_name = tile_id + "_res_fao_ecozones_bor_tem_tro.tif";
string climate_name = tile_id + "_res_climate_zone.tif";
string dead_name = tile_id + "_deadwood.tif";
string litter_name = tile_id + "_litter.tif";
string soil_name = tile_id + "_soil.tif";
string peat_name = tile_id + "_peat.tif";
string ifl_name = tile_id + "_res_ifl_2000.tif";
string plant_name = tile_id + "_res_gfw_plantations.tif";

// out files
string out_name0= "outdata/" + tile_id + "_deforestation_model.tif";
string out_name2 = "outdata/" + tile_id + "_shiftingag_model.tif";
string out_name1 = "outdata/" + tile_id + "_forestry_model.tif";
string out_name3 = "outdata/" + tile_id + "_wildfire_model.tif";
string out_name4 = "outdata/" + tile_id + "_node_totals.tif";
string out_name5 = "outdata/" + tile_id + "_disturbance_model.tif";

int x, y;
int xsize, ysize;
double GeoTransform[6]; double ulx, uly; double pixelsize;

//initialize GDAL for reading
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

//open file and get extent and projection
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

xsize=INBAND3->GetXSize(); 
ysize=INBAND3->GetYSize();
INGDAL->GetGeoTransform(GeoTransform);

ulx=GeoTransform[0]; 
uly=GeoTransform[3]; 
pixelsize=GeoTransform[1];
xsize = 2500;
ysize = 2500;
cout << xsize <<", "<< ysize <<", "<< ulx <<", "<< uly << ", "<< pixelsize << endl;

//initialize GDAL for writing
GDALDriver *OUTDRIVER;
GDALDataset *OUTGDAL;
GDALDataset *OUTGDAL2;
GDALDataset *OUTGDAL3;
GDALDataset *OUTGDAL0;
GDALDataset *OUTGDAL4;
GDALDataset *OUTGDAL5;

GDALRasterBand *OUTBAND1;
GDALRasterBand *OUTBAND2;
GDALRasterBand *OUTBAND3;
GDALRasterBand *OUTBAND0;
GDALRasterBand *OUTBAND4;
GDALRasterBand *OUTBAND5;

OGRSpatialReference oSRS;
char *OUTPRJ = NULL;
char **papszOptions = NULL;
papszOptions = CSLSetNameValue( papszOptions, "COMPRESS", "LZW" );
OUTDRIVER = GetGDALDriverManager()->GetDriverByName("GTIFF"); 
if( OUTDRIVER == NULL ) {cout << "no driver" << endl; exit( 1 );};
oSRS.SetWellKnownGeogCS( "WGS84" );
oSRS.exportToWkt( &OUTPRJ );
double adfGeoTransform[6] = { ulx, pixelsize, 0, uly, 0, -1*pixelsize };

OUTGDAL = OUTDRIVER->Create( out_name1.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL->SetGeoTransform(adfGeoTransform); OUTGDAL->SetProjection(OUTPRJ); 
OUTBAND1 = OUTGDAL->GetRasterBand(1);
OUTBAND1->SetNoDataValue(-9999);

OUTGDAL2 = OUTDRIVER->Create( out_name2.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL2->SetGeoTransform(adfGeoTransform); OUTGDAL2->SetProjection(OUTPRJ);
OUTBAND2 = OUTGDAL2->GetRasterBand(1);
OUTBAND2->SetNoDataValue(-9999);

OUTGDAL3 = OUTDRIVER->Create( out_name3.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL3->SetGeoTransform(adfGeoTransform); OUTGDAL3->SetProjection(OUTPRJ);
OUTBAND3 = OUTGDAL3->GetRasterBand(1);
OUTBAND3->SetNoDataValue(-9999);

OUTGDAL0 = OUTDRIVER->Create( out_name0.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL0->SetGeoTransform(adfGeoTransform); OUTGDAL0->SetProjection(OUTPRJ);
OUTBAND0 = OUTGDAL0->GetRasterBand(1);
OUTBAND0->SetNoDataValue(-9999);

OUTGDAL4 = OUTDRIVER->Create( out_name4.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL4->SetGeoTransform(adfGeoTransform); OUTGDAL4->SetProjection(OUTPRJ);
OUTBAND4 = OUTGDAL4->GetRasterBand(1);
OUTBAND4->SetNoDataValue(-9999);

OUTGDAL5 = OUTDRIVER->Create( out_name5.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL5->SetGeoTransform(adfGeoTransform); OUTGDAL5->SetProjection(OUTPRJ);
OUTBAND5 = OUTGDAL5->GetRasterBand(1);
OUTBAND5->SetNoDataValue(-9999);

//read/write data...
//In Data
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
//Out Data
float out_data1[xsize];
float out_data2[xsize];
float out_data3[xsize];
float out_data0[xsize];
float out_data4[xsize];
float out_data5[xsize];

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

		float outdata3 = 0;
		float outdata2 = 0;
		float outdata0 = 0;
		float outdata1 = 0;
		float outdata4 = 0;
		float outdata5 = 0;
		float outdata5_model = 0;

		// float outdata3 = -9999;
		// float outdata2 = -9999;
		// float outdata0 = -9999;
		// float outdata1 = -9999;
		// float outdata4 = -9999;

		
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
			vars = def_variables(ecozone_data[x], forestmodel_data[x], ifl_data[x], climate_data[x], plant_data[x], loss_data[x]);

			float cf = *(vars + 0);
			float c02 = *(vars + 1);
			float ch = *(vars + 2);
			float n20 = *(vars + 3);
			float peatburn = *(vars + 4);
			float peatdrain = *(vars + 5);
						
			float total_c;
			total_c = agc_data[x] + bgc_data[x] + dead_data[x] +litter_data[x];
	
			float above_below_c;
			above_below_c = agc_data[x] + bgc_data[x];
			
			float minsoil;
			
			float flu;
			
			float Biomass_tCO2e_nofire;
			float Biomass_tCO2e_yesfire;
			
			if (forestmodel_data[x] == 3) // forestry
			{
				Biomass_tCO2e_yesfire = (above_below_c * 3.67) + ((2 * above_below_c) * cf * ch * pow(10, -3) * 28) + ((2 * above_below_c) * cf * n20 * pow(10, -3) * 265);
				Biomass_tCO2e_nofire = (agc_data[x] + bgc_data[x]) * 3.67;
				flu = flu_val(climate_data[x], ecozone_data[x]);
				
				if (peat_data[x] > 0) // forestry, peat
				{
					if (burn_data[x] > 0 ) // forestry, peat, burned
					{
						outdata1 = Biomass_tCO2e_yesfire + peatdrain + peatburn;
						outdata4 = 1;
					}
					else // forestry, peat, not burned
					{
						if ((ecozone_data[x] == 2) || (ecozone_data[x] == 3))
						{
							outdata1 = Biomass_tCO2e_nofire;	
							outdata4 = 2;							
						}
						else
						{
							if (plant_data[x] > 0)
							{
								outdata1 = Biomass_tCO2e_nofire + peatdrain;
								outdata4 = 3;		
							}
							else
							{
								outdata1 = Biomass_tCO2e_nofire;
								outdata4 = 4;
							}
						}
					}
				}
				else 
				{
					if (burn_data[x] > 0) // forestry, not peat, burned
					{
						outdata1 = Biomass_tCO2e_yesfire;
						outdata4 = 5;
					}
					else // forestry, not peat, not burned
					{
						outdata1 = Biomass_tCO2e_nofire;
						outdata4 = 6;
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
						outdata0 = Biomass_tCO2e_yesfire + peatdrain + peatburn;
						outdata4 = 7;
					}
					else // deforestation, peat, not burned
					{
						outdata0 = Biomass_tCO2e_nofire + peatdrain;
						outdata4 = 8;
					}
				}
				else // deforestation, not peat
				{
					if (burn_data[x] > 0) // deforestation, not peat, burned
					{
						outdata0 = Biomass_tCO2e_yesfire + minsoil;
						outdata4 = 9;
					}
					else // deforestation, not peat, not burned
					{
						outdata0 = Biomass_tCO2e_nofire + minsoil;
						outdata4 = 10;
					}
				}	
			}
			
			else if (forestmodel_data[x] == 2) // shifting ag. only diff is flu val
			{
				Biomass_tCO2e_yesfire = (total_c * 3.67) + ((2 * total_c) * cf * ch * pow(10,-3) * 28) + ((2 * total_c) * cf * n20 * pow(10,-3) * 265);
				Biomass_tCO2e_nofire = total_c * 3.67;
				flu == 0.72;
				minsoil = soil_data[x]-(soil_data[x] * .72);
							
				if (peat_data[x] > 0) // deforestation, peat
				{
					if (burn_data[x] > 0) // deforestation, peat, burned
					{
						outdata2 = Biomass_tCO2e_yesfire + peatdrain + peatburn;
						outdata4 = 11;
					}
					else // deforestation, peat, not burned
					{
						outdata2 = Biomass_tCO2e_nofire + peatdrain;
						outdata4 = 12;
					}
				}
				else // deforestation, not peat
				{
					if (burn_data[x] > 0) // deforestation, not peat, burned
					{
						outdata2 = Biomass_tCO2e_yesfire + minsoil;
						outdata4 = 13;
					}
					else // deforestation, not peat, not burned
					{
						outdata2 = Biomass_tCO2e_nofire + minsoil;
						outdata4 = 14;
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
						outdata3 = Biomass_tCO2e_yesfire + peatdrain + peatburn;
						outdata4 = 15;
					}
					
					else // wildfire, peat, not burned
					{
						if ((ecozone_data[x] == 2) || (ecozone_data[x] == 3)) // boreal or temperate
						{
							outdata3 = Biomass_tCO2e_nofire;
							outdata4 = 16;
						}
						else // tropics
						{
							outdata3 = Biomass_tCO2e_nofire + peatdrain;
							outdata4 = 17;
						}
					}
				}
				else  // wildfire, not peat
				{
					if (burn_data[x] > 0)  // wildfire, not peat, burned
					{
						outdata3 = Biomass_tCO2e_yesfire;
						outdata4 = 18;
					}
					else  // wildfire, not peat, not burned
					{
						outdata3 = Biomass_tCO2e_nofire;
						outdata4 = 19;
					}
				}	
			}
		
		   else // no forest model data- make it no data except make disturbance model same as forestry, nancy said.
			{
				out_data1[x] = -9999;
				out_data2[x] = -9999;
				out_data0[x] = -9999;
				out_data3[x] = -9999;
				
				
				Biomass_tCO2e_yesfire = (above_below_c * 3.67) + ((2 * above_below_c) * cf * ch * pow(10, -3) * 28) + ((2 * above_below_c) * cf * n20 * pow(10, -3) * 265);
				
				Biomass_tCO2e_nofire = (agc_data[x] + bgc_data[x]) * 3.67;
				flu = flu_val(climate_data[x], ecozone_data[x]);
				
				if (peat_data[x] > 0) // no class, peat
				{
					if (burn_data[x] > 0 ) // no class, peat, burned
					{
						outdata5_model = Biomass_tCO2e_yesfire + peatdrain + peatburn;
						outdata4 = 20;
					}
					else // no class, peat, not burned
					{
						if ((ecozone_data[x] == 2) || (ecozone_data[x] == 3))
						{
							outdata5_model = Biomass_tCO2e_nofire;	
							outdata4 = 21;							
						}
						else
						{
							if (plant_data[x] > 0)
							{
								outdata5_model = Biomass_tCO2e_nofire + peatdrain;
								outdata4 = 22;		
							}
							else
							{
								outdata5_model = Biomass_tCO2e_nofire;
								outdata4 = 23;
							}
						}
					}
				}
				else 
				{
					if (burn_data[x] > 0) // no class, not peat, burned
					{
						outdata5_model = Biomass_tCO2e_yesfire;
						outdata4 = 24;
					}
					else // no class, not peat, not burned
					{
						outdata5_model = Biomass_tCO2e_nofire;
						outdata4 = 25;
					}
				}	
			}

			// write the variable to the pixel value
			if (forestmodel_data[x] == 1)
			{
				out_data1[x] = -9999;
				out_data0[x] = outdata0;
				out_data2[x] = -9999;
				out_data3[x] = -9999;
			}
			else if (forestmodel_data[x] == 2)
			{
				out_data1[x] = -9999;
				out_data0[x] = -9999;
				out_data2[x] = outdata2;
				out_data3[x] = -9999;
			}
			else if (forestmodel_data[x] == 3)
			{
				out_data1[x] = outdata1;
				out_data0[x] = -9999;
				out_data2[x] = -9999;
				out_data3[x] = -9999;
			}
			else if (forestmodel_data[x] == 4)
			{
				out_data1[x] = -9999;
				out_data0[x] = -9999;
				out_data2[x] = -9999;
				out_data3[x] = outdata3;
			}
			
			else // another else statement to handle if no forest model data
			{
				out_data1[x] = -9999;
				out_data0[x] = -9999;
				out_data2[x] = -9999;
				out_data3[x] = -9999;
			}
				// node total raster
				out_data4[x] = outdata4;
				
				
				
				// add up all outputs to make merged output
	
				outdata5 = outdata0 + outdata1 + outdata2 + outdata3 + outdata5_model;
				if ((outdata5 == 0) || (outdata5 == -9999))
				{
					out_data5[x] = -9999;
				}
				else{
					out_data5[x] = outdata5;
				}
		}	
		
		else // not on loss AND carbon
		{
			
			out_data1[x] = -9999;
			out_data2[x] = -9999;
			out_data3[x] = -9999;
			out_data0[x] = -9999;
			out_data4[x] = -9999;
			out_data5[x] = -9999;
		}
    }
OUTBAND1->RasterIO( GF_Write, 0, y, xsize, 1, out_data1, xsize, 1, GDT_Float32, 0, 0 ); 
OUTBAND2->RasterIO( GF_Write, 0, y, xsize, 1, out_data2, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND3->RasterIO( GF_Write, 0, y, xsize, 1, out_data3, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND0->RasterIO( GF_Write, 0, y, xsize, 1, out_data0, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND4->RasterIO( GF_Write, 0, y, xsize, 1, out_data4, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND5->RasterIO( GF_Write, 0, y, xsize, 1, out_data5, xsize, 1, GDT_Float32, 0, 0 );
}
GDALClose(INGDAL);
GDALClose((GDALDatasetH)OUTGDAL);
GDALClose((GDALDatasetH)OUTGDAL2);
GDALClose((GDALDatasetH)OUTGDAL3);
GDALClose((GDALDatasetH)OUTGDAL0);
GDALClose((GDALDatasetH)OUTGDAL4);
GDALClose((GDALDatasetH)OUTGDAL5);
return 0;
}
