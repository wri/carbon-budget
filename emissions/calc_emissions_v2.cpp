
//
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
#include "calc.cpp"
using namespace std;
//to compile:  c++ raster_math.cpp -o raster_math -lgdal
// ./dead_wood_c_stock.exe 00N_000E_biomass.tif 00N_000E_res_ecozone.tif 00N_000E_res_srtm.tif 00N_000E_res_srtm.tif test.tif > values.txt
// to compile on MINGW
// g++ calc_emissions_v2.cpp -o calc_emissions_v2.exe -I /usr/local/include -L /usr/local/lib -lgdal
int main(int argc, char* argv[])
{
//passing arguments
if (argc != 2){cout << "Use <program name> <tile id>" << endl; return 1;}
string agb_name=argv[1];

string tile_id = argv[1];
string forestmodel_name = tile_id + "_res_forest_model.tif";
string bgc_name = tile_id + "_bgc.tif";
string agc_name = tile_id + "_carbon.tif";
string loss_name = tile_id + "_loss.tif";
string peat_name = tile_id + "_res_peatland_drainage_proj.tif";
string burn_name = tile_id + "_res_burndate.tif";
string hist_name = tile_id + "_res_hwsd_histosoles.tif";
string ecozone_name = tile_id + "_res_fao_ecozones_bor_tem_tro.tif";
string climate_name = tile_id + "_res_climate_zone.tif";
string dead_name = tile_id + "_deadwood.tif";
string litter_name = tile_id + "_litter.tif";
string soil_name = tile_id + "_soil.tif";
string ifl_name = tile_id + "_res_ifl_2000.tif";
string cifor_name = tile_id + "_res_cifor_peat.tif";
string plant_name = tile_id + "_gfw_plantations.tif";
string jukka_name = tile_id + "_res_peatland_drainage_proj.tif";

//either parse this var from inputs or send it in
string out_name1= "outdata/" + tile_id + "_forest_model.tif";
string out_name2 = "outdata/" + tile_id + "_conversion_model.tif";
string out_name3 = "outdata/" + tile_id + "_wildfire_model.tif";
string out_name0 = "outdata/" + tile_id + "_mixed_model.tif";

//setting variables
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
GDALDataset  *INGDAL7; GDALRasterBand  *INBAND7;
GDALDataset  *INGDAL8; GDALRasterBand  *INBAND8;
GDALDataset  *INGDAL9; GDALRasterBand  *INBAND9;
GDALDataset  *INGDAL10; GDALRasterBand  *INBAND10;
GDALDataset  *INGDAL11; GDALRasterBand  *INBAND11;
GDALDataset  *INGDAL12; GDALRasterBand  *INBAND12;
GDALDataset  *INGDAL13; GDALRasterBand  *INBAND13;
GDALDataset  *INGDAL14; GDALRasterBand  *INBAND14;
GDALDataset  *INGDAL15; GDALRasterBand  *INBAND15;
GDALDataset  *INGDAL16; GDALRasterBand  *INBAND16;

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

INGDAL7 = (GDALDataset *) GDALOpen(hist_name.c_str(), GA_ReadOnly );
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

INGDAL13 = (GDALDataset *) GDALOpen(ifl_name.c_str(), GA_ReadOnly );
INBAND13 = INGDAL13->GetRasterBand(1);

INGDAL14 = (GDALDataset *) GDALOpen(cifor_name.c_str(), GA_ReadOnly );
INBAND14 = INGDAL14->GetRasterBand(1);

INGDAL15 = (GDALDataset *) GDALOpen(plant_name.c_str(), GA_ReadOnly );
INBAND15 = INGDAL15->GetRasterBand(1);

INGDAL16 = (GDALDataset *) GDALOpen(jukka_name.c_str(), GA_ReadOnly );
INBAND16 = INGDAL16->GetRasterBand(1);

xsize=INBAND3->GetXSize(); 
ysize=INBAND3->GetYSize();
INGDAL->GetGeoTransform(GeoTransform);

ulx=GeoTransform[0]; 
uly=GeoTransform[3]; 
pixelsize=GeoTransform[1];
cout << xsize <<", "<< ysize <<", "<< ulx <<", "<< uly << ", "<< pixelsize << endl;

//initialize GDAL for writing
GDALDriver *OUTDRIVER;
GDALDataset *OUTGDAL;
GDALDataset *OUTGDAL2;
GDALDataset *OUTGDAL3;
GDALDataset *OUTGDAL0;

GDALRasterBand *OUTBAND1;
GDALRasterBand *OUTBAND2;
GDALRasterBand *OUTBAND3;
GDALRasterBand *OUTBAND0;

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



//read/write data
float agb_data[xsize];
float agc_data[xsize];
float bgc_data[xsize];
float loss_data[xsize];
float peat_data[xsize];
float forestmodel_data[xsize];
float burn_data[xsize];
float hist_data[xsize];
float ecozone_data[xsize];
float climate_data[xsize];
float dead_data[xsize];
float litter_data[xsize];
float soil_data[xsize];
float ifl_data[xsize];
float cifor_data[xsize];
float plant_data[xsize];
float jukka_data[xsize];

float out_data1[xsize];
float out_data2[xsize];
float out_data3[xsize];
float out_data0[xsize];

for (y=0; y<ysize; y++) 
{
INBAND->RasterIO(GF_Read, 0, y, xsize, 1, agc_data, xsize, 1, GDT_Float32, 0, 0);
INBAND2->RasterIO(GF_Read, 0, y, xsize, 1, bgc_data, xsize, 1, GDT_Float32, 0, 0);
INBAND3->RasterIO(GF_Read, 0, y, xsize, 1, forestmodel_data, xsize, 1, GDT_Float32, 0, 0);
INBAND4->RasterIO(GF_Read, 0, y, xsize, 1, loss_data, xsize, 1, GDT_Float32, 0, 0);
INBAND5->RasterIO(GF_Read, 0, y, xsize, 1, peat_data, xsize, 1, GDT_Float32, 0, 0);
INBAND6->RasterIO(GF_Read, 0, y, xsize, 1, burn_data, xsize, 1, GDT_Float32, 0, 0);
INBAND7->RasterIO(GF_Read, 0, y, xsize, 1, hist_data, xsize, 1, GDT_Float32, 0, 0);
INBAND8->RasterIO(GF_Read, 0, y, xsize, 1, ecozone_data, xsize, 1, GDT_Float32, 0, 0);
INBAND9->RasterIO(GF_Read, 0, y, xsize, 1, climate_data, xsize, 1, GDT_Float32, 0, 0);
INBAND10->RasterIO(GF_Read, 0, y, xsize, 1, dead_data, xsize, 1, GDT_Float32, 0, 0);
INBAND11->RasterIO(GF_Read, 0, y, xsize, 1, litter_data, xsize, 1, GDT_Float32, 0, 0);
INBAND12->RasterIO(GF_Read, 0, y, xsize, 1, soil_data, xsize, 1, GDT_Float32, 0, 0);
INBAND13->RasterIO(GF_Read, 0, y, xsize, 1, ifl_data, xsize, 1, GDT_Float32, 0, 0);
INBAND14->RasterIO(GF_Read, 0, y, xsize, 1, cifor_data, xsize, 1, GDT_Float32, 0, 0);
INBAND15->RasterIO(GF_Read, 0, y, xsize, 1, plant_data, xsize, 1, GDT_Float32, 0, 0);
INBAND16->RasterIO(GF_Read, 0, y, xsize, 1, jukka_data, xsize, 1, GDT_Float32, 0, 0);

for(x=0; x<xsize; x++)
	{
		float outdata3 = -9999;
		float outdata2 = -9999;
		float outdata0 = -9999;
		float outdata1 = -9999;
		
		if (loss_data[x] > 0 && agc_data[x] > 0) // on loss AND carbon
		{
			int peat_drn_ann;
			peat_drn_ann = peat_drn_ann_calc(forestmodel_data[x], plant_data[x]);
			float flu_val;
			flu_val = flu(climate_data[x], ecozone_data[x]);

			int peat_val;

			if ((forestmodel_data[x] == 1)||(forestmodel_data[x] == 0))   // forestry or mixed
			{
				out_data2[x] = -9999;
				out_data3[x] = -9999;
				out_data0[x] = -9999;
				
				if ((peat_data[x] > 0) || (hist_data[x] > 0) || (cifor_data[x] > 0)) // forestry, peat
				{
					peat_val=1;

					if (burn_data[x] == 1 ) // forestry, peat, burned
					{
						if (ecozone_data[x] == 3) // forestry, peat, burned, temperate
						{
							outdata1 =  ((agc_data[x] + bgc_data[x]) * 3.67) + ((2*(agc_data[x]+bgc_data[x])) *0.62 * 4.7 * pow(10, -3) * 28) + ((2*(agc_data[x] + bgc_data[x])) * 0.62 *0.26 * pow(10, -3) * 265)+(15 - loss_data[x])*12+104;
						}
						else if (ecozone_data[x] == 2) // forestry, peat, burned, boreal
						{
							outdata1 =  ((agc_data[x] + bgc_data[x]) * 3.67) + ((2*(agc_data[x]+bgc_data[x])) *0.33 * 4.7 * pow(10, -3) * 28) + ((2*(agc_data[x] + bgc_data[x])) * 0.33 *0.26 * pow(10, -3) * 265)+(15 - loss_data[x])*3+104;
						}
						else if (ecozone_data[x] == 1) // forestry, peat, burned, tropic
						{
							if (ifl_data[x] != 0) // forestry, peat, burned, tropic, ifl
							{
								outdata1 =  ((agc_data[x] + bgc_data[x]) * 3.67) + ((2*(agc_data[x]+bgc_data[x])) *0.36 * 6.8 * pow(10, -3) * 28) + ((2*(agc_data[x] + bgc_data[x])) * 0.36 *0.2 * pow(10, -3) * 265)+(15 - loss_data[x])* peat_drn_ann + 355;
							}
							else // forestry, peat, burned, tropic, not ifl
							{
								 outdata1 = ((agc_data[x] + bgc_data[x]) * 3.67) + ((2*(agc_data[x]+bgc_data[x])) *0.55 * 6.8 * pow(10, -3) * 28) + ((2*(agc_data[x] + bgc_data[x])) * 0.55 *0.2 * pow(10, -3) * 265)+(15 - loss_data[x])* peat_drn_ann +355;
							}
						}
					}
					else // forestry, peat, not burned
					{
						if (ecozone_data[x] == 3) // forestry, peat, not burned, temperate
						{
							outdata1 = (agc_data[x] + bgc_data[x]) * 3.67+(15 - loss_data[x])*12;
						}
						else if (ecozone_data[x] == 2) // forestry, peat, not burned, boreal
						{
							outdata1 = (agc_data[x] + bgc_data[x]) * 3.67+(15 - loss_data[x])*3;
						}
						else if (ecozone_data[x] == 1) // forestry, peat, not burned, tropic
						{
							if (plant_data[x] == 0 ) // forestry, peat, not burned, tropic, not plantation
							{
								outdata1 = (agc_data[x] + bgc_data[x]) * 3.67;
							}
							else // forestry, peat, not burned, tropic, plantation
							{
								outdata1 = (agc_data[x] + bgc_data[x]) * 3.67+(15 - loss_data[x])* peat_drn_ann;										
							}
						}
						else // no ecozone
						{
							outdata1 = 0;
						}
					}
				}
				else 
				{
					peat_val = 0;
					if (burn_data[x] == 1) // forestry, not peat, burned
					{
						if (ecozone_data[x] == 3) // forestry, not peat, burned, temperate
						{
							outdata1 =  ((agc_data[x] + bgc_data[x]) * 3.67) + ((2*(agc_data[x]+bgc_data[x])) *0.62 * 4.7 * pow(10, -3) * 28) + ((2*(agc_data[x] + bgc_data[x])) * 0.62 *0.26 * pow(10, -3) * 265);
						}
						else if (ecozone_data[x] == 2) // forestry, not peat, burned, boreal
						{
							outdata1 =  ((agc_data[x] + bgc_data[x]) * 3.67) + ((2*(agc_data[x]+bgc_data[x])) *0.33 * 4.7 * pow(10, -3) * 28) + ((2*(agc_data[x] + bgc_data[x])) * 0.33 *0.26 * pow(10, -3) * 265);
						}
						else if (ecozone_data[x] == 1) // forestry, not peat, burned, tropics
						{
							if (ifl_data[x] != 0) // forestry, not peat, burned, tropics, ifl
							{
								outdata1 =  ((agc_data[x] + bgc_data[x]) * 3.67) + ((2*(agc_data[x]+bgc_data[x])) *0.36 * 6.8 * pow(10, -3) * 28) + ((2*(agc_data[x] + bgc_data[x])) * 0.36 *0.2 * pow(10, -3) * 265);
							}
							else  // forestry, not peat, burned, tropics, not ifl
							{
								outdata1 =  ((agc_data[x] + bgc_data[x]) * 3.67) + ((2*(agc_data[x]+bgc_data[x])) *0.55 * 6.8 * pow(10, -3) * 28) + ((2*(agc_data[x] + bgc_data[x])) * 0.55 *0.2 * pow(10, -3) * 265);
							}
						}
						else  // forestry, not peat, burned, no-ecozone
						{
							outdata1 = 0;
						}
					}
					else // forestry, not peat, not burned
					{
						outdata1 = (agc_data[x] + bgc_data[x]) * 3.67;
					}
				}	
				// set either forest model or mixed raster to the value
				if (forestmodel_data[x] == 1)
				{					
					out_data1[x] = outdata1;
					out_data0[x] = -9999;
				}
				else if (forestmodel_data[x] == 0)
				{
					out_data1[x] = -9999;			
					out_data0[x] = outdata1;
				}
				else
				{
					out_data1[x] = -9999;
					out_data0[x] = -9999;
				}
			}
		   else if ((forestmodel_data[x] == 2) || (forestmodel_data[x] == 0)) // conversion or mixed
			{
				
				if ((peat_data[x] > 0) || (hist_data[x] > 0) || (cifor_data[x] > 0)) // conversion, peat
				{
					if (burn_data[x] == 1) // conversion, peat, burned
					{
						if (ecozone_data[x] == 3) // conversion, peat, burned, temperate
						{
							outdata2 = ((((agc_data[x] +bgc_data[x])*(1-0.55)) + dead_data[x] +litter_data[x])* 3.67) + (((2*agc_data[x]+bgc_data[x])) *0.55 *1580 * pow(10,-3)) + (((2*(agc_data[x]+bgc_data[x])) *0.55 * 6.8 * pow(10,-3) * 28) + (((2*(agc_data[x] + bgc_data[x])) *0.55 *0.2 * pow(10,-3) * 265)));
						}
						else if (ecozone_data[x] == 2) // conversion, peat, burned, boreal
						{
							outdata2 =  ((((agc_data[x] + bgc_data[x])*(1-0.59)) + dead_data[x] + litter_data[x])* 3.67) + (((2*agc_data[x]+bgc_data[x])) *0.59 *1569 * pow(10,-3)) + (((2*(agc_data[x]+bgc_data[x])) *0.59 * 4.7 * pow(10,-3) * 28) + (((2*(agc_data[x] + bgc_data[x])) *0.59 *0.26 * pow(10,-3) * 265)))+(15 - loss_data[x])*36+104;
						}
						else if (ecozone_data[x] == 1) // conversion, peat, burned, tropic
						{
							if (ifl_data[x] != 0) //  conversion, peat, burned, tropics, ifl
							{
								outdata2 =  ((((agc_data[x] + bgc_data[x])*(1-0.36)) + dead_data[x] +litter_data[x])* 3.67) + (((2*agc_data[x]+bgc_data[x])) *0.36 *1580 * pow(10,-3)) + (((2*(agc_data[x]+bgc_data[x])) *0.36 * 6.8 * pow(10,-3) * 28) + (((2*(agc_data[x] +bgc_data[x])) *0.36 *0.2 *pow(10,-3) * 265)))+(15 - loss_data[x])*peat_drn_ann+355;
							}
							else // conversion, peat, burned, tropics, not ifl
							{
								 outdata2 = ((((agc_data[x] +bgc_data[x])*(1-0.55)) + dead_data[x] +litter_data[x])* 3.67) + (((2*agc_data[x]+bgc_data[x])) *0.55 *1580 * pow(10,-3)) + (((2*(agc_data[x]+bgc_data[x])) *0.55 * 6.8 * pow(10,-3) * 28) + (((2*(agc_data[x] + bgc_data[x])) *0.55 *0.2 * pow(10,-3) * 265)))+(15 - loss_data[x])*peat_drn_ann+355;
							}
						}
					}
					else // conversion, peat, not burned
					{
						if (ecozone_data[x] == 3) // peat not burned temperate
						{

							//cout << "peat, not burned, temperate";
							outdata2 = (agc_data[x] +bgc_data[x] +dead_data[x] +litter_data[x]) * 3.67+(15 - loss_data[x])*peat_drn_ann;
										

						}
						else if (ecozone_data[x] == 2) // peat not burned boreal
						{
							// peat, not burned, boreal
							//cout << "peat, not burned, boreal";
							outdata2 = (agc_data[x] + bgc_data[x] + dead_data[x] + litter_data[x]) * 3.67+(15 - loss_data[x])*peat_drn_ann;

						}
						else if (ecozone_data[x] == 1) // peat, not burned, tropic
						{
							if (plant_data[x] == 0 ) // peat, not burned, tropic, not on plantations (need to take off loss yr calc)
							{
								outdata2 = (agc_data[x] + bgc_data[x] + dead_data[x] + litter_data[x]) * 3.67;				


							}
							else
							{
								//cout << "peat, not burned, tropics, plantations";
								// peat, not burned, tropics, plantations
								outdata2 = (agc_data[x] + bgc_data[x]) * 3.67+(15 - loss_data[x])* peat_drn_ann;										
							}

						}
						else // peat, not burned, no ecozone
						{
							outdata2 = 0;
							
						}
					}
				}

				else // conversion, not peat
				{
					if (burn_data[x] == 1) // not peat burned
					{
						if (ecozone_data[x] == 3) // not peat burned temperate
						{

							outdata2 = ((((agc_data[x] + bgc_data[x])*(1-0.51)) + dead_data[x] + litter_data[x])* 3.67) + (((2*agc_data[x]+bgc_data[x])) *0.51 *1569 * pow(10, -3)) + ((2*(agc_data[x]+bgc_data[x])) * 0.51 * 4.7 * pow(10, -3)* 28) + ((2*(agc_data[x] +bgc_data[x])) *0.51 *0.26 * pow(10,-3) * 265)+(soil_data[x]-(soil_data[x] * flu_val));

						}
						else if (ecozone_data[x] == 2) // not peat burned boreal
						{
							outdata2 =  ((((agc_data[x] + bgc_data[x])*(1-0.59)) + dead_data[x] + litter_data[x])* 3.67) + (((2*agc_data[x]+bgc_data[x])) *0.59 *1569 * pow(10,-3) + (((2*(agc_data[x]+bgc_data[x])) *0.59 * 4.7 * pow(10,-3) * 28) + (((2*(agc_data[x] + bgc_data[x])) *0.59 *0.26 * pow(10,-3) * 265)))+(soil_data[x]-(soil_data[x] * flu_val)));

						}
						else if (ecozone_data[x] == 1) // not peat burned tropics
						{
							if (ifl_data[x] != 0) // not peat burned tropics ifl
							{
								outdata2 =  ((((agc_data[x] + bgc_data[x])*(1-0.59)) + dead_data[x] + litter_data[x])* 3.67) + (((2*agc_data[x]+bgc_data[x])) *0.59 *1569 * pow(10,-3) + (((2*(agc_data[x]+bgc_data[x])) *0.59 * 4.7 * pow(10,-3) * 28) + (((2*(agc_data[x] + bgc_data[x])) *0.59 *0.26 * pow(10,-3) * 265)))+(soil_data[x]-(soil_data[x] * flu_val)));

							}
							else  // not peat burned tropics ifl not ifl
							{
								outdata2 =  ((((agc_data[x] +bgc_data[x])*(1-0.55)) + dead_data[x] +litter_data[x])* 3.67) + (((2*agc_data[x]+bgc_data[x])) *0.55 *1580 * pow(10,-3)) + (((2*(agc_data[x]+bgc_data[x])) *0.55 * 6.8 * pow(10,-3) * 28) + (((2*(agc_data[x] + bgc_data[x])) *0.55 *0.2 * pow(10,-3) * 265)))+(soil_data[x]-(soil_data[x] * flu_val));
							}
						}
						else  // not peat burned no-ecozone
						{
							outdata2 = 0;
						}
						
					}
					
					else // not peat not burned
					{
						outdata2 = (agc_data[x] +bgc_data[x] +dead_data[x] +litter_data[x]) * 3.67+(soil_data[x]-(soil_data[x] * flu_val));
					}
				}	
					
				// set either forest model or mixed raster to the value
				if (forestmodel_data[x] == 2)
				{					
					out_data2[x] = outdata2;
					out_data0[x] = -9999;
				}
				else if (forestmodel_data[x] == 0)
				{
					out_data2[x] = -9999;			
					out_data0[x] = outdata2;
				}
				else
				{
					out_data2[x] = -9999;
					out_data0[x] = -9999;
				}

			}
		   else if ((forestmodel_data[x] == 3) || (forestmodel_data[x] == 0))// wildfire or mixed
			{
				if ((peat_data[x] > 0) || (hist_data[x] > 0) || (cifor_data[x] > 0)) // wildfire, peat
				{
					if (burn_data[x] == 1) // wildfire, peat, burned
					{
						if (ecozone_data[x] == 3) // wildfire, peat, burned, temperate
						{
							outdata3 = ((2*agc_data[x]+bgc_data[x])) *0.51 *1569 * pow(10,-3) + (((2*(agc_data[x]+bgc_data[x])) *0.51 * 4.7 *pow(10,-3 * 28)+ (((2*(agc_data[x] + bgc_data[x])) *0.51 *0.26 * pow(10,-3) * 265)+(15 - loss_data[x])*12+104)));
						}
						else if (ecozone_data[x] == 2) // wildfire, peat, burned, boreal
						{
							outdata3 =  ((2*agc_data[x]+bgc_data[x])) *0.59 *1569 * pow(10,-3) + (((2*(agc_data[x]+bgc_data[x])) *0.59 * 4.7 *pow(10,-3) * 28)+ (((2*(agc_data[x] + bgc_data[x])) *0.59 *0.26 * pow(10,-3) * 265)+(15-loss_data[x])*3+104));

						}
						else if (ecozone_data[x] == 1) // wildfire, peat, burned, tropic
						{
							if (ifl_data[x] != 0) //  wildfire, peat, burned, tropics, ifl
							{
								outdata3 =  (((2*agc_data[x]+bgc_data[x])) *0.36 *1580 * pow(10,-3) + (((2*(agc_data[x]+bgc_data[x])) *0.36 * 6.8 *pow(10,-3) * 28)+ (((2*(agc_data[x] + bgc_data[x])) *0.36 *0.2 * pow(10,-3) * 265)+(15 - loss_data[x])*peat_drn_ann+808)));
							}
							else // wildfire, peat, burned, tropics, not ifl
							{
								 outdata3 = (((2*agc_data[x]+bgc_data[x])) *0.55 *1580 * pow(10,-3) + (((2*(agc_data[x]+bgc_data[x])) *0.55 * 6.8 *pow(10,-3 * 28)+ (((2*(agc_data[x] + bgc_data[x])) *0.55 *0.2 * pow(10,-3) * 265)+(15 - loss_data[x])*peat_drn_ann+808))));
							}
						}
					}
					else // wildfire, peat, not burned
					{
						if (ecozone_data[x] == 3) // wildfire, peat, not burned, temperate
						{
							outdata3 = ((agc_data[x] + bgc_data[x]) * 3.67)+(15 - loss_data[x])*12;
						}
						else if (ecozone_data[x] == 2) // wildfire, peat, not burned, boreal
						{
							outdata3 = ((agc_data[x] + bgc_data[x]) * 3.67)+(15 - loss_data[x])*3;
						}
						else if (ecozone_data[x] == 1) // wildfire, peat, not burned, tropic
						{
							if (plant_data[x] == 0 ) // wildfire, peat, not burned, tropic, no plantation
							{
								outdata3 = ((agc_data[x] + bgc_data[x]) * 3.67);		
							}
							else  // wildfire, peat, not burned, tropic, plantation
							{
								outdata3 = ((agc_data[x] + bgc_data[x]) * 3.67)+(15 - loss_data[x])*peat_drn_ann;
							}

						}
						else  // wildfire, peat, not burned, no ecozone
						{
							outdata3 = 0;
						}
					}
				}

				else  // wildfire, not peat
				{
					if (burn_data[x] == 1)  // wildfire, not peat, burned
					{
						if (ecozone_data[x] == 3)  // wildfire, not peat, burned, temperate
						{
							outdata3 = ((2*agc_data[x]+bgc_data[x])) *0.51 *1569 * pow(10,-3) + (((2*(agc_data[x]+bgc_data[x])) *0.51 * 4.7 *pow(10,-3) * 28)+ (((2*(agc_data[x] + bgc_data[x])) *0.51 *0.26 * pow(10,-3) * 265)));
						}
						else if (ecozone_data[x] == 2)  // wildfire, not peat, burned, boreal
						{
							outdata3 = ((2*agc_data[x]+bgc_data[x])) *0.59 *1569 * pow(10,-3) + (((2*(agc_data[x]+bgc_data[x])) *0.59 * 4.7 *pow(10,-3) * 28)+ (((2*(agc_data[x] + bgc_data[x])) *0.59 *0.26 * pow(10,-3) * 265)));

						}
						else if (ecozone_data[x] == 1)  // wildfire, not peat, burned, tropics
						{
							if (ifl_data[x] != 0)  // wildfire, not peat, burned, tropics, ifl
							{
								outdata3 =  ((2*agc_data[x]+bgc_data[x])) *0.55 *1580 * pow(10,-3) + (((2*(agc_data[x]+bgc_data[x])) *0.55 * 6.8 *pow(10,-3) * 28)+ (((2*(agc_data[x] + bgc_data[x])) *0.55 *0.2 * pow(10,-3) * 265)));
							}
							else  // wildfire, not peat, burned, tropics, not ifl
							{
								outdata3 =  ((2*agc_data[x]+bgc_data[x])) *0.55 *1580 * pow(10,-3) + (((2*(agc_data[x]+bgc_data[x])) *0.55 * 6.8 *pow(10,-3) * 28)+ (((2*(agc_data[x] + bgc_data[x])) *0.55 *0.2 * pow(10,-3) * 265)));
							}
						}
						else  // wildfire, not peat, burned, no-ecozone
						{
							outdata3 = 0;
						}
					}
					
					else  // wildfire, not peat, not burned
					{
						outdata3 = ((agc_data[x] + bgc_data[x]) * 3.67);
					}
				}	
					
				// set either forest model or mixed raster to the value
				if (forestmodel_data[x] == 2)
				{					
					out_data2[x] = outdata2;
					out_data0[x] = -9999;
				}
				else if (forestmodel_data[x] == 0)
				{
					out_data2[x] = -9999;			
					out_data0[x] = outdata2;
				}
				else
				{
					out_data2[x] = -9999;
					out_data0[x] = -9999;
				}

			}
		   else // forest model not 1 or 2 or 3
			{
				out_data1[x] = -9999;
				out_data2[x] = -9999;
				out_data0[x] = -9999;
				out_data3[x] = -9999;
			}

			if (forestmodel_data[x] == 0)
			{
				out_data0[x] = ((float(outdata1)*float(.42)) + (float(outdata2)*float(.42)) + (float(outdata3)*float(.16)));
				out_data1[x] = -9999;
				out_data2[x] = -9999;
				out_data3[x] = -9999;
			}
			else if (forestmodel_data[x] == 1)
			{
				out_data1[x] = outdata1;
				out_data0[x] = -9999;
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
				out_data1[x] = -9999;
				out_data0[x] = -9999;
				out_data2[x] = -9999;
				out_data3[x] = outdata3;
			}
			else 
			{
				out_data1[x] = -9999;
				out_data0[x] = -9999;
				out_data2[x] = -9999;
				out_data3[x] = -9999;
			}

		cout << " \n forest model: " << forestmodel_data[x] << " peat val: " << peat_val <<" burn: " << burn_data[x] << " eco zone: " << ecozone_data[x] << " ifl: " << ifl_data[x]<< " above ground: " << agc_data[x] << " below ground: " << bgc_data[x] << " soil: " << soil_data[x] << " dead: " << dead_data[x] << " litter: " << litter_data[x] << " flu: "  << flu_val << " peat drain: " << peat_drn_ann << " plantation data: " << plant_data[x] << " lossyr: " << loss_data[x] << " climate: " << climate_data[x] << " outdata1: " << out_data1[x] << " outdata2: " << out_data2[x] << " outdata3: " << out_data3[x];
			
			
		}
		else // not on loss AND carbon
		{
			out_data1[x] = -9999;
		    out_data2[x] = -9999;
			out_data3[x] = -9999;
			out_data0[x] = -9999;
		}



    }


OUTBAND1->RasterIO( GF_Write, 0, y, xsize, 1, out_data1, xsize, 1, GDT_Float32, 0, 0 ); 
OUTBAND2->RasterIO( GF_Write, 0, y, xsize, 1, out_data2, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND3->RasterIO( GF_Write, 0, y, xsize, 1, out_data3, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND0->RasterIO( GF_Write, 0, y, xsize, 1, out_data0, xsize, 1, GDT_Float32, 0, 0 );
//closes for y loop
}

//close GDAL
GDALClose(INGDAL);
GDALClose((GDALDatasetH)OUTGDAL);
GDALClose((GDALDatasetH)OUTGDAL2);
GDALClose((GDALDatasetH)OUTGDAL3);
GDALClose((GDALDatasetH)OUTGDAL0);
return 0;
}
