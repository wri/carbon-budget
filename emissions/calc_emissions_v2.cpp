
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
#include <gdal/gdal_priv.h>
#include <gdal/cpl_conv.h>
#include <gdal/ogr_spatialref.h>
using namespace std;
//to compile:  c++ raster_math.cpp -o raster_math -lgdal
// ./dead_wood_c_stock.exe 00N_000E_biomass.tif 00N_000E_res_ecozone.tif 00N_000E_res_srtm.tif 00N_000E_res_srtm.tif test.tif > values.txt

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
string burn_name = tile_id + "_res_peatland_drainage_proj.tif";
string hist_name = tile_id + "_res_hwsd_histosoles.tif";
string ecozone_name = tile_id + "_res_fao_ecozones_bor_tem_tro.tif";
string climate_name = tile_id + "_res_climate_zone.tif";
string dead_name = tile_id + "_deadwood.tif";
string litter_name = tile_id + "_litter.tif";
string soil_name = tile_id + "_soil.tif";
string ifl_name = tile_id + "_res_ifl_2000.tif";

//either parse this var from inputs or send it in
string out_name1= tile_id + "_forest_model.tif";
string out_name2 = tile_id + "_conversion_model.tif";
string out_name3 = tile_id + "_wildfire_model.tif";


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

GDALRasterBand *OUTBAND1;
GDALRasterBand *OUTBAND2;
GDALRasterBand *OUTBAND3;

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

float out_data1[xsize];
float out_data2[xsize];
float out_data3[xsize];
//for (y=36800; y<36900; y++) {
//for (y=0; y<ysize; y++) {
for (y=23919; y<23925; y++) {
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


for(x=0; x<xsize; x++)
	{
		if (loss_data[x] > 0)
		{
	//		cout << "\n in the loss data pixels";
	//            if (agc_data[x] != -9999)

			if (agc_data[x] > 0)
				{
					cout << "\n above ground data is not -9999: " << agc_data[x];
					
				   if (forestmodel_data[x] == 1)   // forestry
					{
						out_data2[x] = -9999;
						out_data3[x] = -9999;
						cout << " forest model is 1: ";
						if (peat_data[x] != 0) // if its on peat data
						{
							cout << "on peat: ";
							if (burn_data[x] != 0) // if its on peat and on burn data
							{
							cout << "on burn: ";
								out_data1[x] = ((agc_data[x] + bgc_data[x]) * 3.67) + (15 - loss_data[x]) * peat_data[x] + 917;
							}

							else // on peat but not on burn data
							{
							cout << "not on burn: ";
								out_data1[x] = ((agc_data[x] + bgc_data[x]) * 3.67) + (15 - loss_data[x]) * peat_data[x];
							}
						}

						else // not on peat
						{
						cout << "not on peat, ";
							if (hist_data[x] != 0) // not on peat but is on histosoles
							{
							cout << "on hist, ";
								if (ecozone_data[x] = 1)
								{
								cout << "ecozone 1, ";
									out_data1[x] = ((agc_data[x] + bgc_data[x]) * 3.67) + ((15 - loss_data[x]) * 55);
								}
								else if (ecozone_data[x] = 2)
								{
									out_data1[x] = ((agc_data[x] + bgc_data[x]) * 3.67) + ((15 - loss_data[x]) * 2.16);
								}
								else if (ecozone_data[x] = 3)
								{
									out_data1[x] = ((agc_data[x] + bgc_data[x]) * 3.67) + ((15 - loss_data[x]) * 6.27);
								}
								else
								{
								cout << "no ecozone, ";
									out_data1[x] = -9999;
								}
							}

							else  //not on peat and not on histosole
							{
							cout << "not on hist, ";
								out_data1[x] = (agc_data[x] + bgc_data[x]) * 3.67;
							}

						}

					}

				   else if (forestmodel_data[x] == 2) // conversion
					{
						out_data1[x] = -9999;
						out_data3[x] = -9999;
	//				cout << "forest model is 2: ";
						if (peat_data[x] != 0) // if its on peat data
						{
	//					cout << "peat data yes, " ;
							//if ((loss_data[x] -1) <= burn_data[x] <= loss_data[x]) // if its on peat and on burn data within 1 year of loss year
							if (burn_data[x] != 0)
							{
	//						cout << "burn data yes, ";
								out_data2[x] = ((agc_data[x] + bgc_data[x] + dead_data[x] + litter_data[x]) -5) * 3.67 + (15 - loss_data[x]) * peat_data[x] + 917;
							}
							else //if its on peat and NOT on burn data within 1 year of loss year
							{
	//						cout << "burn data no, ";
								out_data2[x] = ((agc_data[x] + bgc_data[x] + dead_data[x] + litter_data[x]) -5) * 3.67 + (15 - loss_data[x]) * peat_data[x];
							}
						}

						else // its NOT on peat data
						{
	//					cout << "peat data no, ";
							if (hist_data[x] != 0) // not on peat but is on histosoles
							{
	//						cout << "hist data yes, ";
								if ((ecozone_data[x] = 2) || (ecozone_data[x] = 3)) // boreal or temperate
								{
	//							cout << "boreal, ";
									out_data2[x] = (((agc_data[x] + bgc_data[x] + dead_data[x] + litter_data[x]) -5) * 3.67) + 29;
								}
								else if (ecozone_data[x] = 1) // tropics
								{
	  //               					cout << "tropics, ";
									out_data2[x] = (((agc_data[x] + bgc_data[x] + dead_data[x] + litter_data[x]) -5) * 3.67) + 55;
								}
								else // no data for ecozone
								{
	//							cout << "no data for ecozone, ";
									out_data2[x] = -9999;
								}
							}
							else // not on peat and NOT on histosole
							{
	//						cout << "hist data no, ";

								if ((climate_data[x]!= 0) && (soil_data[x] > 0))
								{
	//						cout << "climate data not 0, ";
									if ((climate_data[x] = 2) || (climate_data[x] = 4) || (climate_data[x] = 8)) // warm/cool temperate/boreal dry

									{
										out_data2[x] = (((agc_data[x] + bgc_data[x] + dead_data[x] + litter_data[x]) -5) * 3.67) + (soil_data[x] - (soil_data[x] * .8)) * 3.67;
									}

									else if ((climate_data[x] = 1) || (climate_data[x] = 3) || (climate_data[x] = 7)) // warm/cool temperate/boreal moist
									{
										out_data2[x] = (((agc_data[x] + bgc_data[x] + dead_data[x] + litter_data[x]) -5) * 3.67) + (soil_data[x] - (soil_data[x] * .69)) * 3.67;
									}

									else if (climate_data[x] = 12) // tropical dry
									{
										out_data2[x] = (((agc_data[x] + bgc_data[x] + dead_data[x] + litter_data[x]) -5) * 3.67) + (soil_data[x] - (soil_data[x] * .58)) * 3.67;
									}

									else if ((climate_data[x] = 10) || (climate_data[x] = 11)) // tropical moist/wet
									{
										out_data2[x] = (((agc_data[x] + bgc_data[x] + dead_data[x] + litter_data[x]) -5) * 3.67) + (soil_data[x] - (soil_data[x] * .48)) * 3.67;
										cout << climate_data[x] << " " << out_data2[x] << " " << x << " " << y;
									}

									else if (climate_data[x] = 9) // tropical tropical montane
									{
										out_data2[x] = (((agc_data[x] + bgc_data[x] + dead_data[x] + litter_data[x]) -5) * 3.67) + (soil_data[x] - (soil_data[x] * .64)) * 3.67;
									}
											
								}
								else
								{
	//						cout << "climate data 0, ";
								out_data2[x] = -9999;
	//						cout << "out data is: " << out_data2[x];
								}
							}
						}
					}
				
				   else if (forestmodel_data[x] == 3) // wildfire
				   {
						out_data1[x] = -9999;
						out_data2[x] = -9999;
						
						float a_var = (agc_data[x] + bgc_data[x]) * 2;
						float tropics_ifl_biomass = ((a_var * .36 * (1580/1000)) + (a_var * .36 * (6.8/1000) * 28) + ((a_var * .36 * .2 / 1000) * 265));
						float tropics_notifl_biomass = (a_var * .55 * (1580/1000)) + (a_var * .55 * (6.8/1000)) + (a_var * .55 * .2/1000);
						float boreal_biomass = (a_var * .59 * (1569/1000)) + (a_var * .59 * (4.7/1000)) + (a_var * .59 * .26/1000);
						float temperate_biomass =(a_var * .51 * (1569/1000)) + (a_var * .51 * (4.7/1000)) + (a_var * .51 * .26/1000);
						
						float peat_emiss = (15 - loss_data[x] * peat_data[x]) + 917;
						
						float tropics_drainage = (15 - loss_data[x]) * 55;
						float boreal_drainage = (15 - loss_data[x]) * 2.16;
						float temperate_drainage = (15 - loss_data[x]) * 6.27;
						
						if ((ecozone_data[x] = 1) && (ifl_data[x] = 1)) // tropics and IFL
						{
							if (peat_data[x] != 0) // on peat
							{
								out_data3[x] = tropics_ifl_biomass + tropics_drainage + 917;
							}
							else // not on peat
							{
								out_data3[x] = tropics_ifl_biomass + tropics_drainage;
							}
						}
						else if ((ecozone_data[x] = 1) && (ifl_data[x] != 1)) // tropics and not IFL
						{
							if (peat_data[x] != 0) // on peat
							{
								out_data3[x] = tropics_notifl_biomass + tropics_drainage + 917;
							}
							
							else // not on peat
							{
								out_data3[x] = tropics_notifl_biomass + tropics_drainage;
							}	
						}
						else if (ecozone_data[x] = 2) // boreal
						{
							if (peat_data[x] != 0) // on peat
							{
								out_data3[x] = boreal_biomass + boreal_drainage + 917;
							}
							else // not on peat
							{
								out_data3[x] = boreal_biomass + boreal_drainage;
							}	
						}
						else if (ecozone_data[x] = 3) // temperate
						{
							if (peat_data[x] != 0) // on peat
							{
								out_data3[x] = temperate_biomass + temperate_drainage + 917;
							}
							else // not on peat
							{
								out_data3[x] = temperate_biomass + temperate_drainage;
							}	
						}
						else
						{
							out_data3[x] = -9999;
						}
					}

				   else // forest model not 1 or 2 or 3
					{
						out_data1[x] = -9999;
						out_data2[x] = -9999;
						out_data3[x] = -9999;
					}
				}
				else
				{
	//				cout << "above ground carbon is -9999: " << agc_data[x];
					out_data1[x] = -9999;
					out_data2[x] = -9999;
					out_data3[x] = -9999;

				}
	//		cout << "out data is: " << out_data2[x] << ", ";
			cout << x << ", " << y << ", " << agc_data[x] << ", " << bgc_data[x] << ", ";
			cout << loss_data[x] << ", " << soil_data[x] << ", " << peat_data[x] << ", ";
			cout << forestmodel_data[x] << ", " << hist_data[x] << ", " << ecozone_data[x] << ", ";
			cout << climate_data[x] << ", " << dead_data[x] << ", " << litter_data[x] << ", ";
			cout << out_data1[x] << out_data2[x] << out_data3[x] << ifl_data[x] << "\n";
		}
		
		else // not on loss
		{
			out_data1[x] = -9999;
		    out_data2[x] = -9999;
			out_data3[x] = -9999;
		}

    }

OUTBAND1->RasterIO( GF_Write, 0, y, xsize, 1, out_data1, xsize, 1, GDT_Float32, 0, 0 ); 
OUTBAND2->RasterIO( GF_Write, 0, y, xsize, 1, out_data2, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND3->RasterIO( GF_Write, 0, y, xsize, 1, out_data3, xsize, 1, GDT_Float32, 0, 0 );

//closes for y loop
}

//close GDAL
GDALClose(INGDAL);
GDALClose((GDALDatasetH)OUTGDAL);
GDALClose((GDALDatasetH)OUTGDAL2);
GDALClose((GDALDatasetH)OUTGDAL3);

return 0;
}
