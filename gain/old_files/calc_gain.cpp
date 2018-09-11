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
using namespace std;
#include <gdal_priv.h>
#include <cpl_conv.h>
#include <ogr_spatialref.h>

// #include <gdal/gdal_priv.h>
// #include <gdal/cpl_conv.h>
// #include <gdal/ogr_spatialref.h>
//to compile:  c++ raster_math.cpp -o raster_math -lgdal
// to compile on MINGW: g++ calc_gain.cpp -o calc_gain.exe -I /usr/local/include -L /usr/local/lib -lgdal
int main(int argc, char* argv[])
{
//passing arguments
if (argc != 2){cout << "Use <program name> <tile id>" << endl; return 1;}

// in files
string tile_id = argv[1];

string tcd_name = tile_id + "_tcd.tif";
string plant_gr_name = tile_id + "_plantations.tif";
string loss_name = tile_id + "_loss.tif";
string gain_name = tile_id + "_gain.tif";
string old_gr_name = tile_id + "_old.tif";
string young_gr_name = tile_id + "_young.tif";

// out files
string out_name1= "outdata/" + tile_id + "_gain.tif";

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

//open file and get extent and projection
INGDAL = (GDALDataset *) GDALOpen(tcd_name.c_str(), GA_ReadOnly ); 
INBAND = INGDAL->GetRasterBand(1);

INGDAL2 = (GDALDataset *) GDALOpen(plant_gr_name.c_str(), GA_ReadOnly );
INBAND2 = INGDAL2->GetRasterBand(1);

INGDAL3 = (GDALDataset *) GDALOpen(loss_name.c_str(), GA_ReadOnly );
INBAND3 = INGDAL3->GetRasterBand(1);

INGDAL4 = (GDALDataset *) GDALOpen(gain_name.c_str(), GA_ReadOnly );
INBAND4 = INGDAL4->GetRasterBand(1);

INGDAL5 = (GDALDataset *) GDALOpen(old_gr_name.c_str(), GA_ReadOnly );
INBAND5 = INGDAL5->GetRasterBand(1);

INGDAL6 = (GDALDataset *) GDALOpen(young_gr_name.c_str(), GA_ReadOnly );
INBAND6 = INGDAL6->GetRasterBand(1);

xsize=INBAND3->GetXSize(); 
ysize=INBAND3->GetYSize();

xsize = 5000;
ysize = 5000;
INGDAL->GetGeoTransform(GeoTransform);

ulx=GeoTransform[0]; 
uly=GeoTransform[3]; 
pixelsize=GeoTransform[1];
cout << xsize <<", "<< ysize <<", "<< ulx <<", "<< uly << ", "<< pixelsize << endl;

//initialize GDAL for writing
GDALDriver *OUTDRIVER;
GDALDataset *OUTGDAL;
GDALRasterBand *OUTBAND1;

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

//read/write data
float tcd_data[xsize];
float plant_gr_data[xsize];
float loss_data[xsize];
float gain_data[xsize];
float old_gr_data[xsize];
float young_gr_data[xsize];
float outdata1[xsize];

for (y=0; y<ysize; y++) 
{
INBAND->RasterIO(GF_Read, 0, y, xsize, 1, tcd_data, xsize, 1, GDT_Float32, 0, 0);
INBAND2->RasterIO(GF_Read, 0, y, xsize, 1, plant_gr_data, xsize, 1, GDT_Float32, 0, 0);
INBAND3->RasterIO(GF_Read, 0, y, xsize, 1, loss_data, xsize, 1, GDT_Float32, 0, 0);
INBAND4->RasterIO(GF_Read, 0, y, xsize, 1, gain_data, xsize, 1, GDT_Float32, 0, 0);
INBAND5->RasterIO(GF_Read, 0, y, xsize, 1, old_gr_data, xsize, 1, GDT_Float32, 0, 0);
INBAND6->RasterIO(GF_Read, 0, y, xsize, 1, young_gr_data, xsize, 1, GDT_Float32, 0, 0);

for(x=0; x<xsize; x++)
	{
		int growth_rate;
		growth_rate = 1.99;
		// for now, replace plant_gr_data with old_growth rate
		if (tcd_data[x] > 0) // on TCD > 0
		{
			if (plant_gr_data[x] > 0)   // plantations
			{
				// calc plant_gr_data[x] here
				
				if (loss_data[x] > 0) // plantations, loss
				{
					if (gain_data[x] > 0 ) // plantations, loss, gain
					{
						outdata1[x] = (growth_rate * (loss_data[x]-1)) + ((15-(loss_data[x]+1)/2) * growth_rate);					
					}
					
					else // tcd, plantation, loss, no gain
					{
						outdata1[x] = (growth_rate) * (loss_data[x] -1);
					}
				}
				else // plantation, no loss
				{
					if (gain_data[x] > 0 ) // plantations, no loss, gain
					{
						outdata1[x] = growth_rate * 7.5;
						
					}
					else // plantations, no loss, no gain
					{
						outdata1[x] = growth_rate * 15;
					}
				}
			}
			else // no plantations
			{
				if (loss_data[x] > 0) // no plantations, loss
				{
					if (gain_data[x] > 0 ) // no plantations, loss, gain
					{
						outdata1[x] = ((old_gr_data[x] + young_gr_data[x])/2) * (loss_data[x] -1) + ((((15-(loss_data[x]+1)))/2) * young_gr_data[x]);
					}
					else  // no plantations, loss, no gain
					{
						outdata1[x] = ((old_gr_data[x] + young_gr_data[x]) /2) * (loss_data[x] -1);
					}
				}
				else // no plantations, no loss
				{
					if (gain_data[x] > 0 ) // no plantations, no loss, gain
					{
						outdata1[x] = ((old_gr_data[x] + young_gr_data[x]) /2) * 7.5;
					}
					else // no plantations, no loss, no gain
					{
						outdata1[x] = old_gr_data[x] * 15;
					}
				}
			}
		}
		else // no TCD data
		{
			outdata1[x] = -9999;
		}
	} // close x loop
OUTBAND1->RasterIO( GF_Write, 0, y, xsize, 1, outdata1, xsize, 1, GDT_Float32, 0, 0 ); 

} // close y loop
GDALClose(INGDAL);
GDALClose((GDALDatasetH)OUTGDAL);

return 0;
} // close function

