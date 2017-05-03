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
using namespace std;
//to compile:  c++ raster_math.cpp -o raster_math -lgdal
// ./dead_wood_c_stock.exe 00N_000E_biomass.tif 00N_000E_res_ecozone.tif 00N_000E_res_srtm.tif 00N_000E_res_srtm.tif test.tif > values.txt

int main(int argc, char* argv[])
{
//passing arguments
if (argc != 2){cout << "Use <program name> <tile_id>" << endl; return 1;}

string tile_id = argv[1];

// aux data
string lossclass_name = tile_id + "_res_forest_model.tif";

// set output file name
string out_forestry_name = tile_id + "_forestry.tif";
string out_conversion_name = tile_id + "_conversion.tif";
//setting variables
int x, y;
int xsize, ysize;
double GeoTransform[6]; double ulx, uly; double pixelsize;

//initialize GDAL for reading
GDALAllRegister();
GDALDataset  *INGDAL7; GDALRasterBand  *INBAND7; //  lossclass

//open file and get extent and projection
INGDAL7 = (GDALDataset *) GDALOpen(lossclass_name.c_str(), GA_ReadOnly );
INBAND7 = INGDAL7->GetRasterBand(1);
xsize=INBAND7->GetXSize();
ysize=INBAND7->GetYSize();
INGDAL7->GetGeoTransform(GeoTransform);
ulx=GeoTransform[0];
uly=GeoTransform[3];
pixelsize=GeoTransform[1];
cout << xsize <<", "<< ysize <<", "<< ulx <<", "<< uly << ", "<< pixelsize << endl;

//initialize GDAL for writing
GDALDriver *OUTDRIVER;

GDALDataset *OUTGDAL2;
GDALDataset *OUTGDAL3;

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


OUTGDAL2 = OUTDRIVER->Create( out_forestry_name.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL2->SetGeoTransform(adfGeoTransform); OUTGDAL2->SetProjection(OUTPRJ); 
OUTBAND2 = OUTGDAL2->GetRasterBand(1);
OUTBAND2->SetNoDataValue(-9999);

OUTGDAL3 = OUTDRIVER->Create( out_conversion_name.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL3->SetGeoTransform(adfGeoTransform); OUTGDAL3->SetProjection(OUTPRJ);
OUTBAND3 = OUTGDAL3->GetRasterBand(1);
OUTBAND3->SetNoDataValue(-9999);


//read/write data
uint8_t lossclass_data[xsize];

float out_forestry_data[xsize];
float out_conversion_data[xsize];

for(y=0; y<3; y++) 
{
INBAND7->RasterIO(GF_Read, 0, y, xsize, 1, lossclass_data, xsize, 1, GDT_Byte, 0, 0);

for(x=0; x<xsize; x++) 
{

		if (lossclass_data[x] = 1) // forestry
		{
			out_forestry_data[x] = 1;
			cout << "loss class is forestry: " << lossclass_data[x] << "\n";
		}
		else if (lossclass_data[x] = 2) // conversion
		{
			out_conversion_data[x] = 1;
			cout << "loss class is conversion: " << lossclass_data[x] << "\n";

		}
		else
		{
		       out_conversion_data[x] = -9999;
		       cout << "loss class is something else: " << lossclass_data[x] << "\n";
		}
//closes for x loop
}
OUTBAND2->RasterIO( GF_Write, 0, y, xsize, 1, out_forestry_data, xsize, 1, GDT_Float32, 0, 0 ); 
OUTBAND3->RasterIO( GF_Write, 0, y, xsize, 1, out_conversion_data, xsize, 1, GDT_Float32, 0, 0 );

//closes for y loop
}

//close GDAL
GDALClose(INGDAL7);
GDALClose((GDALDatasetH)OUTGDAL2);
GDALClose((GDALDatasetH)OUTGDAL3);

return 0;
}
