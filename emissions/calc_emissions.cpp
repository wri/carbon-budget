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

// carbon pools
string bgc_name = tile_id + "_bgc.tif";
string agc_name = tile_id + "_carbon.tif";
string deadc_name = tile_id + "_deadwood.tif";
string soilc_name = tile_id + "_soil.tif";
string litterc_name = tile_id + "_litter.tif";

// aux data
string lossyear_name = tile_id + "_loss.tif";
string lossclass_name = tile_id + "_forestmodel.tif"
string peatdran_name = tile_id + "_peatdrain.tif"


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

//open file and get extent and projection
INGDAL = (GDALDataset *) GDALOpen(above_name.c_str(), GA_ReadOnly ); 
INBAND = INGDAL->GetRasterBand(1);
xsize=INBAND->GetXSize(); 
ysize=INBAND->GetYSize();
INGDAL->GetGeoTransform(GeoTransform);
ulx=GeoTransform[0]; 
uly=GeoTransform[3]; 
pixelsize=GeoTransform[1];
cout << xsize <<", "<< ysize <<", "<< ulx <<", "<< uly << ", "<< pixelsize << endl;

INGDAL2 = (GDALDataset *) GDALOpen(below_name.c_str(), GA_ReadOnly ); 
INBAND2 = INGDAL2->GetRasterBand(1);

INGDAL3 = (GDALDataset *) GDALOpen(dead_name.c_str(), GA_ReadOnly ); 
INBAND3 = INGDAL3->GetRasterBand(1);

INGDAL4 = (GDALDataset *) GDALOpen(litter_name.c_str(), GA_ReadOnly ); 
INBAND4 = INGDAL4->GetRasterBand(1);

INGDAL5 = (GDALDataset *) GDALOpen(soil_name.c_str(), GA_ReadOnly ); 
INBAND5 = INGDAL5->GetRasterBand(1);

INGDAL6 = (GDALDataset *) GDALOpen(forestm_name.c_str(), GA_ReadOnly ); 
INBAND6 = INGDAL6->GetRasterBand(1);

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

OUTGDAL = OUTDRIVER->Create( out_wildfire_name.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL = OUTDRIVER->Create( out_conv_name.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL = OUTDRIVER->Create( out_plant_name.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL = OUTDRIVER->Create( out_mixed_name.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL->SetGeoTransform(adfGeoTransform); OUTGDAL->SetProjection(OUTPRJ); 
OUTBAND1 = OUTGDAL->GetRasterBand(1);
OUTBAND1->SetNoDataValue(-9999);

//read/write data
float above_name[xsize];
float below_name[xsize];
float dead_name[xsize];
float litter_name[xsize];
float soil_name[xsize];
float forestm_name[xsize];


float out_wildfire_name[xsize];
float out_conv_name[xsize];
float out_plant_name[xsize];
float out_mixed_name[xsize];

for(y=0; y<ysize; y++) {
INBAND->RasterIO(GF_Read, 0, y, xsize, 1, above_name, xsize, 1, GDT_UInt16, 0, 0); 
INBAND2->RasterIO(GF_Read, 0, y, xsize, 1, below_name, xsize, 1, GDT_UInt16, 0, 0); 
INBAND3->RasterIO(GF_Read, 0, y, xsize, 1, dead_name, xsize, 1, GDT_UInt16, 0, 0); 
INBAND4->RasterIO(GF_Read, 0, y, xsize, 1, precip_data, xsize, 1, GDT_UInt16, 0, 0); 

for(x=0; x<xsize; x++) {
    // biomass * .5 = carbon. so take carbon * the factor
	// biome =1 are all tropics (subtropical, tropical), biome = 2 are temperate and boreal
  if (forestm_name[x] = 1 && elevation_data[x] < 2000 && precip_data[x] < 1000) {
    out_data1[x] = agb_data[x] * .04 * .37;}
  else if (biome_data[x] = 1 && elevation_data[x] < 2000 && precip_data[x] < 1600 && precip_data[x] > 1000) {
    out_data1[x] = agb_data[x] * .01 * .37;}
  else if (biome_data[x] = 1 && elevation_data[x] < 2000 && precip_data[x] > 1600) {
    out_data1[x] = agb_data[x] * .01 * .37;}
  else if (biome_data[x] = 1 && elevation_data[x] > 2000) {
    out_data1[x] = agb_data[x] * .01 * .37;}
  else if (biome_data[x] = 2) {
    out_data1[x] = agb_data[x] * .04 * .37;}
  else {
    out_data1[x] = -9999;}

//closes for x loop
}
OUTBAND1->RasterIO( GF_Write, 0, y, xsize, 1, out_data1, xsize, 1, GDT_Float32, 0, 0 ); 
//closes for y loop
}

//close GDAL
GDALClose(INGDAL);
GDALClose((GDALDatasetH)OUTGDAL);

return 0;
}
