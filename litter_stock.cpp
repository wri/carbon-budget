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
if (argc != 5){cout << "Use <program name> <biomass tile> <climate zone> <landcover> <output name>" << endl; return 1;}
string agb_name=argv[1];
string climate_name=argv[2];
string landcover_name=argv[3];

//either parse this var from inputs or send it in
string out_name=argv[4];

//setting variables
int x, y;
int xsize, ysize;
double GeoTransform[6]; double ulx, uly; double pixelsize;

//initialize GDAL for reading
GDALAllRegister();
GDALDataset  *INGDAL; GDALRasterBand  *INBAND;
GDALDataset  *INGDAL2; GDALRasterBand  *INBAND2;
GDALDataset  *INGDAL3; GDALRasterBand  *INBAND3;

//open file and get extent and projection
INGDAL = (GDALDataset *) GDALOpen(agb_name.c_str(), GA_ReadOnly );
INBAND = INGDAL->GetRasterBand(1);
xsize=INBAND->GetXSize(); 
ysize=INBAND->GetYSize();
INGDAL->GetGeoTransform(GeoTransform);
ulx=GeoTransform[0]; 
uly=GeoTransform[3]; 
pixelsize=GeoTransform[1];
cout << xsize <<", "<< ysize <<", "<< ulx <<", "<< uly << ", "<< pixelsize << endl;

INGDAL2 = (GDALDataset *) GDALOpen(climate_name.c_str(), GA_ReadOnly );
INBAND2 = INGDAL2->GetRasterBand(1);
INGDAL3 = (GDALDataset *) GDALOpen(landcover_name.c_str(), GA_ReadOnly );
INBAND3 = INGDAL3->GetRasterBand(1);

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
OUTGDAL = OUTDRIVER->Create( out_name.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL->SetGeoTransform(adfGeoTransform); OUTGDAL->SetProjection(OUTPRJ); 
OUTBAND1 = OUTGDAL->GetRasterBand(1);
OUTBAND1->SetNoDataValue(255);

//read/write data
uint16_t agb_data[xsize];
uint8_t climate[xsize];
uint8_t landcover[xsize];

float out_data1[xsize];

for(y=0; y<ysize; y++) {
INBAND->RasterIO(GF_Read, 0, y, xsize, 1, agb_data, xsize, 1, GDT_UInt16, 0, 0); 
INBAND2->RasterIO(GF_Read, 0, y, xsize, 1, climate, xsize, 1, GDT_Byte, 0, 0);
INBAND3->RasterIO(GF_Read, 0, y, xsize, 1, landcover, xsize, 1, GDT_Byte, 0, 0);


for(x=0; x<xsize; x++) {
	
	for(x=0; x<xsize; x++) {
  if (climate[x] == 8 && landcover[x] ==4) {
    out_data1[x] = 25;}
  else if (climate[x] == 8 && landcover[x] ==1) {
    out_data1[x] = 31;}
  else if (climate[x] == 7 && landcover[x] ==4) {
    out_data1[x] = 39;}
  else if (climate[x] == 7 && landcover[x] ==1) {
    out_data1[x] = 55;}
  else if (climate[x] == 4 && landcover[x] ==4) {
    out_data1[x] = 28;}
  else if (climate[x] == 4 && landcover[x] ==1) {
    out_data1[x] = 27;}
  else if (climate[x] == 3 && landcover[x] ==4) {
    out_data1[x] = 16;}
  else if (climate[x] == 3 && landcover[x] ==1) {
    out_data1[x] = 26;}
  else if (climate[x] == 2 && landcover[x] ==4) {
    out_data1[x] = 28.2;}
  else if (climate[x] == 2 && landcover[x] ==1) {
    out_data1[x] = 20.3;}
  else if (climate[x] == 1 && landcover[x] ==4) {
    out_data1[x] = 13;}
  else if (climate[x] == 1 && landcover[x] ==1) {
    out_data1[x] = 22;}
  else if (climate[x] == 12 && landcover[x] ==4) {
    out_data1[x] = 2.8;}
  else if (climate[x] == 12 && landcover[x] ==1) {
    out_data1[x] = 4.1;}
  else if (climate[x] == 10 && landcover[x] ==4) {
    out_data1[x] = 2.1;}
  else if (climate[x] == 10 && landcover[x] ==1) {
    out_data1[x] = 5.2;}
  else {
    out_data1[x] = 255;}

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

