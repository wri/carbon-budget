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
int main(int argc, char* argv[])
{
//passing arguments
if (argc != 4){cout << "Use <program name> <extent raster> <loss raster> <output name>" << endl; return 1;}
string extent_name=argv[1];
string loss_name=argv[2];
//either parse this var from inputs or send it in
string out_name=argv[3];

//setting variables
int x, y;
int xsize, ysize;
double GeoTransform[6]; double ulx, uly; double pixelsize;

//initialize GDAL for reading
GDALAllRegister();
GDALDataset  *INGDAL; GDALRasterBand  *INBAND;
GDALDataset  *INGDAL2; GDALRasterBand  *INBAND2;

//open file and get extent and projection
INGDAL = (GDALDataset *) GDALOpen(extent_name.c_str(), GA_ReadOnly ); INBAND = INGDAL->GetRasterBand(1);
xsize=INBAND->GetXSize(); ysize=INBAND->GetYSize();
INGDAL->GetGeoTransform(GeoTransform);
ulx=GeoTransform[0]; uly=GeoTransform[3]; pixelsize=GeoTransform[1];
cout << xsize <<", "<< ysize <<", "<< ulx <<", "<< uly << ", "<< pixelsize << endl;
INGDAL2 = (GDALDataset *) GDALOpen(loss_name.c_str(), GA_ReadOnly ); INBAND2 = INGDAL2->GetRasterBand(1);

//initialize GDAL for writing
GDALDriver *OUTDRIVER;
GDALDataset *OUTGDAL;
GDALRasterBand *OUTBAND1;
OGRSpatialReference oSRS;
char *OUTPRJ = NULL;
char **papszOptions = NULL;
papszOptions = CSLSetNameValue( papszOptions, "COMPRESS", "LZW" );
OUTDRIVER = GetGDALDriverManager()->GetDriverByName("GTIFF"); if( OUTDRIVER == NULL ) {cout << "no driver" << endl; exit( 1 );};
oSRS.SetWellKnownGeogCS( "WGS84" );
oSRS.exportToWkt( &OUTPRJ );
double adfGeoTransform[6] = { ulx, pixelsize, 0, uly, 0, -1*pixelsize };
OUTGDAL = OUTDRIVER->Create( out_name.c_str(), xsize, ysize, 1, GDT_Byte, papszOptions );
OUTGDAL->SetGeoTransform(adfGeoTransform); OUTGDAL->SetProjection(OUTPRJ); 
OUTBAND1 = OUTGDAL->GetRasterBand(1);
OUTBAND1->SetNoDataValue(255);

//read/write data
uint8_t in1_data[xsize];
uint8_t in2_data[xsize];
uint8_t out_data1[xsize];

for(y=0; y<ysize; y++) {
INBAND->RasterIO(GF_Read, 0, y, xsize, 1, in1_data, xsize, 1, GDT_Byte, 0, 0); 
INBAND2->RasterIO(GF_Read, 0, y, xsize, 1, in2_data, xsize, 1, GDT_Byte, 0, 0); 
for(x=0; x<xsize; x++) {
  if (in1_data[x] > 1 && in1_data[x] < 11) {
    out_data1[x] = 20 + in2_data[x];}
  else if (in1_data[x] > 10 && in1_data[x] < 16) {
    out_data1[x] = 40 + in2_data[x];}
  else if (in1_data[x] > 15 && in1_data[x] < 21) {
    out_data1[x] = 60 + in2_data[x];}
  else if (in1_data[x] > 20 && in1_data[x] < 26) {
    out_data1[x] = 80 + in2_data[x];}
  else if (in1_data[x] > 25 && in1_data[x] < 31) {
    out_data1[x] = 100 + in2_data[x];}
  else if (in1_data[x] > 30 && in1_data[x] < 51) {
    out_data1[x] = 120 + in2_data[x];}
  else if (in1_data[x] > 50 && in1_data[x] < 76) {
    out_data1[x] = 140 + in2_data[x];}
  else if (in1_data[x] > 75) {
    out_data1[x] = 160 + in2_data[x];}
  else {
    out_data1[x] = 255;}
  //cout << in1_data[x] << "," << in2_data[x] << "," << out_data1[x] << "\n";

//closes for x loop
}
OUTBAND1->RasterIO( GF_Write, 0, y, xsize, 1, out_data1, xsize, 1, GDT_Byte, 0, 0 ); 
//closes for y loop
}

//close GDAL
GDALClose(INGDAL);
GDALClose((GDALDatasetH)OUTGDAL);

return 0;
}
