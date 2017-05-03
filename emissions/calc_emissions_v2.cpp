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

int main(int argc, char* argv[])
{
//passing arguments
if (argc != 2){cout << "Use <program name> <tile id>" << endl; return 1;}
string tileid_name=argv[1];
string forestmodel_name = tileid_name + "_res_forest_model.tif";

string agb_name=argv[1];
//either parse this var from inputs or send it in
string out_name1="test1.tif";
string out_name2 = "test2.tif";
string out_forestry_name = tileid_name + "_out_forestry.tif";
string out_conversion_name = tileid_name + "_out_conversion.tif";

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

//initialize GDAL for writing
GDALDriver *OUTDRIVER;
GDALDataset *OUTGDAL;
GDALDataset *OUTGDAL2;
GDALRasterBand *OUTBAND1;
GDALRasterBand *OUTBAND2;
OGRSpatialReference oSRS;
char *OUTPRJ = NULL;
char **papszOptions = NULL;
papszOptions = CSLSetNameValue( papszOptions, "COMPRESS", "LZW" );
OUTDRIVER = GetGDALDriverManager()->GetDriverByName("GTIFF"); 
if( OUTDRIVER == NULL ) {cout << "no driver" << endl; exit( 1 );};
oSRS.SetWellKnownGeogCS( "WGS84" );
oSRS.exportToWkt( &OUTPRJ );
double adfGeoTransform[6] = { ulx, pixelsize, 0, uly, 0, -1*pixelsize };



OUTGDAL = OUTDRIVER->Create( out_forestry_name.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL->SetGeoTransform(adfGeoTransform); OUTGDAL->SetProjection(OUTPRJ); 
OUTBAND1 = OUTGDAL->GetRasterBand(1);
OUTBAND1->SetNoDataValue(-9999);

OUTGDAL2 = OUTDRIVER->Create( out_conversion_name.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL2->SetGeoTransform(adfGeoTransform); OUTGDAL2->SetProjection(OUTPRJ);
OUTBAND2 = OUTGDAL2->GetRasterBand(1);
OUTBAND2->SetNoDataValue(-9999);

OUTGDAL3 = OUTDRIVER->Create( out_name2.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL3->SetGeoTransform(adfGeoTransform); OUTGDAL3->SetProjection(OUTPRJ);
OUTBAND2 = OUTGDAL3->GetRasterBand(1);
OUTBAND2->SetNoDataValue(-9999);

//read/write data
float agb_data[xsize];
float out_data1[xsize];
float out_data2[xsize];
float forestmodel_data[xsize];
float out_forestry_data[xsize];
float out_conversion_data[xsize];

for(y=0; y<2; y++) {
INBAND->RasterIO(GF_Read, 0, y, xsize, 1, forestmodel_data, xsize, 1, GDT_Float32, 0, 0); 

for(x=0; x<xsize; x++) {
// no data value for biomass is set to -32768
   if (forestmodel_data[x] == 1) {
    out_forestry_data[x] = 1;}
   else {
    out_forestry_data[x] = -9999;}

   if (forestmodel_data[x] == 2) {
    out_conversion_data[x] = 2;}
   else {
    out_conversion_data[x] = -9999;}

//closes for x loop
}
OUTBAND1->RasterIO( GF_Write, 0, y, xsize, 1, out_forestry_data, xsize, 1, GDT_Float32, 0, 0 ); 
OUTBAND2->RasterIO( GF_Write, 0, y, xsize, 1, out_conversion_data, xsize, 1, GDT_Float32, 0, 0 );

//closes for y loop
}

//close GDAL
GDALClose(INGDAL);
GDALClose((GDALDatasetH)OUTGDAL);
GDALClose((GDALDatasetH)OUTGDAL2);

return 0;
}
