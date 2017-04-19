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
if (argc != 7){cout << "Use <program name> <carbon> <below ground carbon> <deadwood> <litter> <soil> <output name>" << endl; return 1;}
string carbon=argv[1];
string bgb=argv[2];
string dead=argv[3];
string litter=argv[4];
string soil=argv[5];
//either parse this var from inputs or send it in
string out_name=argv[6];

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

//open file and get extent and projection
INGDAL = (GDALDataset *) GDALOpen(carbon.c_str(), GA_ReadOnly ); 
INBAND = INGDAL->GetRasterBand(1);
xsize=INBAND->GetXSize(); 
ysize=INBAND->GetYSize();
INGDAL->GetGeoTransform(GeoTransform);
ulx=GeoTransform[0]; 
uly=GeoTransform[3]; 
pixelsize=GeoTransform[1];
cout << xsize <<", "<< ysize <<", "<< ulx <<", "<< uly << ", "<< pixelsize << endl;

INGDAL2 = (GDALDataset *) GDALOpen(bgb.c_str(), GA_ReadOnly ); 
INBAND2 = INGDAL2->GetRasterBand(1);
INGDAL3 = (GDALDataset *) GDALOpen(dead.c_str(), GA_ReadOnly ); 
INBAND3 = INGDAL3->GetRasterBand(1);
INGDAL4 = (GDALDataset *) GDALOpen(litter.c_str(), GA_ReadOnly ); 
INBAND4 = INGDAL4->GetRasterBand(1);
INGDAL5 = (GDALDataset *) GDALOpen(soil.c_str(), GA_ReadOnly ); 
INBAND5 = INGDAL5->GetRasterBand(1);

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
OUTBAND1->SetNoDataValue(-9999);

//read/write data
float carbon_data[xsize];
float bgc_data[xsize];
float dead_data[xsize];
float litter_data[xsize];
float soil_data[xsize];
float out_data1[xsize];

for(y=0; y<ysize; y++) {
INBAND->RasterIO(GF_Read, 0, y, xsize, 1, carbon_data, xsize, 1, GDT_Float32, 0, 0); 
INBAND2->RasterIO(GF_Read, 0, y, xsize, 1, bgc_data, xsize, 1, GDT_Float32, 0, 0); 
INBAND3->RasterIO(GF_Read, 0, y, xsize, 1, dead_data, xsize, 1, GDT_Float32, 0, 0); 
INBAND4->RasterIO(GF_Read, 0, y, xsize, 1, litter_data, xsize, 1, GDT_Float32, 0, 0); 
INBAND5->RasterIO(GF_Read, 0, y, xsize, 1, soil_data, xsize, 1, GDT_Float32, 0, 0); 

for(x=0; x<xsize; x++) {
    // biomass * .5 = carbon. so take carbon * the factor
	{out_data1[x] = carbon_data[x] + bgc_data[x] + dead_data[x] + litter_data[x] + soil_data[x];}

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
