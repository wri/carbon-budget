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
#include "deadwood_litter.cpp"
using namespace std;
//to compile:  c++ calc_all.cpp -o calc_all.exe -lgdal
// ./dead_wood_c_stock.exe 00N_000E_biomass.tif 00N_000E_res_ecozone.tif 00N_000E_res_srtm.tif 00N_000E_res_srtm.tif test.tif > values.txt

int main(int argc, char* argv[])
{
//passing arguments
if (argc != 2){cout << "Use <program name> <tile id>" << endl; return 1;}

string tile_id =argv[1];
string agb_name = tile_id + "_biomass.tif";
string biome_name = tile_id + "_res_fao_ecozones_bor_tem_tro.tif";
string elevation_name = tile_id + "_res_srtm.tif";
string precip_name = tile_id + "_res_precip.tif";
string soil_name =  tile_id + "_soil.tif";


//either parse this var from inputs or send it in
string outname_carbon = tile_id + "_carbon.tif";
string outname_bgc = tile_id + "_bgc.tif";
string outname_deadwood = tile_id + "_deadwood.tif";
string outname_litter = tile_id + "_litter.tif";
string outname_total = tile_id + "_totalc.tif";

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


//open file and get extent and projection
INGDAL = (GDALDataset *) GDALOpen(agb_name.c_str(), GA_ReadOnly ); 
INBAND = INGDAL->GetRasterBand(1);

INGDAL2 = (GDALDataset *) GDALOpen(biome_name.c_str(), GA_ReadOnly );
INBAND2 = INGDAL2->GetRasterBand(1);

INGDAL3 = (GDALDataset *) GDALOpen(elevation_name.c_str(), GA_ReadOnly );
INBAND3 = INGDAL3->GetRasterBand(1);

INGDAL4 = (GDALDataset *) GDALOpen(precip_name.c_str(), GA_ReadOnly );
INBAND4 = INGDAL4->GetRasterBand(1);

INGDAL5 = (GDALDataset *) GDALOpen(soil_name.c_str(), GA_ReadOnly );
INBAND5 = INGDAL5->GetRasterBand(1);

xsize=INBAND->GetXSize(); 
ysize=INBAND->GetYSize();
INGDAL->GetGeoTransform(GeoTransform);
ulx=GeoTransform[0]; 
uly=GeoTransform[3]; 
pixelsize=GeoTransform[1];
cout << xsize <<", "<< ysize <<", "<< ulx <<", "<< uly << ", "<< pixelsize << endl;

//xsize = 5000;
//ysize = 5000;
//initialize GDAL for writing
GDALDriver *OUTDRIVER;
GDALDataset *OUTGDAL;

GDALDataset *OUTGDAL2;
GDALDataset *OUTGDAL3;
GDALDataset *OUTGDAL4;
GDALDataset *OUTGDAL5;

GDALRasterBand *OUTBAND1;
GDALRasterBand *OUTBAND2;
GDALRasterBand *OUTBAND3;
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

OUTGDAL = OUTDRIVER->Create( outname_carbon.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL->SetGeoTransform(adfGeoTransform); OUTGDAL->SetProjection(OUTPRJ); 
OUTBAND1 = OUTGDAL->GetRasterBand(1);
OUTBAND1->SetNoDataValue(-9999);

OUTGDAL2 = OUTDRIVER->Create( outname_bgc.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL2->SetGeoTransform(adfGeoTransform); OUTGDAL2->SetProjection(OUTPRJ);
OUTBAND2 = OUTGDAL2->GetRasterBand(1);
OUTBAND2->SetNoDataValue(-9999);

OUTGDAL3 = OUTDRIVER->Create( outname_deadwood.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL3->SetGeoTransform(adfGeoTransform); OUTGDAL3->SetProjection(OUTPRJ);
OUTBAND3 = OUTGDAL3->GetRasterBand(1);
OUTBAND3->SetNoDataValue(-9999);

OUTGDAL4 = OUTDRIVER->Create( outname_litter.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL4->SetGeoTransform(adfGeoTransform); OUTGDAL4->SetProjection(OUTPRJ);
OUTBAND4 = OUTGDAL4->GetRasterBand(1);
OUTBAND4->SetNoDataValue(-9999);

OUTGDAL5 = OUTDRIVER->Create( outname_total.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL5->SetGeoTransform(adfGeoTransform); OUTGDAL5->SetProjection(OUTPRJ);
OUTBAND5 = OUTGDAL5->GetRasterBand(1);
OUTBAND5->SetNoDataValue(-9999);

//read/write data
float agb_data[xsize];
float biome_data[xsize];
float elevation_data[xsize];
float precip_data[xsize];
float soil_data[xsize];

float out_carbon[xsize];
float out_bgc[xsize];
float out_deadwood[xsize];
float out_litter[xsize];
float out_total[xsize];

float deadwood;
float litter;

for(y=0; y<ysize; y++) {
INBAND->RasterIO(GF_Read, 0, y, xsize, 1, agb_data, xsize, 1, GDT_Float32, 0, 0); 
INBAND2->RasterIO(GF_Read, 0, y, xsize, 1, biome_data, xsize, 1, GDT_Float32, 0, 0); 
INBAND3->RasterIO(GF_Read, 0, y, xsize, 1, elevation_data, xsize, 1, GDT_Float32, 0, 0); 
INBAND4->RasterIO(GF_Read, 0, y, xsize, 1, precip_data, xsize, 1, GDT_Float32, 0, 0); 
INBAND5->RasterIO(GF_Read, 0, y, xsize, 1, soil_data, xsize, 1, GDT_Float32, 0, 0); 

for(x=0; x<xsize; x++) {
   if (agb_data[x] == -32768) 
   {
		out_carbon[x] = -9999;
		out_bgc[x] = -9999;
		out_deadwood[x] = -9999;

		out_litter[x] = -9999;
        out_total[x] = -9999;
	}

   else if (agb_data[x] == -9999)
   {
		out_carbon[x] = -9999;
		out_bgc[x] = -9999;
		out_deadwood[x] = -9999;

		out_litter[x] = -9999;
        out_total[x] = -9999;
	}
   else 
   {
		out_carbon[x] = agb_data[x] * .47;

		out_bgc[x] = .489 * pow(agb_data[x], 0.89) *.47;

        out_deadwood[x] = deadwood_calc(biome_data[x], elevation_data[x], precip_data[x], agb_data[x]);
                
		out_litter[x] = litter_calc(biome_data[x], elevation_data[x], precip_data[x], agb_data[x]);
 
		out_total[x] = out_carbon[x] + out_bgc[x] + deadwood + litter + soil_data[x];

		cout << "AGC:" << out_carbon[x];
		cout << "BGC:" << out_bgc[x];
		cout << "Deadwood:" << out_deadwood;
		cout << "Litter" << out_litter;
        cout << "Soil:" << soil_data[x];
        cout << "TotalC" << out_total[x];
        cout << "            ";

	}
	
	
	
//closes for x loop
}
OUTBAND1->RasterIO( GF_Write, 0, y, xsize, 1, out_carbon, xsize, 1, GDT_Float32, 0, 0 ); 
OUTBAND2->RasterIO( GF_Write, 0, y, xsize, 1, out_bgc, xsize, 1, GDT_Float32, 0, 0 ); 
OUTBAND3->RasterIO( GF_Write, 0, y, xsize, 1, out_deadwood, xsize, 1, GDT_Float32, 0, 0 ); 
OUTBAND4->RasterIO( GF_Write, 0, y, xsize, 1, out_litter, xsize, 1, GDT_Float32, 0, 0 ); 
OUTBAND5->RasterIO( GF_Write, 0, y, xsize, 1, out_total, xsize, 1, GDT_Float32, 0, 0 );  


//closes for y loop
}

//close GDAL
GDALClose(INGDAL);
GDALClose((GDALDatasetH)OUTGDAL);

return 0;
}
