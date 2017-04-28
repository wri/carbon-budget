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
string lossclass_name = tile_id + "_res_forest_model.tif";
string peatdran_name = tile_id + "_res_peatdrainage.tif";
string hist_name = tile_id + "_res_hwsd_histosoles.tif";


// set output file name
string out_wildfire_name = tile_id + "_wildfire.tif";
string out_forestry_name = tile_id + "_forestry.tif";


// check which files were created. Some wont be there
//inline bool exists(const peatdran_name) {
//    struct stat buffer;
//    return (stat (name.c_str(), &buffer) == 0);
//}


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
GDALDataset  *INGDAL6; GDALRasterBand  *INBAND6; //loss
GDALDataset  *INGDAL7; GDALRasterBand  *INBAND7; //  lossclass
GDALDataset  *INGDAL8; GDALRasterBand  *INBAND8; // peatdran
GDALDataset  *INGDAL9; GDALRasterBand  *INBAND9; // histosoles



//open file and get extent and projection
INGDAL = (GDALDataset *) GDALOpen(bgc_name.c_str(), GA_ReadOnly ); 
INBAND = INGDAL->GetRasterBand(1);
xsize=INBAND->GetXSize(); 
ysize=INBAND->GetYSize();
INGDAL->GetGeoTransform(GeoTransform);
ulx=GeoTransform[0]; 
uly=GeoTransform[3]; 
pixelsize=GeoTransform[1];
cout << xsize <<", "<< ysize <<", "<< ulx <<", "<< uly << ", "<< pixelsize << endl;

INGDAL2 = (GDALDataset *) GDALOpen(agc_name.c_str(), GA_ReadOnly ); 
INBAND2 = INGDAL2->GetRasterBand(1);

INGDAL3 = (GDALDataset *) GDALOpen(deadc_name.c_str(), GA_ReadOnly ); 
INBAND3 = INGDAL3->GetRasterBand(1);

INGDAL4 = (GDALDataset *) GDALOpen(litterc_name.c_str(), GA_ReadOnly ); 
INBAND4 = INGDAL4->GetRasterBand(1);

INGDAL5 = (GDALDataset *) GDALOpen(soilc_name.c_str(), GA_ReadOnly ); 
INBAND5 = INGDAL5->GetRasterBand(1);

INGDAL6 = (GDALDataset *) GDALOpen(lossyear_name.c_str(), GA_ReadOnly ); 
INBAND6 = INGDAL6->GetRasterBand(1);

INGDAL7 = (GDALDataset *) GDALOpen(lossclass_name.c_str(), GA_ReadOnly );
INBAND7 = INGDAL7->GetRasterBand(1);

INGDAL8 = (GDALDataset *) GDALOpen(peatdran_name.c_str(), GA_ReadOnly );
INBAND8 = INGDAL8->GetRasterBand(1);

INGDAL9 = (GDALDataset *) GDALOpen(hist_name.c_str(), GA_ReadOnly );
INBAND9 = INGDAL8->GetRasterBand(1);

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
OUTGDAL->SetGeoTransform(adfGeoTransform); OUTGDAL->SetProjection(OUTPRJ); 
OUTBAND1 = OUTGDAL->GetRasterBand(1);
OUTBAND1->SetNoDataValue(-9999);

OUTGDAL2 = OUTDRIVER->Create( out_forestry_name.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL2->SetGeoTransform(adfGeoTransform); OUTGDAL->SetProjection(OUTPRJ); 
OUTBAND2 = OUTGDAL->GetRasterBand(1);
OUTBAND2->SetNoDataValue(-9999);

//read/write data
float bgc_data[xsize];
float agc_data[xsize];
float deadc_data[xsize];
float litterc_data[xsize];
float soilc_data[xsize];
float loss_data[xsize];
float lossclass_data[xsize];
float peatdran_data[xsize];
float hist_data[xsize];

float out_wildfire_data[xsize];

for(y=0; y<ysize; y++) {
INBAND->RasterIO(GF_Read, 0, y, xsize, 1, bgc_data, xsize, 1, GDT_Float32, 0, 0);
INBAND2->RasterIO(GF_Read, 0, y, xsize, 1, agc_data, xsize, 1, GDT_Float32, 0, 0);
INBAND3->RasterIO(GF_Read, 0, y, xsize, 1, deadc_data, xsize, 1, GDT_Float32, 0, 0);
INBAND4->RasterIO(GF_Read, 0, y, xsize, 1, litterc_data, xsize, 1, GDT_Float32, 0, 0);
INBAND5->RasterIO(GF_Read, 0, y, xsize, 1, soilc_data, xsize, 1, GDT_Float32, 0, 0);
INBAND6->RasterIO(GF_Read, 0, y, xsize, 1, loss_data, xsize, 1, GDT_Float32, 0, 0);
INBAND7->RasterIO(GF_Read, 0, y, xsize, 1, lossclass_data, xsize, 1, GDT_Float32, 0, 0);
INBAND8->RasterIO(GF_Read, 0, y, xsize, 1, peatdran_data, xsize, 1, GDT_Float32, 0, 0);
INBAND9->RasterIO(GF_Read, 0, y, xsize, 1, hist_data, xsize, 1, GDT_Float32, 0, 0);


for(x=0; x<xsize; x++) {
	if (loss_data[x] > 0) 
	{ 
		if (peatdran_data[x] != -9999) 
		{
			if (burned_data[x] != -9999) 
			{
				out_wildfire_data[x] = ((agc_data[x] + bgc_data[x]) * 3.67) + (15 - loss_data[x] * peatdran_data[x]) + 917
			}
			else 
			{
				out_wildfire_data[x] = ((agc_data[x] + bgc_data[x]) * 3.67) + (15 - loss_data[x] * peatdran_data[x])
			}
			
		}
		else 
		{
			if (hist_data[x] != -9999) 
			{
				if (climate_data[x] = 1) // tropics
				{
					((agc_data[x] + bgc_data[x]) * 3.67) + (15 - loss_data[x] * 55)
				}
				if (climate_data[x] = 2) // boreal
				{
					((agc_data[x] + bgc_data[x]) * 3.67) + (15 - loss_data[x] * 2.16)
				}				
				if (climate_data[x] = 3) // temperate
				{
					((agc_data[x] + bgc_data[x]) * 3.67) + (15 - loss_data[x] * 6.27)
				}				
				
			}
			
			else 
			{
				out_wildfire_data[x] = -9999
			}
		}
				
	}
	
	else {
		out_wildfire_data[x] = -9999
	}
//closes for x loop
}
OUTBAND1->RasterIO( GF_Write, 0, y, xsize, 1, out_wildfire_data, xsize, 1, GDT_Float32, 0, 0 ); 
OUTBAND2->RasterIO( GF_Write, 0, y, xsize, 1, out_forestry_data, xsize, 1, GDT_Float32, 0, 0 ); 
//closes for y loop
}

//close GDAL
GDALClose(INGDAL);
GDALClose((GDALDatasetH)OUTGDAL);

return 0;
}
