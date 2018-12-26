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
//to compile:  c++ calc_carbon_pools.cpp -o calc_carbon_pools.exe -lgdal
// ./dead_wood_c_stock.exe 00N_000E_biomass.tif 00N_000E_res_ecozone.tif 00N_000E_res_srtm.tif 00N_000E_res_srtm.tif test.tif > values.txt

int main(int argc, char* argv[])
{
//passing arguments
if (argc != 2){cout << "Use <program name> <tile id>" << endl; return 1;}

string tile_id =argv[1];
string agb_natrl_name = tile_id + "_t_aboveground_biomass_ha_2000.tif";   //Aboveground biomass of natural, non-mangrove forests
string agb_mangrove_name =  tile_id + "_mangrove_agb_t_ha.tif"
string biome_name = tile_id + "_res_fao_ecozones_bor_tem_tro.tif";
string elevation_name = tile_id + "_res_srtm.tif";
string precip_name = tile_id + "_res_precip.tif";
string soil_name =  tile_id + "_soil_t_C_ha.tif";


//output carbon pool tif names for non-mangrove forests
string outname_agc_natrl = tile_id + "_t_AGC_ha_natrl.tif";
string outname_bgc_natrl = tile_id + "_t_BGC_ha_natrl.tif";
string outname_deadwood_natrl = tile_id + "_t_deadwood_C_ha_natrl.tif";
string outname_litter_natrl = tile_id + "_t_litter_C_ha_natrl.tif";
string outname_total_C_natrl = tile_id + "_t_total_C_ha_natrl.tif";

//output carbon pool tif names for mangrove forests
string outname_agc_mang = tile_id + "_t_AGC_ha_mangrove.tif";
string outname_bgc_mang = tile_id + "_t_BGC_ha_mangrove.tif";
string outname_deadwood_mang = tile_id + "_t_deadwood_C_ha_mangrove.tif";
string outname_litter_mang = tile_id + "_t_litter_C_ha_mangrove.tif";
string outname_total_C_mang = tile_id + "_t_total_C_ha_mangrove.tif";

//output tif names for non-mangrove and mangrove forests combined
string outname_agc_total = tile_id + "_t_AGC_ha_total.tif";
string outname_bgc_total = tile_id + "_t_BGC_ha_total.tif";
string outname_deadwood_total = tile_id + "_t_deadwood_C_ha_total.tif";
string outname_litter_total = tile_id + "_t_litter_C_ha_total.tif";
string outname_total_C_total = tile_id + "_t_total_C_ha_total.tif";

//setting variables
int x, y;
int xsize, ysize;
double GeoTransform[6]; double ulx, uly; double pixelsize;

//initialize GDAL for reading
GDALAllRegister();
GDALDataset  *INGDAL1; GDALRasterBand  *INBAND1;
GDALDataset  *INGDAL2; GDALRasterBand  *INBAND2;
GDALDataset  *INGDAL3; GDALRasterBand  *INBAND3;
GDALDataset  *INGDAL4; GDALRasterBand  *INBAND4;
GDALDataset  *INGDAL5; GDALRasterBand  *INBAND5;
GDALDataset  *INGDAL6; GDALRasterBand  *INBAND6;

//open file and get extent and projection
INGDAL1 = (GDALDataset *) GDALOpen(agb_natrl_name.c_str(), GA_ReadOnly );
INBAND1 = INGDAL1->GetRasterBand(1);

INGDAL2 = (GDALDataset *) GDALOpen(agb_mangrove_name.c_str(), GA_ReadOnly );
INBAND2 = INGDAL2->GetRasterBand(1);

INGDAL3 = (GDALDataset *) GDALOpen(biome_name.c_str(), GA_ReadOnly );
INBAND3 = INGDAL3->GetRasterBand(1);

INGDAL4 = (GDALDataset *) GDALOpen(elevation_name.c_str(), GA_ReadOnly );
INBAND4 = INGDAL4->GetRasterBand(1);

INGDAL5 = (GDALDataset *) GDALOpen(precip_name.c_str(), GA_ReadOnly );
INBAND5 = INGDAL5->GetRasterBand(1);

INGDAL6 = (GDALDataset *) GDALOpen(soil_name.c_str(), GA_ReadOnly );
INBAND6 = INGDAL6->GetRasterBand(1);

xsize=INBAND1->GetXSize();
ysize=INBAND1->GetYSize();
INGDAL->GetGeoTransform(GeoTransform);
ulx=GeoTransform[0]; 
uly=GeoTransform[3]; 
pixelsize=GeoTransform[1];
cout << xsize <<", "<< ysize <<", "<< ulx <<", "<< uly << ", "<< pixelsize << endl;

//xsize = 5000;
//ysize = 5000;
//initialize GDAL for writing
GDALDriver *OUTDRIVER;

//non-mangrove forests
GDALDataset *OUTGDAL1;
GDALDataset *OUTGDAL2;
GDALDataset *OUTGDAL3;
GDALDataset *OUTGDAL4;
GDALDataset *OUTGDAL5;
GDALRasterBand *OUTBAND1;
GDALRasterBand *OUTBAND2;
GDALRasterBand *OUTBAND3;
GDALRasterBand *OUTBAND4;
GDALRasterBand *OUTBAND5;

//mangrove forests
GDALDataset *OUTGDAL11;
GDALDataset *OUTGDAL12;
GDALDataset *OUTGDAL13;
GDALDataset *OUTGDAL14;
GDALDataset *OUTGDAL15;
GDALRasterBand *OUTBAND11;
GDALRasterBand *OUTBAND12;
GDALRasterBand *OUTBAND13;
GDALRasterBand *OUTBAND14;
GDALRasterBand *OUTBAND15;

//mangrove and non-mangrove forests combined
GDALDataset *OUTGDAL21;
GDALDataset *OUTGDAL22;
GDALDataset *OUTGDAL23;
GDALDataset *OUTGDAL24;
GDALDataset *OUTGDAL25;
GDALRasterBand *OUTBAND21;
GDALRasterBand *OUTBAND22;
GDALRasterBand *OUTBAND23;
GDALRasterBand *OUTBAND24;
GDALRasterBand *OUTBAND25;

OGRSpatialReference oSRS;
char *OUTPRJ = NULL;
char **papszOptions = NULL;
papszOptions = CSLSetNameValue( papszOptions, "COMPRESS", "LZW" );
OUTDRIVER = GetGDALDriverManager()->GetDriverByName("GTIFF"); 
if( OUTDRIVER == NULL ) {cout << "no driver" << endl; exit( 1 );};
oSRS.SetWellKnownGeogCS( "WGS84" );
oSRS.exportToWkt( &OUTPRJ );
double adfGeoTransform[6] = { ulx, pixelsize, 0, uly, 0, -1*pixelsize };

//non-mangrove forests
OUTGDAL1 = OUTDRIVER->Create( outname_agc_natrl.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL1->SetGeoTransform(adfGeoTransform); OUTGDAL->SetProjection(OUTPRJ);
OUTBAND1 = OUTGDAL1->GetRasterBand(1);
OUTBAND1->SetNoDataValue(-9999);

OUTGDAL2 = OUTDRIVER->Create( outname_bgc_natrl.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL2->SetGeoTransform(adfGeoTransform); OUTGDAL2->SetProjection(OUTPRJ);
OUTBAND2 = OUTGDAL2->GetRasterBand(1);
OUTBAND2->SetNoDataValue(-9999);

OUTGDAL3 = OUTDRIVER->Create( outname_deadwood_natrl.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL3->SetGeoTransform(adfGeoTransform); OUTGDAL3->SetProjection(OUTPRJ);
OUTBAND3 = OUTGDAL3->GetRasterBand(1);
OUTBAND3->SetNoDataValue(-9999);

OUTGDAL4 = OUTDRIVER->Create( outname_litter_natrl.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL4->SetGeoTransform(adfGeoTransform); OUTGDAL4->SetProjection(OUTPRJ);
OUTBAND4 = OUTGDAL4->GetRasterBand(1);
OUTBAND4->SetNoDataValue(-9999);

OUTGDAL5 = OUTDRIVER->Create( outname_total_C_natrl.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL5->SetGeoTransform(adfGeoTransform); OUTGDAL5->SetProjection(OUTPRJ);
OUTBAND5 = OUTGDAL5->GetRasterBand(1);
OUTBAND5->SetNoDataValue(-9999);

// mangrove forests
OUTGDAL11 = OUTDRIVER->Create( outname_agc_mang.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL11->SetGeoTransform(adfGeoTransform); OUTGDAL->SetProjection(OUTPRJ);
OUTBAND11 = OUTGDAL11->GetRasterBand(1);
OUTBAND11->SetNoDataValue(-9999);

OUTGDAL12 = OUTDRIVER->Create( outname_bgc_mang.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL12->SetGeoTransform(adfGeoTransform); OUTGDAL2->SetProjection(OUTPRJ);
OUTBAND12 = OUTGDAL12->GetRasterBand(1);
OUTBAND12->SetNoDataValue(-9999);

OUTGDAL13 = OUTDRIVER->Create( outname_deadwood_mang.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL13->SetGeoTransform(adfGeoTransform); OUTGDAL3->SetProjection(OUTPRJ);
OUTBAND13 = OUTGDAL13->GetRasterBand(1);
OUTBAND13->SetNoDataValue(-9999);

OUTGDAL14 = OUTDRIVER->Create( outname_litter_mang.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL14->SetGeoTransform(adfGeoTransform); OUTGDAL4->SetProjection(OUTPRJ);
OUTBAND14 = OUTGDAL24->GetRasterBand(1);
OUTBAND14->SetNoDataValue(-9999);

OUTGDAL15 = OUTDRIVER->Create( outname_total_C_mang.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL15->SetGeoTransform(adfGeoTransform); OUTGDAL5->SetProjection(OUTPRJ);
OUTBAND15 = OUTGDAL15->GetRasterBand(1);
OUTBAND15->SetNoDataValue(-9999);

// mangrove and non-mangrove forests combined
OUTGDAL21 = OUTDRIVER->Create( outname_agc_total.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL21->SetGeoTransform(adfGeoTransform); OUTGDAL->SetProjection(OUTPRJ);
OUTBAND21 = OUTGDAL21->GetRasterBand(1);
OUTBAND21->SetNoDataValue(-9999);

OUTGDAL22 = OUTDRIVER->Create( outname_bgc_total.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL22->SetGeoTransform(adfGeoTransform); OUTGDAL2->SetProjection(OUTPRJ);
OUTBAND22 = OUTGDAL22->GetRasterBand(1);
OUTBAND22->SetNoDataValue(-9999);

OUTGDAL23 = OUTDRIVER->Create( outname_deadwood_total.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL23->SetGeoTransform(adfGeoTransform); OUTGDAL3->SetProjection(OUTPRJ);
OUTBAND23 = OUTGDAL23->GetRasterBand(1);
OUTBAND23->SetNoDataValue(-9999);

OUTGDAL24 = OUTDRIVER->Create( outname_litter_total.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL24->SetGeoTransform(adfGeoTransform); OUTGDAL4->SetProjection(OUTPRJ);
OUTBAND24 = OUTGDAL24->GetRasterBand(1);
OUTBAND24->SetNoDataValue(-9999);

OUTGDAL25 = OUTDRIVER->Create( outname_total_C_total.c_str(), xsize, ysize, 1, GDT_Float32, papszOptions );
OUTGDAL25->SetGeoTransform(adfGeoTransform); OUTGDAL5->SetProjection(OUTPRJ);
OUTBAND25 = OUTGDAL25->GetRasterBand(1);
OUTBAND25->SetNoDataValue(-9999);

//read/write data
float agb_natrl_data[xsize];
float agb_mang_data[xsize];
float biome_data[xsize];
float elevation_data[xsize];
float precip_data[xsize];
float soil_data[xsize];

float out_carbon[xsize];
float out_bgc[xsize];
float out_deadwood[xsize];
float out_litter[xsize];
float out_total_C[xsize];

float deadwood;
float litter;

for(y=0; y<ysize; y++) {
INBAND1->RasterIO(GF_Read, 0, y, xsize, 1, agb_natrl_data, xsize, 1, GDT_Float32, 0, 0);
INBAND2->RasterIO(GF_Read, 0, y, xsize, 1, agb_mangrove_data, xsize, 1, GDT_Float32, 0, 0);
INBAND3->RasterIO(GF_Read, 0, y, xsize, 1, biome_data, xsize, 1, GDT_Float32, 0, 0);
INBAND4->RasterIO(GF_Read, 0, y, xsize, 1, elevation_data, xsize, 1, GDT_Float32, 0, 0);
INBAND5->RasterIO(GF_Read, 0, y, xsize, 1, precip_data, xsize, 1, GDT_Float32, 0, 0);
INBAND6->RasterIO(GF_Read, 0, y, xsize, 1, soil_data, xsize, 1, GDT_Float32, 0, 0);

for(x=0; x<xsize; x++) {
   if (agb_natrl_data[x] < 0)
   {
		out_agc[x] = -9999;
		out_bgc[x] = -9999;
		out_deadwood[x] = -9999;

		out_litter[x] = -9999;
        out_total_C[x] = -9999;
	}

   else
   {
		out_agc[x] = agb_natrl_data[x] * .47;

		out_bgc[x] = .489 * pow(agb_natrl_data[x], 0.89) *.47;

        out_deadwood[x] = deadwood_calc(biome_data[x], elevation_data[x], precip_data[x], agb_natrl_data[x]);
                
		out_litter[x] = litter_calc(biome_data[x], elevation_data[x], precip_data[x], agb_natrl_data[x]);
 
		out_total_C[x] = out_agc[x] + out_bgc[x] + out_deadwood[x] + out_litter[x] + soil_data[x];

	}
	
	
	
//closes for x loop
}
//non-mangrove forests
OUTBAND1->RasterIO( GF_Write, 0, y, xsize, 1, out_agc_natrl, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND2->RasterIO( GF_Write, 0, y, xsize, 1, out_bgc_natrl, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND3->RasterIO( GF_Write, 0, y, xsize, 1, out_deadwood_natrl, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND4->RasterIO( GF_Write, 0, y, xsize, 1, out_litter_natrl, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND5->RasterIO( GF_Write, 0, y, xsize, 1, out_total_C_natrl, xsize, 1, GDT_Float32, 0, 0 );

//mangrove forests
OUTBAND11->RasterIO( GF_Write, 0, y, xsize, 1, out_agc_mang, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND12->RasterIO( GF_Write, 0, y, xsize, 1, out_bgc_mang, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND13->RasterIO( GF_Write, 0, y, xsize, 1, out_deadwood_mang, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND14->RasterIO( GF_Write, 0, y, xsize, 1, out_litter, xsize_mang, 1, GDT_Float32, 0, 0 );
OUTBAND15->RasterIO( GF_Write, 0, y, xsize, 1, out_total_C, xsize_mang, 1, GDT_Float32, 0, 0 );

//non-mangrove and mangrove forests combined
OUTBAND21->RasterIO( GF_Write, 0, y, xsize, 1, out_agc_total, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND22->RasterIO( GF_Write, 0, y, xsize, 1, out_bgc_total, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND23->RasterIO( GF_Write, 0, y, xsize, 1, out_deadwood_total, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND24->RasterIO( GF_Write, 0, y, xsize, 1, out_litter_total, xsize, 1, GDT_Float32, 0, 0 );
OUTBAND25->RasterIO( GF_Write, 0, y, xsize, 1, out_total_C_total, xsize, 1, GDT_Float32, 0, 0 );


//closes for y loop
}

//close GDAL
GDALClose(INGDAL);
GDALClose((GDALDatasetH)OUTGDAL);

return 0;
}
