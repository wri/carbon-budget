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
	if (argc != 2)
	{
		cout << "Use <program name> <tile id>" << endl; return 1;
	}

	//setting variables
	string tile_id = argv[1];

	string lossclass_name = tile_id + "_res_forest_model.tif";

	string out_forestry_name = tile_id + "_forestry.tif";
	string out_conversion_name = tile_id + "_conversion.tif";

	int x, y;
	int xsize, ysize;
	double GeoTransform[6]; 
	double ulx, uly; 
	double pixelsize;

	//initialize GDAL for reading
	GDALAllRegister();
	GDALDataset  *INGDAL; GDALRasterBand  *INBAND;
	GDALDataset  *INGDAL2; GDALRasterBand  *INBAND2;
	GDALDataset  *INGDAL3; GDALRasterBand  *INBAND3;
	GDALDataset  *INGDAL4; GDALRasterBand  *INBAND4;

	//open file and get extent and projection
	INGDAL = (GDALDataset *) GDALOpen(lossclass_name.c_str(), GA_ReadOnly ); 
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
	GDALRasterBand *OUTBAND1;
	OGRSpatialReference oSRS;
	char *OUTPRJ = NULL;
	char **papszOptions = NULL;
	papszOptions = CSLSetNameValue( papszOptions, "COMPRESS", "LZW" );
	OUTDRIVER = GetGDALDriverManager()->GetDriverByName("GTIFF"); 

	if( OUTDRIVER == NULL ) 
		{
			cout << "no driver" << endl; exit( 1 );
		};
		
	oSRS.SetWellKnownGeogCS( "WGS84" );

	oSRS.exportToWkt( &OUTPRJ );

	double adfGeoTransform[6] = 
		{ ulx, pixelsize, 0, uly, 0, -1*pixelsize };
		

	OUTGDAL = OUTDRIVER->Create( out_forestry_name.c_str(), xsize, ysize, 1, GDT_Byte, papszOptions );
	OUTGDAL->SetGeoTransform(adfGeoTransform); OUTGDAL->SetProjection(OUTPRJ); 
	OUTBAND1 = OUTGDAL->GetRasterBand(1);
	OUTBAND1->SetNoDataValue(-9999);

	OUTGDAL = OUTDRIVER->Create( out_conversion_name.c_str(), xsize, ysize, 1, GDT_Byte, papszOptions );
	OUTGDAL->SetGeoTransform(adfGeoTransform); OUTGDAL->SetProjection(OUTPRJ); 
	OUTBAND1 = OUTGDAL->GetRasterBand(1);
	OUTBAND1->SetNoDataValue(-9999);

	// first declaration of data
	int8_t in_lossclass_data[xsize];
	int8_t out_conversion_data[xsize];
	int8_t out_forestry_data[xsize];


	for(y=0; y<ysize; y++) 
	{
		INBAND->RasterIO(GF_Read, 0, y, xsize, 1, in_lossclass_data, xsize, 1, GDT_Int16, 0, 0); 

		for(x=0; x<xsize; x++) 
		{
		   if (in_lossclass_data[x] == 1) 
		   {
			out_forestry_data[x] = 1;
		   }
		   if (in_lossclass_data[x] == 2) 
		   {
			out_conversion_data[x] = 2;
		   }

		}
		OUTBAND1->RasterIO( GF_Write, 0, y, xsize, 1, out_forestry_data, xsize, 1, GDT_Byte, 0, 0 ); 
		OUTBAND1->RasterIO( GF_Write, 0, y, xsize, 1, out_conversion_data, xsize, 1, GDT_Byte, 0, 0 ); 

	}

	//close GDAL
	GDALClose(INGDAL);
	GDALClose((GDALDatasetH)OUTGDAL);

	return 0;
}
