#include <map>
#include <iostream>
#include <stdlib.h>
using namespace std;


float forestry_peat_burned(int ecozone_data, float agc_data, float bgc_data, int loss_data, int ifl_data, int peat_drn_ann)
{
	float outdata1;
		if (ecozone_data == 3) // forestry, peat, burned, temperate
	{
		outdata1 =  ((agc_data + bgc_data) * 3.67) + ((2*(agc_data+bgc_data)) *0.62 * 4.7 * pow(10, -3) * 28) + ((2*(agc_data + bgc_data)) * 0.62 *0.26 * pow(10, -3) * 265)+(16 - loss_data)*12+104;
	}
	else if (ecozone_data == 2) // forestry, peat, burned, boreal
	{
		outdata1 =  ((agc_data + bgc_data) * 3.67) + ((2*(agc_data+bgc_data)) *0.33 * 4.7 * pow(10, -3) * 28) + ((2*(agc_data + bgc_data)) * 0.33 *0.26 * pow(10, -3) * 265)+(16 - loss_data)*3+104;
	}
	else if (ecozone_data == 1) // forestry, peat, burned, tropic
	{
		if (ifl_data != 0) // forestry, peat, burned, tropic, ifl
		{
			outdata1 =  ((agc_data + bgc_data) * 3.67) + ((2*(agc_data+bgc_data)) *0.36 * 6.8 * pow(10, -3) * 28) + ((2*(agc_data + bgc_data)) * 0.36 *0.2 * pow(10, -3) * 265)+(16 - loss_data)* peat_drn_ann + 355;
		}
		else // forestry, peat, burned, tropic, not ifl
		{
			 outdata1 = ((agc_data + bgc_data) * 3.67) + ((2*(agc_data+bgc_data)) *0.55 * 6.8 * pow(10, -3) * 28) + ((2*(agc_data + bgc_data)) * 0.55 *0.2 * pow(10, -3) * 265)+(16 - loss_data)* peat_drn_ann +355;
		}
	}
	return outdata1;
}

float forestry_xpeat_burned(int ecozone_data, float agc_data, float bgc_data, int loss_data, int ifl_data, int peat_drn_ann)
{
	float outdata1;

	if (ecozone_data == 3) // forestry, not peat, burned, temperate
	{
		outdata1 =  ((agc_data + bgc_data) * 3.67) + ((2*(agc_data+bgc_data)) *0.62 * 4.7 * pow(10, -3) * 28) + ((2*(agc_data + bgc_data)) * 0.62 *0.26 * pow(10, -3) * 265);
	}
	else if (ecozone_data == 2) // forestry, not peat, burned, boreal
	{
		outdata1 =  ((agc_data + bgc_data) * 3.67) + ((2*(agc_data+bgc_data)) *0.33 * 4.7 * pow(10, -3) * 28) + ((2*(agc_data + bgc_data)) * 0.33 *0.26 * pow(10, -3) * 265);
	}
	else if (ecozone_data == 1) // forestry, not peat, burned, tropics
	{
		if (ifl_data != 0) // forestry, not peat, burned, tropics, ifl
		{
			outdata1 =  ((agc_data + bgc_data) * 3.67) + ((2*(agc_data+bgc_data)) *0.36 * 6.8 * pow(10, -3) * 28) + ((2*(agc_data + bgc_data)) * 0.36 *0.2 * pow(10, -3) * 265);
		}
		else  // forestry, not peat, burned, tropics, not ifl
		{
			outdata1 =  ((agc_data + bgc_data) * 3.67) + ((2*(agc_data+bgc_data)) *0.55 * 6.8 * pow(10, -3) * 28) + ((2*(agc_data + bgc_data)) * 0.55 *0.2 * pow(10, -3) * 265);
		}
	}
	else  // forestry, not peat, burned, no-ecozone
	{
		outdata1 = 0;
	}
	return outdata1;
}


float forestry_xpeat_xburn(float agc_data, float bgc_data)
{
	float outdata1;
	outdata1 = (agc_data + bgc_data) * 3.67;
	
	return outdata1;
}

float forestry_peat_xburned(int ecozone_data, int agc_data, int bgc_data, int loss_data, int peat_drn_ann, int plant_data)
{
	float outdata1;
	
	if (ecozone_data == 3) // forestry, peat, not burned, temperate
	{
		outdata1 = (agc_data + bgc_data) * 3.67+(16 - loss_data)*12;
	}
	else if (ecozone_data == 2) // forestry, peat, not burned, boreal
	{
		outdata1 = (agc_data + bgc_data) * 3.67+(16 - loss_data)*3;
	}
	else if (ecozone_data == 1) // forestry, peat, not burned, tropic
	{
		if (plant_data == 0 ) // forestry, peat, not burned, tropic, not plantation
		{
			outdata1 = (agc_data + bgc_data) * 3.67;
		}
		else // forestry, peat, not burned, tropic, plantation
		{
			outdata1 = (agc_data + bgc_data) * 3.67+(16 - loss_data)* peat_drn_ann;										
		}
	}
	else // no ecozone
	{
		outdata1 = 0;
	}
	
	return outdata1;
						
}


int peat_forestry_burned(int ecozone_data)
{
		if (ecozone_data[x] == 3) // forestry, peat, burned, temperate
	{
		outdata1 =  ((agc_data[x] + bgc_data[x]) * 3.67) + ((2*(agc_data[x]+bgc_data[x])) *0.62 * 4.7 * pow(10, -3) * 28) + ((2*(agc_data[x] + bgc_data[x])) * 0.62 *0.26 * pow(10, -3) * 265)+(15 - loss_data[x])*12+104;
	}
	else if (ecozone_data[x] == 2) // forestry, peat, burned, boreal
	{
		outdata1 =  ((agc_data[x] + bgc_data[x]) * 3.67) + ((2*(agc_data[x]+bgc_data[x])) *0.33 * 4.7 * pow(10, -3) * 28) + ((2*(agc_data[x] + bgc_data[x])) * 0.33 *0.26 * pow(10, -3) * 265)+(15 - loss_data[x])*3+104;
	}
	else if (ecozone_data[x] == 1) // forestry, peat, burned, tropic
	{
		if (ifl_data[x] != 0) // forestry, peat, burned, tropic, ifl
		{
			outdata1 =  ((agc_data[x] + bgc_data[x]) * 3.67) + ((2*(agc_data[x]+bgc_data[x])) *0.36 * 6.8 * pow(10, -3) * 28) + ((2*(agc_data[x] + bgc_data[x])) * 0.36 *0.2 * pow(10, -3) * 265)+(15 - loss_data[x])* peat_drn_ann + 355;
		}
		else // forestry, peat, burned, tropic, not ifl
		{
			 outdata1 = ((agc_data[x] + bgc_data[x]) * 3.67) + ((2*(agc_data[x]+bgc_data[x])) *0.55 * 6.8 * pow(10, -3) * 28) + ((2*(agc_data[x] + bgc_data[x])) * 0.55 *0.2 * pow(10, -3) * 265)+(15 - loss_data[x])* peat_drn_ann +355;
		}
	}
	return outdata1
}

int peat_drn_ann_calc(int forestmodel_data, int plant_data)
{
	int peat_drn_ann;
	// 2 = oil palm, 3 = wood fiber 4 = other, nodata = 0, gets treated like 4
	
	if (forestmodel_data == 1 && plant_data == 2) // forestry , oil palm
	{
		peat_drn_ann = 45;
	}
	else if (forestmodel_data == 2 && plant_data == 2) // conversion , oil palm
	{
		peat_drn_ann = 70;
	}
	else if (forestmodel_data == 3 && plant_data == 2)  // wildifre , oil palm
	{
		peat_drn_ann = 80;
	}
	
	else if (forestmodel_data == 1 && plant_data == 3) // forestry , wood fiber
	{
		peat_drn_ann = 79;
	}
	else if (forestmodel_data == 2 && plant_data == 3) // conversion , wood fiber
	{
		peat_drn_ann = 80;
	}
	else if (forestmodel_data == 3 && plant_data == 3) // wildifre , wood fiber
	{
		peat_drn_ann = 79;
	}
	
	else if ((forestmodel_data == 1) && (plant_data == 4))
	{
		peat_drn_ann = 60;
	}
	else if (forestmodel_data == 2 && plant_data == 4)
	{
		peat_drn_ann = 62;
	}
	
	else if (forestmodel_data == 3 && plant_data == 4)
	{
		peat_drn_ann = 60;
	}	
	
		else if ((forestmodel_data == 1) && (plant_data == 0))
	{
		peat_drn_ann = 60;
	}
	else if (forestmodel_data == 2 && plant_data == 0)
	{
		peat_drn_ann = 62;
	}
	
	else if (forestmodel_data == 3 && plant_data == 0)
	{
		peat_drn_ann = 60;
	}
	
	else
	{
		peat_drn_ann = 0;
	}
	
    return peat_drn_ann;
}


