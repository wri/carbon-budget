#include <map>
#include <iostream>
#include <stdlib.h>
using namespace std;

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


