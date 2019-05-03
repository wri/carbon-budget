#include <map>
#include <iostream>
#include <stdlib.h>
using namespace std;

int peat_drn_ann_calc(int forestmodel_data, int plant_data, int lossyr)
{
	int peat_drn_ann;
	int peat_drain;
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
	peat_drain = (16 - lossyr) * peat_drn_ann;
    return peat_drain;
}


float* def_variables(int ecozone, int forestmodel_data, int ifl, int climate, int plant_data, int lossyr)
{
	// returns cf, c02, ch, n20, peatburn, peatdrain, flu
	// static float def_variables[7];
	float cf;
	float c02;
	float ch;
	float n20;
	float peatburn;
	float peat_drain;
	// float flu_val;
	// maybe define ecozone specific ones up here- gef numbers, flu numbers

	// flu_val = flu(climate, ecozone);

	if ((forestmodel_data == 1) || (forestmodel_data == 2)) // deforestation/conversion or shifting ag. only diff is flu val
	{
		// flu val is independent of ecozone


		if (ecozone == 2) // deforestation, boreal
		{
			cf = .59;
			c02 = 1569;
			ch = 4.7;
			n20 = .26;
			peatburn = 104;
			peat_drain = (16 - lossyr) * 36;

		}
		else if (ecozone == 3 )// deforestation, temperate
		{
			cf = .51;
			c02 = 1569;
			ch = 4.7;
			n20 = .26;
			peatburn = 104;
			peat_drain = (16 - lossyr) * 31;

		}
		else if (ecozone == 1) // deforestation, tropics
		{
			c02 = 1580;
			ch = 6.8;
			n20 = .2;
			peatburn = 355;
			peat_drain = peat_drn_ann_calc(forestmodel_data, plant_data, lossyr);

			if (ifl > 0)
			{
				cf = .36;
			}
			else
			{
				cf = .55;
			}

		}

		else
		{
			cf = 0;
		}
	}

	else if (forestmodel_data == 3) // forestry
	{



		if (ecozone == 2) // forestry, boreal
		{
			cf = .33;
			c02 = 1569;
			ch = 4.7;
			n20 = .26;
			peatburn = 104;
			peat_drain = (16 - lossyr) * 3;

		}
		else if (ecozone == 3 )// forestry, temperate
		{
			cf = .51;
			c02 = 1569;
			ch = 4.7;
			n20 = .26;
			peatburn = 104;
			peat_drain = (16 - lossyr) * 31;

		}
		else if (ecozone == 1) // forestry, tropics
		{
			c02 = 1580;
			ch = 6.8;
			n20 = .2;
			peatburn = 355;
			peat_drain = peat_drn_ann_calc(forestmodel_data, plant_data, lossyr);

			if (ifl > 0)
			{
				cf = .36;
			}
			else
			{
				cf = .55;
			}

		}

		else
		{
			cf = 0;
		}
	}

	else if (forestmodel_data == 4) // wildfire
	{

		if (ecozone == 2) // wildfire, boreal
		{
			cf = .59;
			c02 = 1569;
			ch = 4.7;
			n20 = .26;
			peatburn = 104;
			peat_drain = (16 - lossyr) * 3;

		}
		else if (ecozone == 3 )// wildfire, temperate
		{
			cf = .51;
			c02 = 1569;
			ch = 4.7;
			n20 = .26;
			peatburn = 104;
			peat_drain = (16 - lossyr) * 12;

		}
		else if (ecozone == 1) // wildfire, tropics
		{
			c02 = 1580;
			ch = 6.8;
			n20 = .2;
			peatburn = 355;
			peat_drain = peat_drn_ann_calc(forestmodel_data, plant_data, lossyr);

			if (ifl > 0)
			{
				cf = .36;
			}
			else
			{
				cf = .55;
			}

		}

		else
		{
			cf = 0;
		}
	}
	static float def_variables[6] = {cf, c02, ch, n20, peatburn, peat_drain};

	return def_variables;

}