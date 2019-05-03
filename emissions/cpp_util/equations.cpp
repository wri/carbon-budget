#include <map>
#include <iostream>
#include <stdlib.h>
using namespace std;

// come up with the peat drainage number based on different disturbance and plantations type
int peat_drn_ann_calc(int forestmodel_data, int plant_data, int lossyr)
{
	int peat_drn_ann;
	int peat_drain;
	// 1 = oil palm, 2 = wood fiber 3 = other, nodata = 0, gets treated like 3

	if (forestmodel_data == 1 && plant_data == 1)  // Commodities, oil palm
	{
		peat_drn_ann = 47;
	}
	else if (forestmodel_data == 2 && plant_data == 1)  // Shifting ag, oil palm
	{
		peat_drn_ann = 47;
	}
	else if (forestmodel_data == 3 && plant_data == 1) // conversion, oil palm ????????? Where does 70 come from? Should this be 45?
	{
		peat_drn_ann = 70;
	}
	else if (forestmodel_data == 4 && plant_data == 1) // Wildfire, oil palm
	{
		peat_drn_ann = 45;
	}

	else if (forestmodel_data == 1 && plant_data == 2) // Commodities, wood fiber
	{
		peat_drn_ann = 80;
	}
	else if (forestmodel_data == 2 && plant_data == 2) // Shifting ag, wood fiber
	{
		peat_drn_ann = 80;
	}
	else if (forestmodel_data == 3 && plant_data == 2) // Forestry, wood fiber
	{
		peat_drn_ann = 79;
	}
	else if (forestmodel_data == 4 && plant_data == 2) // Wildfire, wood fiber
	{
		peat_drn_ann = 79;
	}

	else if (forestmodel_data == 1 && plant_data == 3) // Commodities, other plantation
	{
		peat_drn_ann = 62;
	}
	else if (forestmodel_data == 2 && plant_data == 3) // Shifting ad, other plantation
	{
		peat_drn_ann = 62;
	}
	else if (forestmodel_data == 3 && plant_data == 3) // Forestry, other plantation
	{
		peat_drn_ann = 60;
	}
	else if (forestmodel_data == 4 && plant_data == 3) // Wildfire, other plantation
	{
		peat_drn_ann = 60;
	}

//	else if (forestmodel_data == 1 && plant_data == 0)    // I don't think I should include these. They might assign peat_drn_ann values where there aren't plantations???
//	{
//		peat_drn_ann = 60;
//	}
//	else if (forestmodel_data == 2 && plant_data == 0)
//	{
//		peat_drn_ann = 62;
//	}
//	else if (forestmodel_data == 3 && plant_data == 0)
//	{
//		peat_drn_ann = 60;
//	}
//	else if (forestmodel_data == 4 && plant_data == 0)
//	{
//		peat_drn_ann = 60;
//	}

	else
	{
		peat_drn_ann = 0;
	}
	peat_drain = (15 - lossyr) * peat_drn_ann;      //     This was 16 in the original model. Should it be 15 or 16?
    return peat_drain;
}


float* def_variables(int ecozone, int forestmodel_data, int ifl, int climate, int plant_data, int lossyr)
{
	// returns cf, CO2, CH4, N2O, peatburn, peatdrain, flu
	// static float def_variables[7];
	float cf;
	float CO2;
	float CH4;
	float N2O;
	float peatburn;
	float peat_drain;
	// float flu_val;
	// maybe define ecozone specific ones up here- gef numbers, flu numbers

	// flu_val = flu(climate, ecozone);

	if ((forestmodel_data == 1) || (forestmodel_data == 2)) // Commodities or shifting ag.-- only diff is flu val
	{
		// flu val is independent of ecozone


		if (ecozone == 2) // Commodities/shifting ag, boreal
		{
			cf = 0.59;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 104;
			peat_drain = (15 - lossyr) * 36;	    // This was 16 in Sam's model. Should it be 16 or 15??????

		}
		else if (ecozone == 3 )// Commodities/shifting ag, temperate
		{
			cf = 0.51;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 104;
			peat_drain = (15 - lossyr) * 31;        // This was 16 in Sam's model. Should it be 16 or 15??????

		}
		else if (ecozone == 1) // Commodities/shifting ag, tropics
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn = 355;
			peat_drain = peat_drn_ann_calc(forestmodel_data, plant_data, lossyr);

			if (ifl > 0)    // Commodities/shifting ag, tropics, in IFL
			{
				cf = 0.36;
			}
			else            // Commodities/shifting ag, tropics, outside IFL
			{
				cf = 0.55;
			}

		}

		else
		{
			cf = 0;
		}
	}

	else if (forestmodel_data == 3) // Forestry
	{

		if (ecozone == 2) // Forestry, boreal
		{
			cf = 0.33;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 104;
			peat_drain = (15 - lossyr) * 3;             // This was 16 in Sam's model. Should it be 16 or 15??????

		}
		else if (ecozone == 3 )// Forestry, temperate
		{
			cf = 0.62;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 104;
			peat_drain = (15 - lossyr) * 12;            // This was 16 in Sam's model. Should it be 16 or 15??????

		}
		else if (ecozone == 1) // Forestry, tropics
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn = 355;
			peat_drain = peat_drn_ann_calc(forestmodel_data, plant_data, lossyr);

			if (ifl > 0)
			{
				cf = 0.36;      // Forestry, tropics, in IFL
			}
			else
			{
				cf = 0.55;      // Forestry, tropics, outside IFL
			}

		}

		else
		{
			cf = 0;
		}
	}

	else if (forestmodel_data == 4) // Wildfire
	{

		if (ecozone == 2) // Wildfire, boreal
		{
			cf = 0.59;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 104;
			peat_drain = (15 - lossyr) * 3;             // This was 16 in Sam's model. Should it be 16 or 15??????

		}
		else if (ecozone == 3 )// Wildfire, temperate
		{
			cf = 0.51;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 104;
			peat_drain = (15 - lossyr) * 12;            // This was 16 in Sam's model. Should it be 16 or 15??????

		}
		else if (ecozone == 1) // Wildfire, tropics
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn = 355;
			peat_drain = peat_drn_ann_calc(forestmodel_data, plant_data, lossyr);

			if (ifl > 0)        // Wildfire, tropics, in IFL
			{
				cf = .36;
			}
			else                // Wildfire, tropics, outside IFL
			{
				cf = .55;
			}

		}

		else
		{
			cf = 0;
		}
	}
	static float def_variables[6] = {cf, CO2, CH4, N2O, peatburn, peat_drain};

	return def_variables;

}