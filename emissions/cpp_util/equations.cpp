#include <map>
#include <iostream>
#include <stdlib.h>
using namespace std;

// come up with the peat drainage number based on different disturbance and plantations type
int peat_drn_ann_calc(int forestmodel_data, int plant_data, int lossyr)
{
	int peat_drn_ann;
	int peat_drain_total;

	// For plant_data, 1 = oil palm, 2 = wood fiber 3 = other, nodata = 0
	if (forestmodel_data == 1 && plant_data == 1)  // Commodities, oil palm
	{
		peat_drn_ann = 47;
	}
	else if (forestmodel_data == 1 && plant_data == 2) // Commodities, wood fiber
	{
		peat_drn_ann = 80;
	}
	else if (forestmodel_data == 1 && plant_data == 3) // Commodities, other plantation
	{
		peat_drn_ann = 62;
	}

	else if (forestmodel_data == 2 && plant_data == 1)  // Shifting ag, oil palm
	{
		peat_drn_ann = 47;
	}
	else if (forestmodel_data == 2 && plant_data == 2) // Shifting ag, wood fiber
	{
		peat_drn_ann = 80;
	}
	else if (forestmodel_data == 2 && plant_data == 3) // Shifting ag, other plantation
	{
		peat_drn_ann = 62;
	}

	else if (forestmodel_data == 3 && plant_data == 1) // Forestry, oil palm
	{
		peat_drn_ann = 45;
	}
	else if (forestmodel_data == 3 && plant_data == 2) // Forestry, wood fiber
	{
		peat_drn_ann = 79;
	}
	else if (forestmodel_data == 3 && plant_data == 3) // Forestry, other plantation
	{
		peat_drn_ann = 60;
	}

	else if (forestmodel_data == 4 && plant_data == 1) // Wildfire, oil palm
	{
		peat_drn_ann = 45;
	}
	else if (forestmodel_data == 4 && plant_data == 2) // Wildfire, wood fiber
	{
		peat_drn_ann = 79;
	}
	else if (forestmodel_data == 4 && plant_data == 3) // Wildfire, other plantation
	{
		peat_drn_ann = 60;
	}

	else if (forestmodel_data == 5 && plant_data == 1) // Urbanization, oil palm
	{
		peat_drn_ann = 47;
	}
	else if (forestmodel_data == 5 && plant_data == 2) // Urbanization, wood fiber
	{
		peat_drn_ann = 80;
	}
	else if (forestmodel_data == 5 && plant_data == 3) // Urbanization, other plantation
	{
		peat_drn_ann = 62;
	}

	else if (forestmodel_data == 0 && plant_data == 1) // No driver, oil palm
	{
		peat_drn_ann = 45;
	}
	else if (forestmodel_data == 0 && plant_data == 2) // No driver, wood fiber
	{
		peat_drn_ann = 79;
	}
	else if (forestmodel_data == 0 && plant_data == 3) // No driver, other plantation
	{
		peat_drn_ann = 60;
	}
	else
	{
		peat_drn_ann = 0;
	}
	peat_drain_total = (15 - lossyr) * peat_drn_ann;
    return peat_drain_total;
}


float* def_variables(int ecozone, int forestmodel_data, int ifl, int climate, int plant_data, int lossyr)
{
	// returns Cf, CO2, CH4, N2O, peatburn, peat_drain_total
	float Cf;
	float CO2;
	float CH4;
	float N2O;
	float peatburn;
	float peat_drain_total;

	if ((forestmodel_data == 1) || (forestmodel_data == 2)) // Commodities or shifting ag.
	{
		if (ecozone == 2) // Commodities/shifting ag, boreal
		{
			Cf = 0.59;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (15 - lossyr) * 36;

		}
		else if (ecozone == 3 )// Commodities/shifting ag, temperate
		{
			Cf = 0.51;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (15 - lossyr) * 31;

		}
		else if (ecozone == 1) // Commodities/shifting ag, tropics
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn = 163;
			peat_drain_total = peat_drn_ann_calc(forestmodel_data, plant_data, lossyr);

			if (ifl > 0)    // Commodities/shifting ag, tropics, in IFL
			{
				Cf = 0.36;
			}
			else            // Commodities/shifting ag, tropics, outside IFL
			{
				Cf = 0.55;
			}

		}

		else
		{
			Cf = 0;
		}
	}

	else if (forestmodel_data == 3) // Forestry
	{

		if (ecozone == 2) // Forestry, boreal
		{
			Cf = 0.33;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (15 - lossyr) * 3;

		}
		else if (ecozone == 3 )// Forestry, temperate
		{
			Cf = 0.62;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (15 - lossyr) * 12;

		}
		else if (ecozone == 1) // Forestry, tropics
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn = 163;
			peat_drain_total = peat_drn_ann_calc(forestmodel_data, plant_data, lossyr);

			if (ifl > 0)
			{
				Cf = 0.36;      // Forestry, tropics, in IFL
			}
			else
			{
				Cf = 0.55;      // Forestry, tropics, outside IFL
			}

		}

		else
		{
			Cf = 0;
		}
	}

	else if (forestmodel_data == 4) // Wildfire
	{

		if (ecozone == 2) // Wildfire, boreal
		{
			Cf = 0.59;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (15 - lossyr) * 3;             // This was 16 in Sam's model. Should it be 16 or 15??????

		}
		else if (ecozone == 3 )// Wildfire, temperate
		{
			Cf = 0.51;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (15 - lossyr) * 12;            // This was 16 in Sam's model. Should it be 16 or 15??????

		}
		else if (ecozone == 1) // Wildfire, tropics
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn = 371;
			peat_drain_total = peat_drn_ann_calc(forestmodel_data, plant_data, lossyr);

			if (ifl > 0)        // Wildfire, tropics, in IFL
			{
				Cf = 0.36;
			}
			else                // Wildfire, tropics, outside IFL
			{
				Cf = 0.55;
			}

		}

		else
		{
			Cf = 0;
		}
	}
	static float def_variables[6] = {Cf, CO2, CH4, N2O, peatburn, peat_drain_total};

	return def_variables;

}