#include <map>
#include <iostream>
#include <stdlib.h>
using namespace std;

float* def_variables(int ecozone, int forestmodel_data, int ifl, int climate, int plant_data, int lossyr)
{
	// returns Cf, CO2, CH4, N2O, peatburn, peat_drain_total
	float Cf;
	float CO2;
	float CH4;
	float N2O;
	float peatburn;
	float peat_drain_annual;
	float peat_drain_total;

	if ((forestmodel_data == 1) || (forestmodel_data == 2) || (forestmodel_data == 5)) // Commodities or shifting ag.
	{
		if (ecozone == 2) // Commodities/shifting ag/urbanization, boreal
		{
			Cf = 0.59;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (15 - lossyr) * 36;

		}
		else if (ecozone == 3 )// Commodities/shifting ag/urbanization, temperate
		{
			Cf = 0.51;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (15 - lossyr) * 31;

		}
		else if (ecozone == 1) // Commodities/shifting ag/urbanization, tropics
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn = 163;

            if (plant_data == 1)  // Commodities/shifting ag/urbanization, tropics, oil palm
            {
                peat_drain_annual = 47;
            }
            else if (plant_data == 2) // Commodities/shifting ag/urbanization, tropics, wood fiber
            {
                peat_drain_annual = 80;
            }
            else if (plant_data == 3) // Commodities/shifting ag/urbanization, tropics, other plantation
            {
                peat_drain_annual = 62;
            }
            peat_drain_total = (15 - lossyr) * peat_drain_annual;

			if (ifl > 0)    // Commodities/shifting ag/urbanization, tropics, in IFL
			{
				Cf = 0.36;
			}
			else            // Commodities/shifting ag/urbanization, tropics, outside IFL
			{
				Cf = 0.55;
			}

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

			if (plant_data == 1) // Forestry, tropics, oil palm
            {
                peat_drain_annual = 45;
            }
            else if (plant_data == 2) // Forestry, tropics, wood fiber
            {
                peat_drain_annual = 79;
            }
            else if (plant_data == 3) // Forestry, tropics, other plantation
            {
                peat_drain_annual = 60;
            }
            peat_drain_total = (15 - lossyr) * peat_drain_annual;

			if (ifl > 0)
			{
				Cf = 0.36;      // Forestry, tropics, in IFL
			}
			else
			{
				Cf = 0.55;      // Forestry, tropics, outside IFL
			}

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
			peat_drain_total = (15 - lossyr) * 3;

		}
		else if (ecozone == 3 )// Wildfire, temperate
		{
			Cf = 0.51;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (15 - lossyr) * 12;

		}
		else if (ecozone == 1) // Wildfire, tropics
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn = 371;

		    if (plant_data == 1) // Wildfire, tropics, oil palm
            {
                peat_drain_annual = 45;
            }
            else if (plant_data == 2) // Wildfire, tropics, wood fiber
            {
                peat_drain_annual = 79;
            }
            else if (plant_data == 3) // Wildfire, tropics, other plantation
            {
                peat_drain_annual = 60;
            }
            peat_drain_total = (15 - lossyr) * peat_drain_annual;

			if (ifl > 0)        // Wildfire, tropics, in IFL
			{
				Cf = 0.36;
			}
			else                // Wildfire, tropics, outside IFL
			{
				Cf = 0.55;
			}

		}
	}

	else  // No driver-- same as forestry
	{
		if (ecozone == 2) // No driver, boreal
		{
			Cf = 0.33;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (15 - lossyr) * 3;

		}
		else if (ecozone == 3 )// No driver, temperate
		{
			Cf = 0.62;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (15 - lossyr) * 12;

		}
		else if (ecozone == 1) // No driver, tropics
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn = 163;

			if (plant_data == 1) // No driver, tropics, oil palm
            {
                peat_drain_annual = 45;
            }
            else if (plant_data == 2) // No driver, tropics, wood fiber
            {
                peat_drain_annual = 79;
            }
            else if (plant_data == 3) // No driver, tropics, other plantation
            {
                peat_drain_annual = 60;
            }
            peat_drain_total = (15 - lossyr) * peat_drain_annual;

			if (ifl > 0)
			{
				Cf = 0.36;      // No driver, tropics, in IFL
			}
			else
			{
				Cf = 0.55;      // No driver, tropics, outside IFL
			}

		}
	}

	static float def_variables[6] = {Cf, CO2, CH4, N2O, peatburn, peat_drain_total};

	return def_variables;

}