// Provides most of the constants for the gross emissions calculation.
// These are basically found in the table preceding the emissions model decision trees.

#include <map>
#include <iostream>
#include <stdlib.h>

//// http://www.math.uaa.alaska.edu/~afkjm/csce211/handouts/SeparateCompilation.pdf
//#ifndef CONSTANTS_H
//#define CONSTANTS_H
////#include "constants.h"
//#endif

using namespace std;

void def_variables(float *q, int ecozone, int forestmodel_data, int ifl, int climate, int plant_data, int lossyr)
{

	int model_years;    // How many loss years are in the model
    model_years = 23;

	int tropical;       // The ecozone code for the tropics
    tropical = 1;
    int temperate;      // The ecozone code for the temperate zone
    temperate = 3;
    int boreal;         // The ecozone code for the boreal zone
    boreal = 2;

//    int model_years;    // How many loss years are in the model
//    model_years = constants::model_years;
//
//	int tropical;       // The ecozone code for the tropics
//    tropical = constants::tropical;
//    int temperate;      // The ecozone code for the temperate zone
//    temperate = constants::temperate;
//    int boreal;         // The ecozone code for the boreal zone
//    boreal = constants::boreal;

	// instantiates Cf, CO2, CH4, N2O, peatburn, peat_drain_total.
	// peatburn and peat_drain_annual both have CO2 and non-CO2 components. They are calculated separately and
	// passed back to the main script as separate values.
	float Cf;
	float CO2;
	float CH4;
	float N2O;
	float peatburn_CO2_only;
	float peatburn_non_CO2;
	float peat_drain_annual_CO2_only;
	float peat_drain_annual_non_CO2;
	float peat_drain_total_CO2_only;
	float peat_drain_total_non_CO2;


    //TODO: Change driver coefficients starting here
	if ((forestmodel_data == 1) || (forestmodel_data == 2) || (forestmodel_data == 5)) // Commodities, shifting ag., or urbanization
	{
		if (ecozone == boreal) // Commodities/shifting ag/urbanization, boreal
		{
			Cf = 0.59;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn_CO2_only = 446;
			peatburn_non_CO2 = 82;
			peat_drain_annual_CO2_only = 2;
			peat_drain_annual_non_CO2 = 1;
			peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
			peat_drain_total_non_CO2 = (model_years - lossyr) * peat_drain_annual_non_CO2;
		}
		else if (ecozone == temperate)// Commodities/shifting ag/urbanization, temperate
		{
			Cf = 0.51;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn_CO2_only = 446;
			peatburn_non_CO2 = 82;
			peat_drain_annual_CO2_only = 11;
			peat_drain_annual_non_CO2 = 3;
			peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
			peat_drain_total_non_CO2 = (model_years - lossyr) * peat_drain_annual_non_CO2;
		}
		else // Commodities/shifting ag/urbanization, tropics (or no boreal/temperate/tropical assignment)
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn_CO2_only = 264;
			peatburn_non_CO2 = 88;

            if (plant_data == 1)  // Commodities/shifting ag/urbanization, tropics, oil palm
            {
                peat_drain_annual_CO2_only = 43;
                peat_drain_annual_non_CO2 = 2;
            }
            else if (plant_data == 2) // Commodities/shifting ag/urbanization, tropics, wood fiber
            {
                peat_drain_annual_CO2_only = 76;
                peat_drain_annual_non_CO2 = 3;
            }
            else // Commodities/shifting ag/urbanization, tropics, other plantation or no plantation
            {
                peat_drain_annual_CO2_only = 58;
                peat_drain_annual_non_CO2 = 3;
            }
            peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
            peat_drain_total_non_CO2 = (model_years - lossyr) * peat_drain_annual_non_CO2;

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
		if (ecozone == boreal) // Forestry, boreal
		{
			Cf = 0.33;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn_CO2_only = 446;
			peatburn_non_CO2 = 82;
			peat_drain_annual_CO2_only = 2;
			peat_drain_annual_non_CO2 = 1;
			peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
			peat_drain_total_non_CO2 = (model_years - lossyr) * peat_drain_annual_non_CO2;
		}
		else if (ecozone == temperate)// Forestry, temperate
		{
			Cf = 0.62;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn_CO2_only = 446;
			peatburn_non_CO2 = 82;
			peat_drain_annual_CO2_only = 11;
			peat_drain_annual_non_CO2 = 3;
			peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
			peat_drain_total_non_CO2 = (model_years - lossyr) * peat_drain_annual_non_CO2;
		}
		else  // Forestry, tropics (or no boreal/temperate/tropical assignment)
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn_CO2_only = 264;
			peatburn_non_CO2 = 88;

			if (plant_data == 1) // Forestry, tropics, oil palm
            {
                peat_drain_annual_CO2_only = 43;
                peat_drain_annual_non_CO2 = 2;
            }
            else if (plant_data == 2) // Forestry, tropics, wood fiber
            {
                peat_drain_annual_CO2_only = 76;
                peat_drain_annual_non_CO2 = 3;
            }
            else // Forestry, tropics, other plantation or no plantation
            {
                peat_drain_annual_CO2_only = 58;
                peat_drain_annual_non_CO2 = 3;
            }
            peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
            peat_drain_total_non_CO2 = (model_years - lossyr) * peat_drain_annual_non_CO2;

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
		if (ecozone == boreal) // Wildfire, boreal
		{
			Cf = 0.59;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn_CO2_only = 446;
			peatburn_non_CO2 = 82;
			peat_drain_annual_CO2_only = 2;
			peat_drain_annual_non_CO2 = 1;
			peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
			peat_drain_total_non_CO2 = (model_years - lossyr) * peat_drain_annual_non_CO2;
		}
		else if (ecozone == temperate)// Wildfire, temperate
		{
			Cf = 0.51;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn_CO2_only = 446;
			peatburn_non_CO2 = 82;
			peat_drain_annual_CO2_only = 11;
			peat_drain_annual_non_CO2 = 3;
			peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
			peat_drain_total_non_CO2 = (model_years - lossyr) * peat_drain_annual_non_CO2;
		}
		else // Wildfire, tropics (or no boreal/temperate/tropical assignment)
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn_CO2_only = 601;
			peatburn_non_CO2 = 200;

		    if (plant_data == 1) // Wildfire, tropics, oil palm
            {
                peat_drain_annual_CO2_only = 43;
                peat_drain_annual_non_CO2 = 2;
            }
            else if (plant_data == 2) // Wildfire, tropics, wood fiber
            {
                peat_drain_annual_CO2_only = 76;
                peat_drain_annual_non_CO2 = 3;
            }
            else // Wildfire, tropics, other plantation or no plantation
            {
                peat_drain_annual_CO2_only = 58;
                peat_drain_annual_non_CO2 = 3;
            }
            peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
            peat_drain_total_non_CO2 = (model_years - lossyr) * peat_drain_annual_non_CO2;

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
		if (ecozone == boreal) // No driver, boreal
		{
			Cf = 0.33;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn_CO2_only = 446;
			peatburn_non_CO2 = 82;
			peat_drain_annual_CO2_only = 2;
			peat_drain_annual_non_CO2 = 1;
			peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
			peat_drain_total_non_CO2 = (model_years - lossyr) * peat_drain_annual_non_CO2;
		}
		else if (ecozone == temperate)// No driver, temperate
		{
			Cf = 0.62;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn_CO2_only = 446;
			peatburn_non_CO2 = 82;
			peat_drain_annual_CO2_only = 11;
			peat_drain_annual_non_CO2 = 3;
			peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
			peat_drain_total_non_CO2 = (model_years - lossyr) * peat_drain_annual_non_CO2;
		}
		else // No driver, tropics (or no boreal/temperate/tropical assignment)
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn_CO2_only = 264;
			peatburn_non_CO2 = 88;

			if (plant_data == 1) // Forestry, tropics, oil palm
            {
                peat_drain_annual_CO2_only = 43;
                peat_drain_annual_non_CO2 = 2;
            }
            else if (plant_data == 2) // Forestry, tropics, wood fiber
            {
                peat_drain_annual_CO2_only = 76;
                peat_drain_annual_non_CO2 = 3;
            }
            else // Forestry, tropics, other plantation or no plantation
            {
                peat_drain_annual_CO2_only = 58;
                peat_drain_annual_non_CO2 = 3;
            }
            peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
            peat_drain_total_non_CO2 = (model_years - lossyr) * peat_drain_annual_non_CO2;

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
    q[0] = Cf;
    q[1] = CO2;
    q[2] = CH4;
    q[3] = N2O;
    q[4] = peatburn_CO2_only;
    q[5] = peatburn_non_CO2;
    q[6] = peat_drain_total_CO2_only;
    q[7] = peat_drain_total_non_CO2;
}