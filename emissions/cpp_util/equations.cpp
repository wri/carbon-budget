// Provides most of the constants for the gross emissions calculation.
// These values are found in the powerpoint with the emissions model decision trees:
// https://onewri-my.sharepoint.com/:p:/r/personal/david_gibbs_wri_org/Documents/Documents/Projects/Carbon%20model%20phase%201/carbon_budget_flowchart_v8.pptx?d=w701e66825ac24e23a9ba6c7a408f84ad&csf=1&web=1&e=4ufGOx

#include <map>
#include <iostream>
#include <stdlib.h>

using namespace std;

void def_variables(float *q, int ecozone, int forestmodel_data, int ifl, int climate, int plant_data, int lossyr)
{

	int model_years;    // How many loss years are in the model
    model_years = 24;

	int tropical;       // The ecozone code for the tropics
    tropical = 1;
    int temperate;      // The ecozone code for the temperate zone
    temperate = 3;
    int boreal;         // The ecozone code for the boreal zone
    boreal = 2;

	// Initiates Cf, CO2, CH4, N2O, peatburn, peat_drain_total.
	// peat_drain_annual and peat_drain_total has CO2, CH4, and N2O components.
	// peatburn has CO2 and CH4 components but not a N2O component because there are no N2O emissions from burning peat.
	// They are calculated separately and passed back to the main script as separate values.
	float Cf;
	float CO2;
	float CH4;
	float N2O;
	float peatburn_CO2_only;
	float peatburn_CH4_only;          // Note: there are no N2O emissions from burning peat
	float peat_drain_annual_CO2_only;
	float peat_drain_annual_CH4_only;
	float peat_drain_annual_N2O_only;
	float peat_drain_total_CO2_only;
	float peat_drain_total_CH4_only;
	float peat_drain_total_N2O_only;


	if ((forestmodel_data == 1) || (forestmodel_data == 2) || (forestmodel_data == 3) || (forestmodel_data == 6)) // permanent ag, hard commodities, shifting cultivation, settlements & infrastructure
	{
		if (ecozone == boreal) // permanent ag, hard commodities, shifting cultivation, settlements & infrastructure
		{
			Cf = 0.59;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn_CO2_only = 446;
			peatburn_CH4_only = 82;
			peat_drain_annual_CO2_only = 1.8;
			peat_drain_annual_CH4_only = 0.33;
			peat_drain_annual_N2O_only = 0.19;
			peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
			peat_drain_total_CH4_only = (model_years - lossyr) * peat_drain_annual_CH4_only;
			peat_drain_total_N2O_only = (model_years - lossyr) * peat_drain_annual_N2O_only;
		}
		else if (ecozone == temperate)// permanent ag, hard commodities, shifting cultivation, settlements & infrastructure
		{
			Cf = 0.51;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn_CO2_only = 446;
			peatburn_CH4_only = 82;
			peat_drain_annual_CO2_only = 11;
			peat_drain_annual_CH4_only = 0.21;
			peat_drain_annual_N2O_only = 2.4;
			peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
			peat_drain_total_CH4_only = (model_years - lossyr) * peat_drain_annual_CH4_only;
			peat_drain_total_N2O_only = (model_years - lossyr) * peat_drain_annual_N2O_only;
		}
		else // permanent ag, hard commodities, shifting cultivation, settlements & infrastructure, tropics (or no boreal/temperate/tropical assignment)
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn_CO2_only = 264;
			peatburn_CH4_only = 88;

            if (plant_data == 1)  // permanent ag, hard commodities, shifting cultivation, settlements & infrastructure, tropics, oil palm
            {
                peat_drain_annual_CO2_only = 43;
                peat_drain_annual_CH4_only = 1.2;
			    peat_drain_annual_N2O_only = 1.0;
            }
            else if (plant_data == 2) // permanent ag, hard commodities, shifting cultivation, settlements & infrastructure, tropics, wood fiber
            {
                peat_drain_annual_CO2_only = 76;
                peat_drain_annual_CH4_only = 1.3;
			    peat_drain_annual_N2O_only = 2.1;
            }
            else // permanent ag, hard commodities, shifting cultivation, settlements & infrastructure, tropics, other plantation or no plantation
            {
                peat_drain_annual_CO2_only = 58;
                peat_drain_annual_CH4_only = 1.3;
			    peat_drain_annual_N2O_only = 2.1;
            }
            peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
            peat_drain_total_CH4_only = (model_years - lossyr) * peat_drain_annual_CH4_only;
			peat_drain_total_N2O_only = (model_years - lossyr) * peat_drain_annual_N2O_only;

			if (ifl > 0)    // permanent ag, hard commodities, shifting cultivation, settlements & infrastructure, tropics, in IFL
			{
				Cf = 0.36;
			}
			else            // permanent ag, hard commodities, shifting cultivation, settlements & infrastructure, tropics, outside IFL
			{
				Cf = 0.55;
			}
		}
	}

	else if (forestmodel_data == 4) // Logging
	{
		if (ecozone == boreal) // Logging, boreal
		{
			Cf = 0.33;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn_CO2_only = 446;
			peatburn_CH4_only = 82;
			peat_drain_annual_CO2_only = 1.8;
			peat_drain_annual_CH4_only = 0.33;
			peat_drain_annual_N2O_only = 0.19;
			peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
			peat_drain_total_CH4_only = (model_years - lossyr) * peat_drain_annual_CH4_only;
			peat_drain_total_N2O_only = (model_years - lossyr) * peat_drain_annual_N2O_only;
		}
		else if (ecozone == temperate)// Logging, temperate
		{
			Cf = 0.62;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn_CO2_only = 446;
			peatburn_CH4_only = 82;
			peat_drain_annual_CO2_only = 11;
			peat_drain_annual_CH4_only = 0.21;
			peat_drain_annual_N2O_only = 2.4;
			peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
			peat_drain_total_CH4_only = (model_years - lossyr) * peat_drain_annual_CH4_only;
			peat_drain_total_N2O_only = (model_years - lossyr) * peat_drain_annual_N2O_only;
		}
		else  // Logging, tropics (or no boreal/temperate/tropical assignment)
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn_CO2_only = 264;
			peatburn_CH4_only = 88;

			if (plant_data == 1) // Logging, tropics, oil palm
            {
                peat_drain_annual_CO2_only = 43;
                peat_drain_annual_CH4_only = 1.2;
			    peat_drain_annual_N2O_only = 1.0;
            }
            else if (plant_data == 2) // Logging, tropics, wood fiber
            {
                peat_drain_annual_CO2_only = 76;
                peat_drain_annual_CH4_only = 1.3;
			    peat_drain_annual_N2O_only = 2.1;
            }
            else // Logging, tropics, other plantation or no plantation
            {
                peat_drain_annual_CO2_only = 58;
                peat_drain_annual_CH4_only = 1.3;
			    peat_drain_annual_N2O_only = 2.1;
            }
            peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
            peat_drain_total_CH4_only = (model_years - lossyr) * peat_drain_annual_CH4_only;
			peat_drain_total_N2O_only = (model_years - lossyr) * peat_drain_annual_N2O_only;

			if (ifl > 0)
			{
				Cf = 0.36;      // Logging, tropics, in IFL
			}
			else
			{
				Cf = 0.55;      // Logging, tropics, outside IFL
			}
		}
	}

	else if (forestmodel_data == 5) // Wildfire
	{
		if (ecozone == boreal) // Wildfire, boreal
		{
			Cf = 0.59;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn_CO2_only = 446;
			peatburn_CH4_only = 82;
			peat_drain_annual_CO2_only = 1.8;
			peat_drain_annual_CH4_only = 0.33;
			peat_drain_annual_N2O_only = 0.19;
			peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
			peat_drain_total_CH4_only = (model_years - lossyr) * peat_drain_annual_CH4_only;
			peat_drain_total_N2O_only = (model_years - lossyr) * peat_drain_annual_N2O_only;
		}
		else if (ecozone == temperate)// Wildfire, temperate
		{
			Cf = 0.51;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn_CO2_only = 446;
			peatburn_CH4_only = 82;
			peat_drain_annual_CO2_only = 11;
			peat_drain_annual_CH4_only = 0.21;
			peat_drain_annual_N2O_only = 2.4;
			peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
			peat_drain_total_CH4_only = (model_years - lossyr) * peat_drain_annual_CH4_only;
			peat_drain_total_N2O_only = (model_years - lossyr) * peat_drain_annual_N2O_only;
		}
		else // Wildfire, tropics (or no boreal/temperate/tropical assignment)
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn_CO2_only = 601;
			peatburn_CH4_only = 200;

		    if (plant_data == 1) // Wildfire, tropics, oil palm
            {
                peat_drain_annual_CO2_only = 43;
                peat_drain_annual_CH4_only = 1.2;
			    peat_drain_annual_N2O_only = 1.0;
            }
            else if (plant_data == 2) // Wildfire, tropics, wood fiber
            {
                peat_drain_annual_CO2_only = 76;
                peat_drain_annual_CH4_only = 1.3;
			    peat_drain_annual_N2O_only = 2.1;
            }
            else // Wildfire, tropics, other plantation or no plantation
            {
                peat_drain_annual_CO2_only = 58;
                peat_drain_annual_CH4_only = 1.3;
			    peat_drain_annual_N2O_only = 2.1;
            }
            peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
            peat_drain_total_CH4_only = (model_years - lossyr) * peat_drain_annual_CH4_only;
			peat_drain_total_N2O_only = (model_years - lossyr) * peat_drain_annual_N2O_only;

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

	else if (forestmodel_data == 7) // Other natural disturbances
	{
		if (ecozone == boreal) // Other natural disturbances, boreal
		{
			Cf = 0.34;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn_CO2_only = 446;
			peatburn_CH4_only = 82;
			peat_drain_annual_CO2_only = 1.8;
			peat_drain_annual_CH4_only = 0.33;
			peat_drain_annual_N2O_only = 0.19;
			peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
			peat_drain_total_CH4_only = (model_years - lossyr) * peat_drain_annual_CH4_only;
			peat_drain_total_N2O_only = (model_years - lossyr) * peat_drain_annual_N2O_only;
		}
		else if (ecozone == temperate)// Other natural disturbances, temperate
		{
			Cf = 0.45;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn_CO2_only = 446;
			peatburn_CH4_only = 82;
			peat_drain_annual_CO2_only = 11;
			peat_drain_annual_CH4_only = 0.21;
			peat_drain_annual_N2O_only = 2.4;
			peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
			peat_drain_total_CH4_only = (model_years - lossyr) * peat_drain_annual_CH4_only;
			peat_drain_total_N2O_only = (model_years - lossyr) * peat_drain_annual_N2O_only;
		}
		else  // Other natural disturbances, tropics (or no boreal/temperate/tropical assignment)
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn_CO2_only = 264;
			peatburn_CH4_only = 88;

			if (plant_data == 1) // Other natural disturbances, tropics, oil palm
            {
                peat_drain_annual_CO2_only = 43;
                peat_drain_annual_CH4_only = 1.2;
			    peat_drain_annual_N2O_only = 1.0;
            }
            else if (plant_data == 2) // Other natural disturbances, tropics, wood fiber
            {
                peat_drain_annual_CO2_only = 76;
                peat_drain_annual_CH4_only = 1.3;
			    peat_drain_annual_N2O_only = 2.1;
            }
            else // Other natural disturbances, tropics, other plantation or no plantation
            {
                peat_drain_annual_CO2_only = 58;
                peat_drain_annual_CH4_only = 1.3;
			    peat_drain_annual_N2O_only = 2.1;
            }
            peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
            peat_drain_total_CH4_only = (model_years - lossyr) * peat_drain_annual_CH4_only;
			peat_drain_total_N2O_only = (model_years - lossyr) * peat_drain_annual_N2O_only;

			if (ifl > 0)
			{
				Cf = 0.36;      // Other natural disturbances, tropics, in IFL
			}
			else
			{
				Cf = 0.55;      // Other natural disturbances, tropics, outside IFL
			}
		}
	}

	else  // No driver-- same as other natural disturbances
	{
		if (ecozone == boreal) // No driver, boreal
		{
			Cf = 0.34;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn_CO2_only = 446;
			peatburn_CH4_only = 82;
			peat_drain_annual_CO2_only = 1.8;
			peat_drain_annual_CH4_only = 0.33;
			peat_drain_annual_N2O_only = 0.19;
			peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
			peat_drain_total_CH4_only = (model_years - lossyr) * peat_drain_annual_CH4_only;
			peat_drain_total_N2O_only = (model_years - lossyr) * peat_drain_annual_N2O_only;
		}
		else if (ecozone == temperate)// No driver, temperate
		{
			Cf = 0.45;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn_CO2_only = 446;
			peatburn_CH4_only = 82;
			peat_drain_annual_CO2_only = 11;
			peat_drain_annual_CH4_only = 0.21;
			peat_drain_annual_N2O_only = 2.4;
			peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
			peat_drain_total_CH4_only = (model_years - lossyr) * peat_drain_annual_CH4_only;
			peat_drain_total_N2O_only = (model_years - lossyr) * peat_drain_annual_N2O_only;
		}
		else // No driver, tropics (or no boreal/temperate/tropical assignment)
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn_CO2_only = 264;
			peatburn_CH4_only = 88;

			if (plant_data == 1) // No driver, tropics, oil palm
            {
                peat_drain_annual_CO2_only = 43;
                peat_drain_annual_CH4_only = 1.2;
			    peat_drain_annual_N2O_only = 1;
            }
            else if (plant_data == 2) // No driver, tropics, wood fiber
            {
                peat_drain_annual_CO2_only = 76;
                peat_drain_annual_CH4_only = 1.3;
			    peat_drain_annual_N2O_only = 2.1;
            }
            else // No driver, tropics, other plantation or no plantation
            {
                peat_drain_annual_CO2_only = 58;
                peat_drain_annual_CH4_only = 1.3;
			    peat_drain_annual_N2O_only = 2.1;
            }
            peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
            peat_drain_total_CH4_only = (model_years - lossyr) * peat_drain_annual_CH4_only;
			peat_drain_total_N2O_only = (model_years - lossyr) * peat_drain_annual_N2O_only;

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
    q[5] = peatburn_CH4_only;
    q[6] = peat_drain_total_CO2_only;
    q[7] = peat_drain_total_CH4_only;
    q[8] = peat_drain_total_N2O_only;

}