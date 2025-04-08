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


	if ((forestmodel_data == 1) || (forestmodel_data == 2) || (forestmodel_data == 5)       // commodities, shifting ag., or urbanization
	{
		if (ecozone == boreal)      // commodities/shifting ag/urbanization, boreal
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
		else if (ecozone == temperate)      // commodities, shifting ag., or urbanization, temperate
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
		else    // commodities, shifting ag., or urbanization, tropics (or no boreal/temperate/tropical assignment)
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn_CO2_only = 264;
			peatburn_CH4_only = 88;

            if (plant_data == 1)    // commodities, shifting ag., or urbanization, tropics, oil palm
            {
                peat_drain_annual_CO2_only = 43;
                peat_drain_annual_CH4_only = 1.2;
			    peat_drain_annual_N2O_only = 1.0;
            }
            else if (plant_data == 2)   // commodities, shifting ag., or urbanization, tropics, wood fiber
            {
                peat_drain_annual_CO2_only = 76;
                peat_drain_annual_CH4_only = 1.3;
			    peat_drain_annual_N2O_only = 2.1;
            }
            else    // commodities, shifting ag., or urbanization, tropics, other plantation or no plantation
            {
                peat_drain_annual_CO2_only = 58;
                peat_drain_annual_CH4_only = 1.3;
			    peat_drain_annual_N2O_only = 2.1;
            }
            peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
            peat_drain_total_CH4_only = (model_years - lossyr) * peat_drain_annual_CH4_only;
			peat_drain_total_N2O_only = (model_years - lossyr) * peat_drain_annual_N2O_only;

			if (ifl > 0)    // commodities, shifting ag., or urbanization, tropics, in IFL
			{
				Cf = 0.36;
			}
			else            // commodities, shifting ag., or urbanization, tropics, outside IFL
			{
				Cf = 0.55;
			}
		}
	}

	else if (forestmodel_data == 3) // forestry
	{
		if (ecozone == boreal) // forestry, boreal
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
		else if (ecozone == temperate)// forestry, temperate
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
		else  // forestry, tropics (or no boreal/temperate/tropical assignment)
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn_CO2_only = 264;
			peatburn_CH4_only = 88;

			if (plant_data == 1) // forestry, tropics, oil palm
            {
                peat_drain_annual_CO2_only = 43;
                peat_drain_annual_CH4_only = 1.2;
			    peat_drain_annual_N2O_only = 1.0;
            }
            else if (plant_data == 2) // forestry, tropics, wood fiber
            {
                peat_drain_annual_CO2_only = 76;
                peat_drain_annual_CH4_only = 1.3;
			    peat_drain_annual_N2O_only = 2.1;
            }
            else // forestry, tropics, other plantation or no plantation
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
				Cf = 0.36;      // forestry, tropics, in IFL
			}
			else
			{
				Cf = 0.55;      // forestry, tropics, outside IFL
			}
		}
	}
	else if (forestmodel_data == 4) // wildfire
	{
		if (ecozone == boreal) // wildfire, boreal
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
		else if (ecozone == temperate)// wildfire, temperate
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
		else // wildfire, tropics (or no boreal/temperate/tropical assignment)
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn_CO2_only = 601;
			peatburn_CH4_only = 200;

		    if (plant_data == 1) // wildfire, tropics, oil palm
            {
                peat_drain_annual_CO2_only = 43;
                peat_drain_annual_CH4_only = 1.2;
			    peat_drain_annual_N2O_only = 1.0;
            }
            else if (plant_data == 2) // wildfire, tropics, wood fiber
            {
                peat_drain_annual_CO2_only = 76;
                peat_drain_annual_CH4_only = 1.3;
			    peat_drain_annual_N2O_only = 2.1;
            }
            else // wildfire, tropics, other plantation or no plantation
            {
                peat_drain_annual_CO2_only = 58;
                peat_drain_annual_CH4_only = 1.3;
			    peat_drain_annual_N2O_only = 2.1;
            }
            peat_drain_total_CO2_only = (model_years - lossyr) * peat_drain_annual_CO2_only;
            peat_drain_total_CH4_only = (model_years - lossyr) * peat_drain_annual_CH4_only;
			peat_drain_total_N2O_only = (model_years - lossyr) * peat_drain_annual_N2O_only;

			if (ifl > 0)        // wildfire, tropics, in IFL
			{
				Cf = 0.36;
			}
			else                // wildfire, tropics, outside IFL
			{
				Cf = 0.55;
			}
		}
	}
	else  // no driver
	{
		if (ecozone == boreal) // no driver, boreal
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
		else if (ecozone == temperate)// no driver, temperate
		{
			Cf = 0.45;   //TODO: This was 0.62 (post-logging slash and burn) but we updated to 0.45 (land clearing fire) b/c salvage logging is classified as forest management
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
		else // no driver, tropics (or no boreal/temperate/tropical assignment)
		{
			CO2 = 1580;
			CH4 = 6.8;
			N2O = 0.2;
			peatburn_CO2_only = 264;
			peatburn_CH4_only = 88;

			if (plant_data == 1) // no driver, tropics, oil palm
            {
                peat_drain_annual_CO2_only = 43;
                peat_drain_annual_CH4_only = 1.2;
			    peat_drain_annual_N2O_only = 1;
            }
            else if (plant_data == 2) // no driver, tropics, wood fiber
            {
                peat_drain_annual_CO2_only = 76;
                peat_drain_annual_CH4_only = 1.3;
			    peat_drain_annual_N2O_only = 2.1;
            }
            else // no driver, tropics, other plantation or no plantation
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
				Cf = 0.36;      // no driver, tropics, in IFL
			}
			else
			{
				Cf = 0.55;      // no driver, tropics, outside IFL
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