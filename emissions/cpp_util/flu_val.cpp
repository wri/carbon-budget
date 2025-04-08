// These values are found in the powerpoint with the emissions model decision trees:
// https://onewri-my.sharepoint.com/:p:/r/personal/david_gibbs_wri_org/Documents/Documents/Projects/Carbon%20model%20phase%201/carbon_budget_flowchart_v8.pptx?d=w701e66825ac24e23a9ba6c7a408f84ad&csf=1&web=1&e=4ufGOx

#include <map>
#include <iostream>
#include <stdlib.h>
using namespace std;

//This function is specifically for the commodity-driven deforestation driver class which has several possible flu_values based on eco zone and climate zone
float flu_val(int climate_zone, int eco_zone)
{
	float flu_value;
	if ((climate_zone == 2) || (climate_zone == 4) || (climate_zone == 6) || (climate_zone == 8) || (climate_zone == 12)) // Dry
	{
		if (eco_zone == 1) // Dry, tropics
		{
			flu_value = 0.92;
		}
		else if ((eco_zone == 2) || (eco_zone == 3)) // Dry, boreal/temperate
		{
			flu_value = 0.77;
		}
		else
		{
			flu_value = 0;
		}
	}
	else if ((climate_zone == 1) || (climate_zone == 3) || (climate_zone == 5) || (climate_zone == 7) || (climate_zone == 10) || (climate_zone == 11)) // Wet
	{
		if (eco_zone == 1) // Wet, tropics
		{
			flu_value = 0.83;
		}
		else if ((eco_zone == 2) || (eco_zone == 3)) // Wet, boreal/temperate
		{
			flu_value = 0.70;
		}
		else
		{
			flu_value = 0;
		}
	}
	else if (climate_zone == 9) // Montane
	{
		if (eco_zone == 1) // Montane, tropics
		{
			flu_value = 0.88;
		}
		else if ((eco_zone == 2) || (eco_zone == 3)) // Montane, boreal/temperate
		{
			flu_value = 0.74;
		}
		else
		{
			flu_value = 0;
		}
	}
	else
	{
		flu_value = 0;
	}

	return flu_value;
}