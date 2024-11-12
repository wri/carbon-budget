#include <map>
#include <iostream>
#include <stdlib.h>
using namespace std;

float flu_val(int climate_zone, int eco_zone)
{
	float flu_value;
	if ((climate_zone == 2) || (climate_zone == 4) || (climate_zone == 6) || (climate_zone == 8) || (climate_zone == 12)) // Dry
	{
		if (eco_zone == 1) // Dry, tropics
		{
            flu_value = 0.58;
			//flu_value = 0.92; //TODO:Update with new Flu after testing
		}
		else if ((eco_zone == 2) || (eco_zone == 3)) // Dry, boreal/temperate
		{
			flu_value = 0.8
			//flu_value = 0.77; //TODO:Update with new Flu after testing
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
            flu_value = 0.48
			//flu_value = 0.83; //TODO:Update with new Flu after testing
		}
		else if ((eco_zone == 2) || (eco_zone == 3)) // Wet, boreal/temperate
		{
            flu_value = 0.69;
			//flu_value = 0.70; //TODO:Update with new Flu after testing
		}
		else
		{
			flu_value = 0;
		}
	}
	else if (climate_zone == 9) // Montane  #TODO: What do we do for montane climate domain?
	{
		if (eco_zone == 1) // Montane, tropics
		{
			flu_value = 0.64;
		}
		else if ((eco_zone == 2) || (eco_zone == 3)) // Montane, boreal/temperate
		{
			flu_value = 0.75;
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