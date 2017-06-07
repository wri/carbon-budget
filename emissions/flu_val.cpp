#include <map>
#include <iostream>
#include <stdlib.h>
using namespace std;

float flu(int climate_zone, int eco_zone)
{
	float flu_value;
	if ((climate_zone == 2) || (climate_zone == 4) || (climate_zone == 6) || (climate_zone == 8) || (climate_zone == 12)) // dry
	{
		if (eco_zone == 1) // dry, tropics
		{
			flu_value = .58;
		}
		
		else if ((eco_zone == 2) || (eco_zone == 3)) // dry boreal, temperate
		{
			flu_value = .8;
		}
		else
		{
			flu_value = 0;
		}
	}	
	else if ((climate_zone == 1) || (climate_zone == 3) || (climate_zone == 5) || (climate_zone == 7) || (climate_zone == 10) || (climate_zone == 11)) // wet
	{
		
		if (eco_zone == 1) // wet, tropics
		{
			flu_value = .48;
		}
		
		else if ((eco_zone == 2) || (eco_zone == 3)) // wet boreal, temperate
		{
			flu_value = .69;
			
		}
		else
		{
			flu_value = 0;
		}		
		
	}
	else if (climate_zone == 9) // montane
	{
		if (eco_zone == 1) // montane tropics
		{
			flu_value = .64;
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
