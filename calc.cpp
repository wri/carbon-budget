#include <map>
#include <iostream>
#include <stdlib.h>
using namespace std;

float deadwood_calc(float biome_data, float elevation_data, float precip_data, float agb_data)
{
	float deadwood;
	if (elevation_data == -32768 && biome_data ==2)
	{
		deadwood = agb_data * .08 * .47;
	}
	else
	{
	if (biome_data == 1 && elevation_data < 2000 && precip_data< 1000) {
		deadwood = agb_data * .02 * .47;}
	
	else if (biome_data == 1 && elevation_data < 2000 && precip_data< 1600 && precip_data > 1000) {
		deadwood = agb_data * .01 * .47;}
		
	else if (biome_data == 1 && elevation_data< 2000 && precip_data > 1600) {
		deadwood = agb_data * .06 * .47;}
		
	else if (biome_data == 1 && elevation_data> 2000) {
		deadwood = agb_data * .07 * .47;}

	else if ((biome_data == 2) || (biome_data == 3)) {
		deadwood = agb_data * .08 * .47;}
		
	else {
		deadwood = 0;}
	}
	return deadwood;
}

float litter_calc(float biome_data, float elevation_data, float precip_data, float agb_data)
{
	float litter;
        if (elevation_data == -32768 && biome_data == 2)
        {
                litter = agb_data * .04 * .37;
        }
        else
        {

	if (biome_data== 1 && elevation_data < 2000 && precip_data < 1000) 
	{  
		litter = agb_data * .04 * .37;
	}
	
	else if (biome_data == 1 && elevation_data < 2000 && precip_data < 1600 && precip_data > 1000) 
	{
		litter = agb_data * .01 * .37;
	}

	else if (biome_data == 1 && elevation_data < 2000 && precip_data > 1600) 
	{
		litter = agb_data * .01 * .37;
	}

	else if (biome_data == 1 && elevation_data > 2000) 
	{
		litter = agb_data * .01 * .37;
	}
	
	else if ((biome_data == 2) || (biome_data == 3)) 
	{
		litter = agb_data * .04 * .37;
	}

	else 
	{
		litter = 0;
	}
}
	
	return litter;
}
