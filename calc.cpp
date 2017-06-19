#include <map>
#include <iostream>
#include <stdlib.h>
using namespace std;

int deadwood_calc(int biome_data, int elevation_data, int precip_data, int agb_data)
{
	int deadwood;

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
		deadwood = -9999;}
	
	return deadwood;
}

int litter_calc(int biome_data, int elevation_data, int precip_data, int agb_data)
{
	int litter;
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
		litter = -9999;
	}
	
	return litter;
}