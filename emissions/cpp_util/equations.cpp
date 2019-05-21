#include <map>
#include <iostream>
#include <stdlib.h>
using namespace std;

float* def_variables(int ecozone, int forestmodel_data, int ifl, int climate, int plant_data, int lossyr)
{

	int model_years;    // How many loss years are in the model
    model_years = 15;

	int tropical;       // The ecozone code for the tropics
    tropical = 1;
    int temperate;      // The ecozone code for the temperate zone
    temperate = 3;
    int boreal;         // The ecozone code for the boreal zone
    boreal = 2;

	// returns Cf, CO2, CH4, N2O, peatburn, peat_drain_total
	float Cf;
	float CO2;
	float CH4;
	float N2O;
	float peatburn;
	float peat_drain_annual;
	float peat_drain_total;

	if ((forestmodel_data == 1) || (forestmodel_data == 2) || (forestmodel_data == 5)) // Commodities, shifting ag., or urbanization
	{
		if (ecozone == boreal) // Commodities/shifting ag/urbanization, boreal
		{
			Cf = 0.59;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (model_years - lossyr) * 36;
		}
		else if (ecozone == temperate)// Commodities/shifting ag/urbanization, temperate
		{
			Cf = 0.51;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (model_years - lossyr) * 31;
		}
		else if (ecozone == tropical) // Commodities/shifting ag/urbanization, tropics
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
            else // Commodities/shifting ag/urbanization, tropics, other plantation or no plantation
            {
                peat_drain_annual = 62;
            }
            peat_drain_total = (model_years - lossyr) * peat_drain_annual;

			if (ifl > 0)    // Commodities/shifting ag/urbanization, tropics, in IFL
			{
				Cf = 0.36;
			}
			else            // Commodities/shifting ag/urbanization, tropics, outside IFL
			{
				Cf = 0.55;
			}
		}
//        cout << "ecozone: " << ecozone << endl;
//        cout << "forestmodel_data: " << forestmodel_data << endl;
//        cout << "ifl: " << ifl << endl;
//        cout << "climate: " << climate << endl;
//        cout << "plant_data: " << plant_data << endl;
//        cout << "model_years: " << model_years << endl;
//        cout << "loss_yr: " << lossyr << endl;
//        cout << "peat_drain_annual: " << peat_drain_annual << endl;
//        cout << "peat_drain_total: " << peat_drain_total << endl;
	}

	else if (forestmodel_data == 3) // Forestry
	{

		if (ecozone == boreal) // Forestry, boreal
		{
			Cf = 0.33;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (model_years - lossyr) * 3;
		}
		else if (ecozone == temperate)// Forestry, temperate
		{
			Cf = 0.62;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (model_years - lossyr) * 12;
		}
		else if (ecozone == tropical) // Forestry, tropics
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
            else // Forestry, tropics, other plantation or no plantation
            {
                peat_drain_annual = 60;
            }
            peat_drain_total = (model_years - lossyr) * peat_drain_annual;

			if (ifl > 0)
			{
				Cf = 0.36;      // Forestry, tropics, in IFL
			}
			else
			{
				Cf = 0.55;      // Forestry, tropics, outside IFL
			}
		}
//        cout << "ecozone: " << ecozone << endl;
//        cout << "forestmodel_data: " << forestmodel_data << endl;
//        cout << "ifl: " << ifl << endl;
//        cout << "climate: " << climate << endl;
//        cout << "plant_data: " << plant_data << endl;
//        cout << "model_years: " << model_years << endl;
//        cout << "loss_yr: " << lossyr << endl;
//        cout << "peat_drain_annual: " << peat_drain_annual << endl;
//        cout << "peat_drain_total: " << peat_drain_total << endl;
	}

	else if (forestmodel_data == 4) // Wildfire
	{

		if (ecozone == boreal) // Wildfire, boreal
		{
			Cf = 0.59;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (model_years - lossyr) * 3;
		}
		else if (ecozone == temperate)// Wildfire, temperate
		{
			Cf = 0.51;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (model_years - lossyr) * 12;
		}
		else if (ecozone == tropical) // Wildfire, tropics
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
            else // Wildfire, tropics, other plantation or no plantation
            {
                peat_drain_annual = 60;
            }
            peat_drain_total = (model_years - lossyr) * peat_drain_annual;

			if (ifl > 0)        // Wildfire, tropics, in IFL
			{
				Cf = 0.36;
			}
			else                // Wildfire, tropics, outside IFL
			{
				Cf = 0.55;
			}
		}
//        cout << "ecozone: " << ecozone << endl;
//        cout << "forestmodel_data: " << forestmodel_data << endl;
//        cout << "ifl: " << ifl << endl;
//        cout << "climate: " << climate << endl;
//        cout << "plant_data: " << plant_data << endl;
//        cout << "model_years: " << model_years << endl;
//        cout << "loss_yr: " << lossyr << endl;
//        cout << "peat_drain_annual: " << peat_drain_annual << endl;
//        cout << "peat_drain_total: " << peat_drain_total << endl;
	}

	else  // No driver-- same as forestry
	{
		if (ecozone == boreal) // No driver, boreal
		{
			Cf = 0.33;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (model_years - lossyr) * 3;
		}
		else if (ecozone == temperate)// No driver, temperate
		{
			Cf = 0.62;
			CO2 = 1569;
			CH4 = 4.7;
			N2O = 0.26;
			peatburn = 41;
			peat_drain_total = (model_years - lossyr) * 12;
		}
		else if (ecozone == tropical) // No driver, tropics
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
            else // No driver, tropics, other plantation or no plantation
            {
                peat_drain_annual = 60;
            }
            peat_drain_total = (model_years - lossyr) * peat_drain_annual;

			if (ifl > 0)
			{
				Cf = 0.36;      // No driver, tropics, in IFL
			}
			else
			{
				Cf = 0.55;      // No driver, tropics, outside IFL
			}
		}
//        cout << "ecozone: " << ecozone << endl;
//        cout << "forestmodel_data: " << forestmodel_data << endl;
//        cout << "ifl: " << ifl << endl;
//        cout << "climate: " << climate << endl;
//        cout << "plant_data: " << plant_data << endl;
//        cout << "model_years: " << model_years << endl;
//        cout << "loss_yr: " << lossyr << endl;
//        cout << "peat_drain_annual: " << peat_drain_annual << endl;
//        cout << "peat_drain_total: " << peat_drain_total << endl;
	}

//    cout << "ecozone end of fx: " << ecozone << endl;
//    cout << "forestmodel_data end of fx: " << forestmodel_data << endl;
//    cout << "ifl end of fx: " << ifl << endl;
//    cout << "climate end of fx: " << climate << endl;
//    cout << "plant_data end of fx: " << plant_data << endl;
//    cout << "model_years end of fx: " << model_years << endl;
//    cout << "loss_yr end of fx: " << lossyr << endl;

//    float test;
//    srand(time(0));
//    test = rand();

    cout << "cf end of fx: " << Cf << endl;
    cout << "gef_co2 end of fx: " << CO2 << endl;
    cout << "gef_Ch4 end of fx: " << CH4 << endl;
    cout << "gef_n2o end of fx: " << N2O << endl;
    cout << "peatburn end of fx: " << peatburn << endl;
//    cout << "peat_drain_annual end of fx: " << peat_drain_annual << endl;
//    cout << "peat_drain_total end of fx: " << peat_drain_total << endl;
//    cout << "random_number end of fx: " << test << endl;
    cout << endl;

	static float def_variables[6] = {Cf, CO2, CH4, N2O, peatburn};

	return def_variables;

}

float peat_drain_fx(int ecozone, int forestmodel_data, int ifl, int climate, int plant_data, int lossyr)
{

	int model_years;    // How many loss years are in the model
    model_years = 15;

	int tropical;       // The ecozone code for the tropics
    tropical = 1;
    int temperate;      // The ecozone code for the temperate zone
    temperate = 3;
    int boreal;         // The ecozone code for the boreal zone
    boreal = 2;

	// returns Cf, CO2, CH4, N2O, peatburn, peat_drain_total
	float peat_drain_annual;
	float peat_drain_total;

	if ((forestmodel_data == 1) || (forestmodel_data == 2) || (forestmodel_data == 5)) // Commodities, shifting ag., or urbanization
	{
		if (ecozone == boreal) // Commodities/shifting ag/urbanization, boreal
		{
			peat_drain_total = (model_years - lossyr) * 36;
		}
		else if (ecozone == temperate)// Commodities/shifting ag/urbanization, temperate
		{
			peat_drain_total = (model_years - lossyr) * 31;
		}
		else if (ecozone == tropical) // Commodities/shifting ag/urbanization, tropics
		{
            if (plant_data == 1)  // Commodities/shifting ag/urbanization, tropics, oil palm
            {
                peat_drain_annual = 47;
            }
            else if (plant_data == 2) // Commodities/shifting ag/urbanization, tropics, wood fiber
            {
                peat_drain_annual = 80;
            }
            else // Commodities/shifting ag/urbanization, tropics, other plantation or no plantation
            {
                peat_drain_annual = 62;
            }
            peat_drain_total = (model_years - lossyr) * peat_drain_annual;
		}
//        cout << "ecozone: " << ecozone << endl;
//        cout << "forestmodel_data: " << forestmodel_data << endl;
//        cout << "ifl: " << ifl << endl;
//        cout << "climate: " << climate << endl;
//        cout << "plant_data: " << plant_data << endl;
//        cout << "model_years: " << model_years << endl;
//        cout << "loss_yr: " << lossyr << endl;
//        cout << "peat_drain_annual: " << peat_drain_annual << endl;
//        cout << "peat_drain_total: " << peat_drain_total << endl;
	}

	else if (forestmodel_data == 3) // Forestry
	{

		if (ecozone == boreal) // Forestry, boreal
		{
			peat_drain_total = (model_years - lossyr) * 3;
		}
		else if (ecozone == temperate)// Forestry, temperate
		{
			peat_drain_total = (model_years - lossyr) * 12;
		}
		else if (ecozone == tropical) // Forestry, tropics
		{
			if (plant_data == 1) // Forestry, tropics, oil palm
            {
                peat_drain_annual = 45;
            }
            else if (plant_data == 2) // Forestry, tropics, wood fiber
            {
                peat_drain_annual = 79;
            }
            else // Forestry, tropics, other plantation or no plantation
            {
                peat_drain_annual = 60;
            }
            peat_drain_total = (model_years - lossyr) * peat_drain_annual;
		}
//        cout << "ecozone: " << ecozone << endl;
//        cout << "forestmodel_data: " << forestmodel_data << endl;
//        cout << "ifl: " << ifl << endl;
//        cout << "climate: " << climate << endl;
//        cout << "plant_data: " << plant_data << endl;
//        cout << "model_years: " << model_years << endl;
//        cout << "loss_yr: " << lossyr << endl;
//        cout << "peat_drain_annual: " << peat_drain_annual << endl;
//        cout << "peat_drain_total: " << peat_drain_total << endl;
	}

	else if (forestmodel_data == 4) // Wildfire
	{

		if (ecozone == boreal) // Wildfire, boreal
		{
			peat_drain_total = (model_years - lossyr) * 3;
		}
		else if (ecozone == temperate)// Wildfire, temperate
		{
			peat_drain_total = (model_years - lossyr) * 12;
		}
		else if (ecozone == tropical) // Wildfire, tropics
		{
		    if (plant_data == 1) // Wildfire, tropics, oil palm
            {
                peat_drain_annual = 45;
            }
            else if (plant_data == 2) // Wildfire, tropics, wood fiber
            {
                peat_drain_annual = 79;
            }
            else // Wildfire, tropics, other plantation or no plantation
            {
                peat_drain_annual = 60;
            }
            peat_drain_total = (model_years - lossyr) * peat_drain_annual;
		}
//        cout << "ecozone: " << ecozone << endl;
//        cout << "forestmodel_data: " << forestmodel_data << endl;
//        cout << "ifl: " << ifl << endl;
//        cout << "climate: " << climate << endl;
//        cout << "plant_data: " << plant_data << endl;
//        cout << "model_years: " << model_years << endl;
//        cout << "loss_yr: " << lossyr << endl;
//        cout << "peat_drain_annual: " << peat_drain_annual << endl;
//        cout << "peat_drain_total: " << peat_drain_total << endl;
	}

	else  // No driver-- same as forestry
	{
		if (ecozone == boreal) // No driver, boreal
		{
			peat_drain_total = (model_years - lossyr) * 3;
		}
		else if (ecozone == temperate)// No driver, temperate
		{
			peat_drain_total = (model_years - lossyr) * 12;
		}
		else if (ecozone == tropical) // No driver, tropics
		{
			if (plant_data == 1) // No driver, tropics, oil palm
            {
                peat_drain_annual = 45;
            }
            else if (plant_data == 2) // No driver, tropics, wood fiber
            {
                peat_drain_annual = 79;
            }
            else // No driver, tropics, other plantation or no plantation
            {
                peat_drain_annual = 60;
            }
            peat_drain_total = (model_years - lossyr) * peat_drain_annual;
		}
//        cout << "ecozone: " << ecozone << endl;
//        cout << "forestmodel_data: " << forestmodel_data << endl;
//        cout << "ifl: " << ifl << endl;
//        cout << "climate: " << climate << endl;
//        cout << "plant_data: " << plant_data << endl;
//        cout << "model_years: " << model_years << endl;
//        cout << "loss_yr: " << lossyr << endl;
//        cout << "peat_drain_annual: " << peat_drain_annual << endl;
//        cout << "peat_drain_total: " << peat_drain_total << endl;
	}

//    cout << "ecozone end of fx: " << ecozone << endl;
//    cout << "forestmodel_data end of fx: " << forestmodel_data << endl;
//    cout << "ifl end of fx: " << ifl << endl;
//    cout << "climate end of fx: " << climate << endl;
//    cout << "plant_data end of fx: " << plant_data << endl;
//    cout << "model_years end of fx: " << model_years << endl;
//    cout << "loss_yr end of fx: " << lossyr << endl;

//    float test;
//    srand(time(0));
//    test = rand();

    cout << "peat_drain_annual end of fx: " << peat_drain_annual << endl;
    cout << "peat_drain_total end of fx: " << peat_drain_total << endl;
//    cout << "random_number end of fx: " << test << endl;
    cout << endl;

	return peat_drain_total;

}