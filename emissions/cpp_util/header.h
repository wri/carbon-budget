namespace constants
{
    // Model constants
    constexpr int CH4_equiv {28};      // The CO2 equivalency (global warming potential) of CH4
//    CH4_equiv = 28;

    constexpr int N2O_equiv {265};      // The CO2 equivalency (global warming potential) of N2O
//    N2O_equiv = 265;

    constexpr float C_to_CO2 {44.0/12.0};       // The conversion of carbon to CO2
//    C_to_CO2 = 44.0/12.0;

    constexpr float biomass_to_c {0.47};    // Fraction of carbon in biomass
//    biomass_to_c = 0.47;

    constexpr int model_years {20};    // How many loss years are in the model
//    model_years = 20;
    constexpr string model_years_str;
    model_years_str = to_string(model_years);

    constexpr int tropical {1};       // The ecozone code for the tropics
//    tropical = 1;

    constexpr int temperate {3};      // The ecozone code for the temperate zone
//    temperate = 3;

    constexpr int boreal {2};         // The ecozone code for the boreal zone
//    boreal = 2;
}