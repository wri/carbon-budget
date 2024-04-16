// Model constants and variable names used in all c++ scripts except equations.cpp.
// model_years needs to be updated in equations.cpp separately at this time.

namespace constants
{
    // Emissions constants
    // per https://www.learncpp.com/cpp-tutorial/global-constants-and-inline-variables/
    constexpr int model_years {23};    // How many loss years are in the model. Must also be updated in equations.cpp!

    constexpr int CH4_equiv {27};      // The CO2 equivalency (global warming potential) of CH4, AR6 WG1 Table 7.15

    constexpr int N2O_equiv {273};      // The CO2 equivalency (global warming potential) of N2O, AR6 WG1 Table 7.15

    constexpr float C_to_CO2 {44.0/12.0};       // The conversion of carbon to CO2

    constexpr float biomass_to_c {0.47};    // Fraction of carbon in biomass

    constexpr int tropical {1};       // The ecozone code for the tropics

    constexpr int temperate {3};      // The ecozone code for the temperate zone

    constexpr int boreal {2};         // The ecozone code for the boreal zone

    constexpr int soil_emis_period {20};      // The number of years over which soil emissions are calculated (separate from model years)

    constexpr float shiftag_flu {0.72}; // F_lu for shifting agriculture (fraction of soil C not emitted over 20 years)

    constexpr float urb_flu {0.80}; // F_lu for urbanization (fraction of soil C not emitted over 20 years)

    // Input and output variable patterns
    // per https://stackoverflow.com/questions/27123306/is-it-possible-to-use-stdstring-in-a-constexpr
    // Inputs
    constexpr char AGC_emis_year[] = "_Mg_AGC_ha_emis_year";
    constexpr char BGC_emis_year[] = "_Mg_BGC_ha_emis_year";
    constexpr char deadwood_C_emis_year[] = "_Mg_deadwood_C_ha_emis_year_2000";
    constexpr char litter_C_emis_year[] = "_Mg_litter_C_ha_emis_year_2000";
    constexpr char soil_C_emis_year[] = "_Mg_soil_C_ha_emis_year_2000";

    constexpr char legal_Amazon_loss[] = "_legal_Amazon_annual_loss_2001_2019.tif";

    constexpr char lossyear[] = "GFW2023_";
    constexpr char burnyear[] = "_tree_cover_loss_fire_processed.tif";
    constexpr char fao_ecozones[] = "_fao_ecozones_bor_tem_tro_processed.tif";
    constexpr char climate_zones[] = "_climate_zone_processed.tif";
    constexpr char tcl_drivers[] = "_tree_cover_loss_driver_processed.tif";
    constexpr char peat_mask[] = "_peat_mask_processed.tif";
    constexpr char ifl_primary[] = "_ifl_2000_primary_2001_merged.tif";
    constexpr char plantation_type[] = "_plantation_type_oilpalm_woodfiber_other.tif";

    // Outputs
    constexpr char commod_emis[] = "_gross_emis_commodity_Mg_CO2e_ha_biomass_soil_2001_";
    constexpr char shifting_ag_emis[] = "_gross_emis_shifting_ag_Mg_CO2e_ha_biomass_soil_2001_";
    constexpr char forestry_emis[] = "_gross_emis_forestry_Mg_CO2e_ha_biomass_soil_2001_";
    constexpr char wildfire_emis[] = "_gross_emis_wildfire_Mg_CO2e_ha_biomass_soil_2001_";
    constexpr char urbanization_emis[] = "_gross_emis_urbanization_Mg_CO2e_ha_biomass_soil_2001_";
    constexpr char no_driver_emis[] = "_gross_emis_no_driver_Mg_CO2e_ha_biomass_soil_2001_";
    constexpr char all_gases_all_drivers_emis[] = "_gross_emis_all_gases_all_drivers_Mg_CO2e_ha_biomass_soil_2001_";
    constexpr char CO2_only_all_drivers_emis[] = "_gross_emis_CO2_only_all_drivers_Mg_CO2e_ha_biomass_soil_2001_";
    constexpr char non_CO2_all_drivers_emis[] = "_gross_emis_non_CO2_all_drivers_Mg_CO2e_ha_biomass_soil_2001_";
    constexpr char decision_tree_all_drivers_emis[] = "_gross_emis_decision_tree_nodes_biomass_soil_2001_";

}