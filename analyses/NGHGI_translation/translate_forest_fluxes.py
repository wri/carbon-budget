'''
GFW Managed Land Proxy cases:
Case 1: Includes 49 countries that explicitly or implicitly consider all forest land to be managed.
        For these countries, we consider the entire GFW forest flux model extent to be managed.
Case 2a: Includes three countries (Brazil, the United States, and Canada) for which there are georeferenced boundaries of managed lands.
        For these countries, managed forest is the entire GFW forest flux model extent in the managed land boundaries.
Case 2b: Includes the remaining 143 countries in which NGHGIs do not report enough details regarding the managed land proxy and its extent.
        For these countries we consider "managed forests" in tropical regions to be forests outside humid tropical primary forests from 2001
        (Turubanova et al. 2018) and in extratropical regions as forests outside intact forest landscapes from 2000 (Potapov et al. 2017).

'''
import pandas as pd
from pathlib import Path

import constants as cn
import utilities as ut

def main(excel_path):

    #--------------------------------------------------------------------------------------------------------------------
    # Step 1: Read in the untranslated GFW forest flux data into pandas dataframes and standardize column names
    #--------------------------------------------------------------------------------------------------------------------
    excel_path = Path(excel_path)

    managed_land_proxy_codes_df = pd.read_excel(excel_path, sheet_name=cn.managed_land_proxy_sheet)
    managed_land_proxy_codes_df.columns = managed_land_proxy_codes_df.columns.astype(str).str.strip().str.lower().str.replace(r"\s+", "_", regex=True)

    gfw_removals_df = pd.read_excel(excel_path, sheet_name=cn.gfw_removals_sheet)
    gfw_removals_df.columns = gfw_removals_df.columns.astype(str).str.strip().str.lower().str.replace(r"\s+", "_", regex=True)

    gfw_emissions_df = pd.read_excel(excel_path, sheet_name=cn.gfw_emissions_sheet)
    gfw_emissions_df.columns = gfw_emissions_df.columns.astype(str).str.strip().str.lower().str.replace(r"\s+", "_", regex=True)

    usa_df = pd.read_excel(excel_path, sheet_name=cn.usa_sheet)
    canada_df = pd.read_excel(excel_path, sheet_name=cn.canada_sheet)
    brazil_df = pd.read_excel(excel_path, sheet_name=cn.brazil_sheet)
    managed_polygons_df = pd.concat([usa_df, canada_df, brazil_df], ignore_index=True, sort=False)
    managed_polygons_df.columns = managed_polygons_df.columns.astype(str).str.strip().str.lower().str.replace(r"\s+", "_", regex=True)

    #--------------------------------------------------------------------------------------------------------------------
    # Step 2: Reclassify JRC managed land proxy codes into GFW codes that determine which translation method to apply
    #--------------------------------------------------------------------------------------------------------------------
    if cn.gfw_code_col not in managed_land_proxy_codes_df.columns:
        managed_land_proxy_codes_df = ut.update_managed_land_proxy_df(managed_land_proxy_codes_df, cn.jrc_code_col, cn.gfw_code_col)
    else:
        print("GFW managed land proxy code exists. Skipping JRC -> GFW reclassification step.")

    #--------------------------------------------------------------------------------------------------------------------
    # Step 3: Translate country removals according to the GFW managed land proxy code
    #--------------------------------------------------------------------------------------------------------------------
    # Check that the managed land proxy df has iso, country, and gfw managed land code info and copy to new df
    keep_cols = [cn.iso_col, cn.country_col, cn.gfw_code_col]
    missing_cols = [c for c in keep_cols if c not in managed_land_proxy_codes_df.columns]
    if missing_cols:
        raise KeyError(
            f"The following required column(s) are missing from managed_land_proxy_codes_df: {missing_cols}."
            f"Available columns: {list(managed_land_proxy_codes_df.columns)}"
        )
    keep_col_df = managed_land_proxy_codes_df[keep_cols].copy()

    # Use the GFW managed land code to assign translated removals per country
    translated_removals_df = ut.translate_removals(keep_col_df, gfw_removals_df, managed_polygons_df)
    print("Translated GFW removals into anthropogenic forest and non-anthropogenic forest removals")

    #--------------------------------------------------------------------------------------------------------------------
    # Step 4: Translate country emissions according to the GFW managed land proxy code
    #--------------------------------------------------------------------------------------------------------------------
    # Use the GFW managed land code to assign translated emissions per country
    translated_emissions_df = ut.translate_emissions(keep_col_df, gfw_emissions_df, usa_df, canada_df, brazil_df)
    print("Translated GFW emissions into anthropogenic deforestation, anthropogenic forest, and non-anthropogenic forest emissions")

    # --------------------------------------------------------------------------------------------------------------------
    # Step 5: Combine translated country emissions and removals to calculate anthropogenic deforestation emissions,
    #         anthropogenic forest flux, and non-anthropogenic forest flux timeseries.
    # --------------------------------------------------------------------------------------------------------------------


   # --------------------------------------------------------------------------------------------------------------------
   # Step 6: Write out the translated results to a new spreadsheet
   # --------------------------------------------------------------------------------------------------------------------
    with pd.ExcelWriter(cn.out_sheet, engine="openpyxl", mode="w") as writer:
        translated_removals_df.to_excel(writer, sheet_name=cn.nghgi_removals_sheet, index=False)
        translated_emissions_df.to_excel(writer, sheet_name=cn.nghgi_emissions_sheet, index=False)

        if cn.keep_raw_data:
            managed_land_proxy_codes_df.to_excel(writer, sheet_name=cn.managed_land_proxy_sheet, index=False)
            gfw_removals_df.to_excel(writer, sheet_name=cn.gfw_removals_sheet, index=False)
            gfw_emissions_df.to_excel(writer, sheet_name=cn.gfw_emissions_sheet, index=False)

    #TODO: Replace mg_co2 --> Mg_CO2 in column names, column order, format numbers, fill NANs with 0s.

if __name__ == "__main__":
    main(cn.in_sheet)