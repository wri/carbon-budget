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

   #Check that the input path exists
    excel_path = Path(excel_path)
    if not excel_path.exists():
        raise FileNotFoundError(f"File not found: {excel_path}")

    #Read in the untranslaetd GFW forest flux data into pandas dataframes
    #Standardize column names by stripping whitespaces, setting to lowearcase, and replacing any spaces with "_".
    managed_land_proxy_codes_df = pd.read_excel(excel_path, sheet_name=cn.managed_land_proxy_sheet)
    managed_land_proxy_codes_df.columns = managed_land_proxy_codes_df.columns.astype(str).str.strip().str.lower().str.replace(r"\s+", "_", regex=True)

    gfw_removals_df = pd.read_excel(excel_path, sheet_name=cn.gfw_removals_sheet)
    gfw_removals_df.columns = gfw_removals_df.columns.astype(str).str.strip().str.lower().str.replace(r"\s+", "_", regex=True)

    gfw_emissions_df = pd.read_excel(excel_path, sheet_name=cn.gfw_emissions_sheet)
    gfw_emissions_df.columns = gfw_emissions_df.columns.astype(str).str.strip().str.lower().str.replace(r"\s+", "_", regex=True)


    #Step 1: Reclassify JRC managed land proxy codes to GFW codes which determine which translation method to apply
    if cn.gfw_code_col not in managed_land_proxy_codes_df.columns:
        managed_land_proxy_codes_df = ut.update_managed_land_proxy_df(managed_land_proxy_codes_df, cn.jrc_code_col, cn.gfw_code_col)
    else:
        print("GFW managed land proxy code exists. Skipping reclassification step.")


    #Step 2: Translate country removals according to GFW managed land proxy code
    # Check that the managed land proxy df has iso, country, and gfw managed land code info and copy to new df
    keep_cols = ['iso', 'country', cn.gfw_code_col]
    missing_cols = [c for c in keep_cols if c not in managed_land_proxy_codes_df.columns]
    if missing_cols:
        raise KeyError(
            f"The following required column(s) are missing from managed_land_proxy_codes_df: {missing_cols}."
            f"Available columns: {list(managed_land_proxy_codes_df.columns)}"
        )
    keep_col_df = managed_land_proxy_codes_df[keep_cols].copy()

    #Use the GFW managed land code to assign translated removals per country
    translated_removals_df = ut.translate_removals(keep_col_df, gfw_removals_df)



    # Write out translated results to new spreadsheet
    managed_land_proxy_codes_df.to_excel(cn.out_sheet, sheet_name=cn.managed_land_proxy_sheet, index=False)
    gfw_removals_df.to_excel(cn.out_sheet, sheet_name=cn.gfw_removals_sheet, index=False)
    gfw_emissions_df.to_excel(cn.out_sheet, sheet_name=cn.gfw_emissions_sheet, index=False)


if __name__ == "__main__":
    main(cn.in_sheet)