'''
'''
import pandas as pd
from pathlib import Path

import constants as cn
import utilities as ut

def main(excel_path):

    # Read in the untranslaetd GFW forest flux data into pandas dataframes
    excel_path = Path(excel_path)
    if not excel_path.exists():
        raise FileNotFoundError(f"File not found: {excel_path}")

    managed_land_proxy_codes_df = pd.read_excel(excel_path, sheet_name=cn.managed_land_proxy_sheet)
    gfw_removals_df = pd.read_excel(excel_path, sheet_name=cn.gfw_removals_sheet)
    gfw_emissions_df = pd.read_excel(excel_path, sheet_name=cn.gfw_emissions_sheet)

    #Step 1: Reclassify JRC managed land proxy codes to GFW codes which determine which translation method to apply
    if cn.gfw_code_col_name not in managed_land_proxy_codes_df.columns:
        managed_land_proxy_codes_df = ut.update_managed_land_proxy_df(managed_land_proxy_codes_df, cn.jrc_code_col_name, cn.gfw_code_col_name)
    else:
        print("GFW managed land proxy code exists. Skipping reclassification step.")

    #Step 2: Translate country removals according to GFW managed land proxy code
    keep_cols = ['iso', 'country', cn.gfw_code_col_name]
    existing_cols = [c for c in keep_cols if c in managed_land_proxy_codes_df.columns]
    translated_removals_df = managed_land_proxy_codes_df[existing_cols].copy()



    # Write out translated results to new spreadsheet
    managed_land_proxy_codes_df.to_excel(cn.out_sheet, sheet_name=cn.managed_land_proxy_sheet, index=False)
    gfw_removals_df.to_excel(cn.out_sheet, sheet_name=cn.gfw_removals_sheet, index=False)
    gfw_emissions_df.to_excel(cn.out_sheet, sheet_name=cn.gfw_emissions_sheet, index=False)


if __name__ == "__main__":
    main(cn.in_sheet)