#Author: Melissa Rose
#Date Modified: 
#Title: SDPT Version 2 RF Updates
###############################################################################
#Step 1: Load packages
library(readxl)
library(openxlsx)
library(stringr)
library(data.table)
library(tidyverse)
#------------------------------------------------------------------------------

#Step 2: Open SDPT Version 1 and SDPT Version 2 spreadsheets 
sdpt_v1 <- data.table(read_excel('C://Users/Melissa.Rose/OneDrive - World Resources Institute/Documents/Projects/sdpt/plant_attributes_simp_jul2020_toliz_from_Nancy_Harris_20200716.xlsx', sheet = 'plant_attributes_simp_jul2020'))
sdpt_v2 <- data.table(read_excel('C://Users/Melissa.Rose/OneDrive - World Resources Institute/Documents/Projects/sdpt/plantation_attributes_v2.0_draft_v6142023.xlsx', sheet = 'in'))
sdpt_calc <- data.table(read_excel('C://Users/Melissa.Rose/OneDrive - World Resources Institute/Documents/Projects/sdpt/plantation_attributes_v2.0_draft_v6142023.xlsx', sheet = 'sc_calc'))
sdpt_calc <- sdpt_calc[is.na(sdpt_calc$final_id) == FALSE, ] #remove NA values

#Step 3: Join tables based on final_id value
#check and remove duplicated final_id codes from version 1
anyDuplicated(sdpt_v1$final_id)                                                          #47 duplicated final_ids
sdptv1_dup <- sdpt_v1[duplicated(sdpt_v1$final_id) == TRUE, ]                   #create new DT with 47 duplicated final_id codes from version 1                   
sdptv1_trunc <- sdpt_v1[duplicated(sdpt_v1$final_id) == FALSE, ]                #remove 47 duplicated final_id codes from version 1
sdptv1_dup <- left_join(sdptv1_dup, sdptv1_trunc, by = 'final_id')              #join tables and check to see if duplicated values have the same removal factors
sdptv1_dup <- sdptv1_dup[growth.x != growth.y]                                  #all duplicated values match 
rm(sdptv1_dup)

#copy V1 removal factors to V2
sdptv1_trunc <- sdptv1_trunc[, .(final_id, growth, SD_error, grow_source)]      #keep only columns in v1 that need to be copied over to v2
sdptv2_update <- left_join(sdpt_v2, sdptv1_trunc, by = 'final_id')              #join v1 and v2 by 'final_id'
sdptv2_update[, removal_factor_Mg_AGC_BGC_ha_yr := growth]                      #move v1 data into v2 columns
sdptv2_update[, removal_factor_SD_Mg_AGC_BGC_ha_yr := SD_error]
sdptv2_update[, removal_factor_source := grow_source]                 
sdptv2_update[, ":=" (growth = NULL,                                            #remove v1 columns from updated v2 dataset
                      SD_error = NULL, 
                      grow_source = NULL)]                          
sdptv2_update <- sdptv2_update[ , removal_factor_notes := as.character(removal_factor_notes)]
sdptv2_update$sd_source <- as.character('')                                     #add sd_source column
rm(sdptv1_trunc)

#Step 4: Subset V2 data to single classes and compare values with sdpt_calc
sdptv2_sc <- sdptv2_update[is.na(sdptv2_update$sciName2)]                       #843 single classes 
sdptv2_sc_empty <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr) 
                                 &  is.na(sdptv2_update$sciName2)]              #430 empty single classes 

#join calculated spdt RF values with applied RF values from V1
sdpt_sc_trunc <- sdptv2_sc[, .(final_id, removal_factor_Mg_AGC_BGC_ha_yr, removal_factor_SD_Mg_AGC_BGC_ha_yr, removal_factor_source)]
sdptv2_comb <- left_join(sdpt_calc, sdpt_sc_trunc, by = 'final_id') 
rm(sdpt_sc_trunc)

#compare v1 values with v2 updates
sdptv2_comb_duplicates <- sdptv2_comb[is.na(removal_factor_Mg_AGC_BGC_ha_yr.x) == FALSE 
                                    & is.na(removal_factor_SD_Mg_AGC_BGC_ha_yr.y) == FALSE, ]

sdptv2_comb_duplicates <- sdptv2_comb_duplicates[round(removal_factor_Mg_AGC_BGC_ha_yr.x, digits = 2) != 
                                                  round(removal_factor_Mg_AGC_BGC_ha_yr.y, digits = 2), 
                                                 list(final_id, country, originalName, vernacName, sciName, simpleName, sciName1, 
                                                      removal_factor_Mg_AGC_BGC_ha_yr.x, removal_factor_SD_Mg_AGC_BGC_ha_yr.x, removal_factor_source.x, removal_factor_notes, 
                                                      removal_factor_Mg_AGC_BGC_ha_yr.y, removal_factor_SD_Mg_AGC_BGC_ha_yr.y, removal_factor_source.y)]                      #57 updated removal factors from V1

#Step 5: Table join updated Version 2 with sdpt_calc for updated removal factors
sdpt_calc_trunc <- sdpt_calc[, .(final_id, removal_factor_Mg_AGC_BGC_ha_yr, removal_factor_SD_Mg_AGC_BGC_ha_yr, removal_factor_source, sd_source, removal_factor_notes)]
sdptv2_calc_update <- left_join(sdptv2_update, sdpt_calc_trunc, by = 'final_id') 

#copy over v1 into empty v2 rows
sdptv2_calc_update[is.na(removal_factor_Mg_AGC_BGC_ha_yr.y), ":=" 
                                  (removal_factor_Mg_AGC_BGC_ha_yr.y = removal_factor_Mg_AGC_BGC_ha_yr.x, 
                                    removal_factor_SD_Mg_AGC_BGC_ha_yr.y = removal_factor_SD_Mg_AGC_BGC_ha_yr.x, 
                                    removal_factor_notes.y = removal_factor_source.x)]

#remove .x columns
sdptv2_calc_update[, ":=" (removal_factor_Mg_AGC_BGC_ha_yr.x = NULL,                                            
                      removal_factor_SD_Mg_AGC_BGC_ha_yr.x = NULL, 
                      removal_factor_source.x = NULL, 
                      removal_factor_notes.x = NULL, 
                      sd_source.x = NULL)]   

write.xlsx(sdptv2_calc_update, 'C://Users/Melissa.Rose/OneDrive - World Resources Institute/Documents/Projects/sdpt/SPDT_v2.0_updates_MR_080423.xlsx')



















#####################################################################################################################

#To keep version 1 rf
#sdptv2_calc_update <- sdptv2_comb[is.na(removal_factor_Mg_AGC_BGC_ha_yr.x), ":=" 
#                                  (removal_factor_Mg_AGC_BGC_ha_yr.x = removal_factor_Mg_AGC_BGC_ha_yr.y, 
#                                    removal_factor_SD_Mg_AGC_BGC_ha_yr.x = removal_factor_SD_Mg_AGC_BGC_ha_yr.y, 
#                                    removal_factor_notes = removal_factor_source.y)]

#sdptv2_calc_update[, ":=" (removal_factor_Mg_AGC_BGC_ha_yr.y = NULL,                                            
#                           removal_factor_SD_Mg_AGC_BGC_ha_yr.y = NULL, 
#                           removal_factor_source.y = NULL)]