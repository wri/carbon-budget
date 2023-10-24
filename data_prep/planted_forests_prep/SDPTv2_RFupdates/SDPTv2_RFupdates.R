#Author: Melissa Rose
#Date Modified: 
#Title: SDPT Version 2 RF Updates
###############################################################################
#Step 1: Load packages, create path variable to carbon-budget repo, and open SDPT spreadsheets 
library(readxl)
library(openxlsx)
library(stringr)
library(data.table)
library(tidyverse)
library(janitor)

#update with user-specified path to carbon-budget repo
carbon_budget_repo <- paste0(getwd(), '/git', '/carbon-budget/') 

sdpt_v1 <- data.table(read_excel(paste0(carbon_budget_repo, 'data_prep/planted_forests_prep/SDPTv2_RFupdates/SDPTv1_RF_20200716.xlsx'), sheet = 'plant_attributes_simp_jul2020'))
sdpt_v2 <- data.table(read_excel(paste0(carbon_budget_repo, 'data_prep/planted_forests_prep/SDPTv2_RFupdates/plantation_attributes_v2.0_draft_v6142023.xlsx'), sheet = 'in'))
sdpt_calc <- data.table(read_excel(paste0(carbon_budget_repo, 'data_prep/planted_forests_prep/SDPTv2_RFupdates/SDPTv2_singleClassCalculations_20230831.xlsx'), sheet = 'sc_calc', n_max=844))
source(paste0(carbon_budget_repo, 'data_prep/planted_forests_prep/SDPTv2_RFupdates/SDPTv2_utils.R'))

#Step 2: Update sdpt_v2 with RFs from sdpt_v1 spreadsheet
#check and remove duplicated final_id codes from v1
anyDuplicated(sdpt_v1$final_id)
sdptv1_dup <- sdpt_v1[duplicated(sdpt_v1$final_id) == TRUE, ]                   #create new DT with only duplicated final_id codes from v1                   
sdptv1_trunc <- sdpt_v1[duplicated(sdpt_v1$final_id) == FALSE, ]                #remove 47 duplicated final_id codes from v1
sdptv1_dup <- left_join(sdptv1_dup, sdptv1_trunc, by = 'final_id')              #check to see if duplicated values have the same RFs
sdptv1_dup <- sdptv1_dup[growth.x != growth.y]                                  #all duplicated values match 
rm(sdptv1_dup)

#copy V1 RFs to V2
sdptv1_trunc <- sdptv1_trunc[, .(final_id, growth, SD_error, grow_source)]      #keep only columns in v1 that need to be copied over to v2
sdptv2_update <- left_join(sdpt_v2, sdptv1_trunc, by = 'final_id')              #join v1 and v2 by 'final_id'
sdptv2_update[, ":=" (removal_factor_Mg_AGC_BGC_ha_yr = growth,                 #move v1 data into v2 columns
                      removal_factor_SD_Mg_AGC_BGC_ha_yr = SD_error, 
                      removal_factor_source = grow_source)]                     

sdptv2_update[, ":=" (growth = NULL,                                            #remove v1 columns 
                      SD_error = NULL, 
                      grow_source = NULL)]                          

sdptv2_update <- sdptv2_update[ , removal_factor_notes := as.character(removal_factor_notes)]
sdptv2_update$sd_source <- as.character('')                                     #add sd_source column
rm(sdptv1_trunc)


#Step 3: Update sdpt_v2 with updated RFs from sdpt_calc spreadsheet
sdpt_calc_trunc <- sdpt_calc[, .(final_id, `agc (t C/ha/yr)`, agc_uncertainty, `bgc (t C/ha/yr)`, bgc_uncertainty, removal_factor_Mg_AGC_BGC_ha_yr, removal_factor_SD_Mg_AGC_BGC_ha_yr, removal_factor_Mg_AGC_ha_yr, removal_factor_SD_Mg_AGC_ha_yr, removal_factor_source, sd_source, removal_factor_notes, removal_factor_region)]
sdptv2_update <- left_join(sdptv2_update, sdpt_calc_trunc, by = 'final_id') 

#check which final_ids have updated RF values                                   #32 final_ids have been changed or updated from v1
sdptv2_check <- sdptv2_update[is.na(removal_factor_Mg_AGC_BGC_ha_yr.x) == FALSE & is.na(removal_factor_Mg_AGC_BGC_ha_yr.y) == FALSE & 
                                between(removal_factor_Mg_AGC_BGC_ha_yr.x, (removal_factor_Mg_AGC_BGC_ha_yr.y - 1), (removal_factor_Mg_AGC_BGC_ha_yr.y + 1)) == FALSE] 
#rm(sdptv2_check)

#replace empty v2 RFs with v1 values
sdptv2_update[is.na(removal_factor_Mg_AGC_BGC_ha_yr.y), ":="
                   (removal_factor_Mg_AGC_BGC_ha_yr.y = removal_factor_Mg_AGC_BGC_ha_yr.x, 
                    removal_factor_SD_Mg_AGC_BGC_ha_yr.y = removal_factor_SD_Mg_AGC_BGC_ha_yr.x, 
                    removal_factor_notes.y = removal_factor_source.x)]

#remove V1 columns
sdptv2_update[, ":=" (removal_factor_Mg_AGC_BGC_ha_yr.x = NULL, 
                      removal_factor_SD_Mg_AGC_BGC_ha_yr.x = NULL, 
                      removal_factor_source.x = NULL, 
                      removal_factor_notes.x = NULL, 
                      sd_source.x = NULL)]    


#Step 4: Calculate removal factors for mixed classes 
#Cambodia 
#KHM_4 - Acacia and Teak
rf_average('KHM_4', 'KHM_9', 'IDN_7', sdptv2_update)

sdptv2_update[final_id == 'KHM_4' , ":="
                (removal_factor_source.y = 'IPCC 2019 Refinement MAI Table 4.11',
                removal_factor_region = 'Asia', 
                removal_factor_notes.y = 'Average of Acacia sp. for S and SE Asia and Tectona grandis for Asia.')]
         
#Uruguay
#URY_13 & URY_14: Eucalyptus mix
sdptv2_update[final_id == 'URY_13' | final_id == 'URY_14', ":=" 
              (`agc (t C/ha/yr)` = sdptv2_update[final_id == 'URY_11', `agc (t C/ha/yr)`], 
                agc_uncertainty = sdptv2_update[final_id == 'URY_11', agc_uncertainty], 
                `bgc (t C/ha/yr)` = sdptv2_update[final_id == 'URY_11', `bgc (t C/ha/yr)`], 
                bgc_uncertainty = sdptv2_update[final_id == 'URY_11', bgc_uncertainty], 
                removal_factor_Mg_AGC_ha_yr = sdptv2_update[final_id == 'URY_11', removal_factor_Mg_AGC_ha_yr], 
                removal_factor_SD_Mg_AGC_ha_yr = sdptv2_update[final_id == 'URY_11', removal_factor_SD_Mg_AGC_ha_yr], 
                removal_factor_Mg_AGC_BGC_ha_yr.y = sdptv2_update[final_id == 'URY_11', removal_factor_Mg_AGC_BGC_ha_yr.y],
                removal_factor_SD_Mg_AGC_BGC_ha_yr.y = sdptv2_update[final_id == 'URY_11', removal_factor_SD_Mg_AGC_BGC_ha_yr.y], 
                removal_factor_source.y = sdptv2_update[final_id == 'URY_11', removal_factor_source.y], 
                sd_source.y = sdptv2_update[final_id == 'URY_11', sd_source.y], 
                removal_factor_region = sdptv2_update[final_id == 'URY_11', removal_factor_region], 
                removal_factor_notes.y = 'Assume similar to other Eucalyptus sp. for Uruguay.')]

#URY_16: Pinus elliotti and Pinus taeda
sdptv2_update[final_id == 'URY_16', ":=" 
              (`agc (t C/ha/yr)` = sdptv2_update[final_id == 'URY_18', `agc (t C/ha/yr)`], 
                agc_uncertainty = sdptv2_update[final_id == 'URY_18', agc_uncertainty], 
                `bgc (t C/ha/yr)` = sdptv2_update[final_id == 'URY_18', `bgc (t C/ha/yr)`], 
                bgc_uncertainty = sdptv2_update[final_id == 'URY_18', bgc_uncertainty], 
                removal_factor_Mg_AGC_ha_yr = sdptv2_update[final_id == 'URY_18', removal_factor_Mg_AGC_ha_yr], 
                removal_factor_SD_Mg_AGC_ha_yr = sdptv2_update[final_id == 'URY_18', removal_factor_SD_Mg_AGC_ha_yr], 
                removal_factor_Mg_AGC_BGC_ha_yr.y = sdptv2_update[final_id == 'URY_18', removal_factor_Mg_AGC_BGC_ha_yr.y],
                removal_factor_SD_Mg_AGC_BGC_ha_yr.y = sdptv2_update[final_id == 'URY_18', removal_factor_SD_Mg_AGC_BGC_ha_yr.y], 
                removal_factor_source.y = sdptv2_update[final_id == 'URY_18', removal_factor_source.y], 
                sd_source.y = sdptv2_update[final_id == 'URY_18', sd_source.y], 
                removal_factor_region = sdptv2_update[final_id == 'URY_18', removal_factor_region], 
                removal_factor_notes.y = 'Assume similar to Pinus pinaster for Uruguay.')]

#URY_17: Salix sp. and Populus sp. 
rf_average('URY_17', 'ARG_22', 'ARG_25', sdptv2_update)

sdptv2_update[final_id == 'URY_17' , ":="
              (removal_factor_source.y = 'FAO 2006 Planted Forest Assessment Table 6a | IPCC 2019 Refinement MAI Table 4.11',
                removal_factor_region = 'South America', 
                removal_factor_notes.y = 'Average of Salix sp. for Argentina and Populus sp. for South America.')]

#Mexico 
mex_mix_list <- sdptv2_update[iso3 == 'MEX' & is.na(sciName2) == FALSE & is.na(removal_factor_Mg_AGC_ha_yr) == TRUE, final_id]

for(i in 1:(length(mex_mix_list)+1)){
  id <- mex_mix_list[i]
  rf_average_country(id, sdptv2_update, 'Mexico')
}

#Guatemala 
gtm_mix_list <- sdptv2_update[iso3 == 'GTM' & is.na(sciName2) == FALSE & is.na(removal_factor_Mg_AGC_ha_yr) == TRUE, final_id]

for(i in 1:(length(gtm_mix_list)+1)){
  id <- gtm_mix_list[i]
  rf_average_country(id, sdptv2_update, 'Guatemala')
}


##Step 5: QA/QC
#Check that all classes have AGB and BGB split
check <- sdptv2_update[is.na(`agc (t C/ha/yr)`) & is.na(`bgc (t C/ha/yr)`),]
sdptv2_update[is.na(`agc (t C/ha/yr)`) & is.na(`bgc (t C/ha/yr)`), ":="
              (`agc (t C/ha/yr)` = , 
                `bgc (t C/ha/yr)` = , 
                agc_uncertainty = , 
                bgc_uncertainty = , 
                removal_factor_Mg_AGC_ha_yr = , 
                removal_factor_SD_Mg_AGC_ha_yr = ,
                removal_factor_source.y = , 
                sd_source.y = , )]


#Step 6: Write out csv/ xlsx  

write.xlsx(sdptv2_update, 'C://Users/Melissa.Rose/OneDrive - World Resources Institute/Documents/Projects/sdpt/SPDT_v2.0_updates_MR_083123.xlsx')


#####################################################################################################################

#To keep version 1 rf
#sdptv2_calc_update <- sdptv2_comb[is.na(removal_factor_Mg_AGC_BGC_ha_yr.x), ":=" 
#                                  (removal_factor_Mg_AGC_BGC_ha_yr.x = removal_factor_Mg_AGC_BGC_ha_yr.y, 
#                                    removal_factor_SD_Mg_AGC_BGC_ha_yr.x = removal_factor_SD_Mg_AGC_BGC_ha_yr.y, 
#                                    removal_factor_notes = removal_factor_source.y)]

#sdptv2_calc_update[, ":=" (removal_factor_Mg_AGC_BGC_ha_yr.y = NULL,                                            
#                           removal_factor_SD_Mg_AGC_BGC_ha_yr.y = NULL, 
#                           removal_factor_source.y = NULL)]