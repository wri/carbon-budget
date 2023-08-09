#Author: Melissa Rose
#Date Modified: 7-7-2023
#Title: SDPT Version 2 RF Updates
#To Do: Apply different default rubber rate to countries in S America
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

#Step 3: Identify final_id codes that do not currently have a RF value (will be used as a check)
sdptv1_codelist <- unique(sdpt_v1$final_id)
sdptv2_codelist <- unique(sdpt_v2$final_id)
empty_codelist <- setdiff(sdptv2_codelist, sdptv1_codelist)   #1347 classes with no current RF

#Step 4: Join tables based on final_id value
sdptv1_trunc <- sdpt_v1[duplicated(sdpt_v1$final_id) == FALSE, ]           #remove 47 duplicated final_id codes from version 1
sdptv1_trunc <- sdptv1_trunc[, .(final_id, growth, SD_error, grow_source)] #keep only columns in v1 that need to be copied over to v2
sdptv2_update <- left_join(sdpt_v2, sdptv1_trunc, by = 'final_id')         #join v1 and v2 by 'final_id'
sdptv2_update[, removal_factor_Mg_AGC_BGC_ha_yr := growth]                 #move v1 data into v2 columns
sdptv2_update[, removal_factor_SD_Mg_AGC_BGC_ha_yr := SD_error]
sdptv2_update[, removal_factor_source := grow_source]                 
sdptv2_update[, ":=" (growth = NULL,                                       #remove v1 columns from updated v2 dataset
                     SD_error = NULL, 
                     grow_source = NULL)]                          
sdptv2_update$removal_factor_note <- as.character('')                      #add removal_factor_notes column
                                                                       
#check
sum(is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr))                  #this value is 2 more than the empty_codelist
#Aha! This is because USA_28 and USA_29 did not have removal factors in the v1 data set

#Identify rows which need updated removal factors 
sdptv2_empty <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr)]  #1349 rows which need updated removal factors (many mixed)
sdptv2_empty_unmix <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr) &  is.na(sdptv2_update$sciName2)]   #only 430 single class rows need updated removal factors

#STEP 5: PALM OIL
sdptv2_update[simpleName == 'Oil palm' & is.na(sciName2) & is.na(removal_factor_Mg_AGC_BGC_ha_yr) |
                simpleName == 'Oil palm mix' & is.na(sciName2) & is.na(removal_factor_Mg_AGC_BGC_ha_yr), ":="
                       (removal_factor_Mg_AGC_BGC_ha_yr = 3.024,
                         removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.9859766,
                         removal_factor_source = 'IPCC 2019 Refinement Cropland Table 5.3', 
                         sd_source = 'IPCC 2019 Refinement Cropland Table 5.3',
                         root_source = 'Mokany et al. 2006',
                         removal_factor_notes = 'Oil palm default rate: AGB 2.4 +/- 0.41 t C/ha/yr. Added in BGB (x0.26).')]

#STEP 6: RUBBER
#To Do: Apply different default rubber rate to countries in S America
sdptv2_update[simpleName == 'Rubber' & is.na(sciName2) & is.na(removal_factor_Mg_AGC_BGC_ha_yr) |
                simpleName == 'Rubber mix' & is.na(sciName2) & is.na(removal_factor_Mg_AGC_BGC_ha_yr), ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 3.780000,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.3977235,
                removal_factor_source = 'IPCC 2019 Refinement Cropland Table 5.3', 
                sd_source = 'IPCC 2019 Refinement Cropland Table 5.3',
                root_source = 'Mokany et al. 2006',
                removal_factor_notes = 'Tropical rubber default rate: AGB 3.0 +/- 0 .13 t C/ha/yr. Added in BGB (x0.26)')]
##############################################START UPDATING HERE

#STEP 7: SHADED PERENNIAL (COFFEE, COCOA)
#Africa - tropical, wet (Democratic Republic of the Congo)
sdptv2_update[final_id == 'COD_11'| final_id == 'COD_12', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 3.87,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 2.3710633,
                removal_factor_source = '2019 IPCC Refinement Cropland Table 5.2.', 
                removal_factor_notes = 'Africa, tropical wet, shaded perennial default rates: AGB 3.16 t C/ha/yr and BGB 0.71 t C/ha/yr')]

#Asia - tropical, wet (Indonesia)
sdptv2_update[final_id == 'IDN_201' | final_id == 'IDN_202', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 2.21,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.3431568,
                removal_factor_source = '2019 IPCC Refinement Cropland Table 5.2.', 
                removal_factor_notes = 'Asia, tropical wet, shaded perennial default rates: AGB 1.79 t C/ha/yr and BGB 0.42 t C/ha/yr')]

#Central America - tropical wet (Panama)
sdptv2_update[final_id == 'PAN_14', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 2.790,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.9589571,
                removal_factor_source = '2019 IPCC Refinement Cropland Table 5.2.', 
                removal_factor_notes = 'Central America, tropical wet, shaded perennial default rates: AGB 2.28 t C/ha/yr and BGB 0.51 t C/ha/yr')]

#Identify rows which need updated removal factors 
sdptv2_empty <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr)]  #1276 rows which need updated removal factors (many mixed)
sdptv2_empty_unmix <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr) &  is.na(sdptv2_update$sciName2)]   #only 357 single class rows need updated removal factors


#STEP 8: ORCHARD [103 rows updated]
#Temperate orchard default
sdptv2_update[originalCode == 7215 & is.na(sciName2) & is.na(removal_factor_Mg_AGC_BGC_ha_yr) | final_id == 'CHL_140' | final_id == 'ZAF_35', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 0.54,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.1981157,
                removal_factor_source = '2019 IPCC Refinement Cropland Table 5.3.', 
                removal_factor_notes = 'Temperate orchard default rate 0.43 t C/ha/yr. Added in BGB (x0.26)')]
#Japan
sdptv2_update[final_id == 'JPN_7215', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 2.20,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.4400000,
                removal_factor_source = 'https://academic.oup.com/forestry/article/82/1/105/528043', 
                removal_factor_notes = 'Table 2 here suggests combined AGB and BGB of 2.2 t C/ha/yr across all Japan plantations')]


#Identify rows which need updated removal factors 
sdptv2_empty <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr)]  #1173 rows which need updated removal factors (many mixed)
sdptv2_empty_unmix <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr) &  is.na(sdptv2_update$sciName2)]   #only 254 single class rows need updated removal factors

#Step 9: FRUIT [9 rows updated]
#Central America - tropical wet (Belize, Panama)
sdptv2_update[iso3 == 'BLZ' & simpleName == 'Fruit' & is.na(sciName2) & is.na(removal_factor_Mg_AGC_BGC_ha_yr) | 
                iso3 == 'PAN' & simpleName == 'Fruit' & is.na(sciName2) & is.na(removal_factor_Mg_AGC_BGC_ha_yr), ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 4.03,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 2.438747681,
                removal_factor_source = '2019 IPCC Refinement Cropland Table 5.2', 
                removal_factor_notes = 'Central America, tropical wet, multistrata default rates: AGB 3.25 t C/ha/yr and BGB 0.78 t C/ha/yr.')]

#South America - tropical, wet (Ecuador) [Note: Also updating Columbia - banana which previously had Africa multistrata data]
sdptv2_update[iso3 == 'ECU' & simpleName == 'Fruit' & is.na(sciName2) & is.na(removal_factor_Mg_AGC_BGC_ha_yr)|
                final_id == 'COL_14', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 3.3,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.094241,
                removal_factor_source = '2019 IPCC Refinement Cropland Table 5.2', 
                removal_factor_notes = 'South America, tropical wet, multistrata default rates: AGB 2.6 t C/ha/yr and BGB 0.70 t C/ha/yr.')]

#Africa - tropical, wet (Cameroon)
sdptv2_update[iso3 == 'CMR' & simpleName == 'Fruit' & is.na(sciName2) & is.na(removal_factor_Mg_AGC_BGC_ha_yr), ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 3.58,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 2.168598,
                removal_factor_source = '2019 IPCC Refinement Cropland Table 5.2', 
                removal_factor_notes = 'Africa, tropical wet, multistrata default rates: AGB 2.89 t C/ha/yr and BGB 0.69 t C/ha/yr')]

#China - Morus sp. 
sdptv2_update[final_id == 'CHN_8' , ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 3.153465,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.057704316,
                removal_factor_source = 'FAO 2006 Planted Forest Assessment', 
                removal_factor_notes = 'MAI values 2 - 8 for China (assume Morus alba). Added in BGB (x0.26)')]

#North Korea - Castanea sp. 
sdptv2_update[final_id == 'PRK_3' , ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 2.26,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.452,
                removal_factor_source = 'https://doi.org/10.1080/21580103.2019.1596843', 
                removal_factor_notes = 'Assume similar to Castanea sp. in South Korea (see KOR_139)')]

#Identify rows which need updated removal factors 
sdptv2_empty <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr)]  #1164 rows which need updated removal factors (many mixed)
sdptv2_empty_unmix <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr) &  is.na(sdptv2_update$sciName2)]   #only 245 single class rows need updated removal factors

#Step 10: TEAK [3 rows updated]
#Ecuador
sdptv2_update[final_id == 'ECU_3', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 5.418,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.952583456,
                removal_factor_source = 'https://doi.org/10.1093/forestry/cpaa017', 
                removal_factor_notes = 'AGB 4.3 +/- 0.22 t C/ha/yr for T. grandis in Ecuador. Added in BGB (x0.26)')]

#Trinidad and Tobago
sdptv2_update[final_id == 'TTO_32', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 3.44438325,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.926849301,
                removal_factor_source = 'Pandey, D., & Brown, C. (2000). Teak: a global overview. UNASYLVA-FAO-, 3-13', 
                removal_factor_notes = 'MAI values 3.9 - 10.2 for Trinidad and Tobago. Added in BGB (x0.26)')]

#Argentina 
sdptv2_update[final_id == 'ARG_29', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 4.8074796,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.214413393,
                removal_factor_source = '2019 IPCC Refinement MAI Table 4.11', 
                removal_factor_notes = 'MAI values 7.3 - 17.3 for South America (assume slow growth/ temperate). Added in BGB (x0.26)')]

#India 
sdptv2_update[final_id == 'IND_37' | final_id == 'IND_92', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 3.49323975,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.362814499,
                removal_factor_source = 'Pandey, D., & Brown, C. (2000). Teak: a global overview. UNASYLVA-FAO-, 3-13', 
                removal_factor_notes = 'MAI values 2 - 12.3 for India. Added in BGB (x0.26)')]

#Indonesia 
sdptv2_update[final_id == 'IDN_7' | final_id == 'IDN_48' | final_id == 'IDN_118', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 7.4750445,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.787742385,
                removal_factor_source = 'Pandey, D., & Brown, C. (2000). Teak: a global overview. UNASYLVA-FAO-, 3-13', 
                removal_factor_notes = 'MAI values 9.6 - 21 for Indonesia. Added in BGB (x0.26)')]

#Mexico 
sdptv2_update[final_id == 'MEX_27', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 4.95893475,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.018822771,
                removal_factor_source = 'Fierros, M. (2012). Programa de Desarrollo de Plantaciones Forestales Comerciales: a 15 años de su creación. CONAFOR, Jalisco, México.', 
                removal_factor_notes = 'MAI values 7.3 - 13 for Mexico. Added in BGB (x0.26)')]

#Identify rows which need updated removal factors 
sdptv2_empty <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr)]  #1161 rows which need updated removal factors (many mixed)
sdptv2_empty_unmix <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr) &  is.na(sdptv2_update$sciName2)]   #only 242 single class rows need updated removal factors

#STEP 11: EUCALYPTUS [21 rows updated]
#South and Southeast Asia: Eucalyptus sp. - productive (India and Indonesia) 
sdptv2_update[final_id == 'IND_93' | final_id == 'IDN_21', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 4.56541785,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.910946959,
                removal_factor_source = '2019 IPCC Refinement MAI Table 4.11', 
                removal_factor_notes = 'MAI values 7 - 12 for South and Southeast Asia. Added in BGB (x0.26)')]

#South and Southeast Asia: Eucalyptus sp. - average of productive and semi-natural (Thailand) 
sdptv2_update[final_id == 'THA_133', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 4.685560425,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.889810333,
                removal_factor_source = '2019 IPCC Refinement MAI Table 4.11', 
                removal_factor_notes = 'Average of productive (MAI values 7 - 12) and semi-natural (MAI values 8 - 12) for South and Southeast Asia. Added in BGB (x0.26)')]

#East Asia: Eucalyptus sp. - productive (Japan) 
sdptv2_update[final_id == 'JPN_6', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 2.474937045,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.930295386,
                removal_factor_source = '2019 IPCC Refinement MAI Table 4.11', 
                removal_factor_notes = 'MAI values 1.6 - 8.7 for East Asia. Added in BGB (x0.26)')]

#East and Southern Africa: Eucalyptus grandis (Rwanda) 
sdptv2_update[final_id == 'RWA_8', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 10.0919763,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.676667008,
                removal_factor_source = '2019 IPCC Refinement MAI Table 4.11', 
                removal_factor_notes = 'MAI values 18 - 24 for East and Southern Africa (assume E. grandis). Added in BGB (x0.26)')]

#Argentina
sdptv2_update[final_id == 'ARG_7', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 9.22694976,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.499379336,
                removal_factor_source = '2006 FAO Planted Forest Assessment Table 6a', 
                removal_factor_notes = 'MAI values 21 - 27 for Argentina (assume E. grandis). Added in BGB (x0.26)')]

#Chile
#E. globulus
sdptv2_update[final_id == 'CHL_1', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 5.32731276,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.436773507,
                removal_factor_source = '2006 FAO Planted Forest Assessment Table 6a', 
                removal_factor_notes = 'MAI values 8 - 21 for Chile. Added in BGB (x0.26)')]
#E. nitens
sdptv2_update[final_id == 'CHL_2', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 5.57461548,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.503470923,
                removal_factor_source = '2006 FAO Planted Forest Assessment Table 6a', 
                removal_factor_notes = 'MAI values 8 - 21 for Chile. Added in BGB (x0.26)')]

#China
sdptv2_update[final_id == 'CHN_6', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 6.96826935,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.879338654,
                removal_factor_source = '2006 FAO Planted Forest Assessment Table 6a', 
                removal_factor_notes = 'MAI values 8 - 21 for China. Added in BGB (x0.26)')]

#Uruguay 
sdptv2_update[final_id == 'URY_11' | final_id == 'URY_12', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 9.85169115,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.831100549,
                removal_factor_source = '2000 FAO Global Forest Products Outlook Table 6', 
                removal_factor_notes = 'MAI values 16 - 25 for Uruguay. Added in BGB (x0.26)')]

#Ecuador
#Eucalyptus sp. 
sdptv2_update[final_id == 'ECU_14' | final_id == 'ECU_16', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 4.925845575,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.915550275,
                removal_factor_source = '2000 FAO Global Forest Products Outlook Table 6', 
                removal_factor_notes = 'MAI values 8 - 12.5 for Ecuador. Added in BGB (x0.26)')]

#E. globulus
sdptv2_update[final_id == 'ECU_7', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 4.707323775,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.874934366,
                removal_factor_source = '2000 FAO Global Forest Products Outlook Table 6', 
                removal_factor_notes = 'MAI values 8 - 12.5 for Ecuador. Added in BGB (x0.26)')]

#Guatemala
#Eucalyptus sp. 
sdptv2_update[final_id == 'GTM_6' | final_id == 'GTM_29' | final_id == 'GTM_62' | final_id == 'GTM_122' | final_id == 'GTM_203' | final_id == 'GTM_635' | final_id == 'GTM_636', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 4.925845575,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.915550275,
                removal_factor_source = '2000 FAO Global Forest Products Outlook Table 6', 
                removal_factor_notes = 'MAI values 8 - 12.5 for Guatemala. Added in BGB (x0.26)')]

#E. globulus
sdptv2_update[final_id == 'GTM_407', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 4.707323775,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.874934366,
                removal_factor_source = '2000 FAO Global Forest Products Outlook Table 6', 
                removal_factor_notes = 'MAI values 8 - 12.5 for Guatemala. Added in BGB (x0.26)')]

#Mexico
#Eucalyptus sp. 
sdptv2_update[final_id == 'MEX_29' | final_id == 'MEX_37' | final_id == 'MEX_59' | final_id == 'MEX_92' | final_id == 'MEX_93' | final_id == 'MEX_118' | final_id == 'MEX_219', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 11.38951611,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 2.074846618,
                removal_factor_source = 'Fierros, M. (2012). Programa de Desarrollo de Plantaciones Forestales Comerciales: a 15 años de su creación. CONAFOR, Jalisco, México.', 
                removal_factor_notes = 'MAI values 18.8 - 28.6 for Mexico. Added in BGB (x0.26)')]

#E. globulus
sdptv2_update[final_id == 'MEX_201', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 10.88425107,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.982801666,
                removal_factor_source = 'Fierros, M. (2012). Programa de Desarrollo de Plantaciones Forestales Comerciales: a 15 años de su creación. CONAFOR, Jalisco, México.', 
                removal_factor_notes = 'MAI values 18.8 - 28.6 for Mexico. Added in BGB (x0.26)')]

#Brazil
sdptv2_update[final_id == 'BRA_21' | final_id == 'BRA_78', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 13.51175417,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 2.244824181,
                removal_factor_source = 'https://doi.org/10.1186/s13021-018-0106-4', 
                removal_factor_notes = 'MAI values 36 - 48 for mix of E. grandis/urograndis in Brazil. Wood density of 0.5090. Average BEF of 1.16675. Root ratio of 0.170. Carbon fraction of 0.463.')]

#Identify rows which need updated removal factors 
sdptv2_empty <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr)]  #1140 rows which need updated removal factors (many mixed)
sdptv2_empty_unmix <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr) &  is.na(sdptv2_update$sciName2)]   #only 221 single class rows need updated removal factors


#STEP 12: PINE
#Japan
sdptv2_update[final_id == 'JPN_10', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 4.34600775,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.416925261,
                removal_factor_source = '2019 IPCC Refinement MAI Table 4.11', 
                removal_factor_notes = 'MAI values 4 - 15 for Asia. Added in BGB (x0.26)')]

#south America (Ecuador)
#P. radiata 
sdptv2_update[final_id == 'ECU_6', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 10.979388,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 2.81857758,
                removal_factor_source = '2019 IPCC Refinement MAI Table 4.11', 
                removal_factor_notes = 'MAI values 14 - 34 for South America. Added in BGB (x0.26)')]
#P. patula 
sdptv2_update[final_id == 'ECU_21', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 9.59364,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 2.462834778,
                removal_factor_source = '2019 IPCC Refinement MAI Table 4.11', 
                removal_factor_notes = 'MAI values 14 - 34 for South America (assume similar to P. radiata). Added in BGB (x0.26)')]

#Uruguay 
#P. pinaster
sdptv2_update[final_id == 'URY_18', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 6.058206,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.954841517,
                removal_factor_source = '2019 IPCC Refinement MAI Table 4.11', 
                removal_factor_notes = 'MAI values 14 - 17 (assume similar to P. taeda in Argentina). Added in BGB (x0.26)')]

#N and C America (Guatemala and Mexico)
#P. oocarpa
sdptv2_update[final_id == 'GTM_18' | final_id == 'MEX_15', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 4.6413675,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.706838073,
                removal_factor_source = '2019 IPCC Refinement MAI Table 4.11', 
                removal_factor_notes = 'MAI values 9 - 10 for N and C America (assume similar to Pinus taeda). Wood density of 0.55. Added in BGB (x0.26)')]

#Pinus sp. 
sdptv2_update[final_id == 'GTM_7' | final_id == 'GTM_34' | final_id == 'GTM_35' | final_id == 'GTM_264' | final_id == 'GTM_299' | final_id == 'GTM_301' | final_id == 'GTM_372' | final_id == 'GTM_414' | final_id == 'GTM_451' | final_id == 'GTM_452' |
                final_id == 'MEX_3' | final_id == 'MEX_12' | final_id == 'MEX_19' | final_id == 'MEX_23' | final_id == 'MEX_34' | final_id == 'MEX_54' | final_id == 'MEX_61' | final_id == 'MEX_80' | final_id == 'MEX_81' | final_id == 'MEX_87' | final_id == 'MEX_133' | final_id == 'MEX_184' | final_id == 'MEX_199' | final_id == 'MEX_239', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 4.34600775,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.661857468,
                removal_factor_source = '2019 IPCC Refinement MAI Table 4.11', 
                removal_factor_notes = 'MAI values 9 - 10 for N and C America (assume similar to Pinus taeda). Added in BGB (x0.26)')]

#P. caribaea 
sdptv2_update[final_id == 'GTM_13' | final_id == 'GTM_162' | final_id == 'GTM_192' | final_id == 'GTM_225' | final_id == 'MEX_32', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 4.3038135,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.655431667,
                removal_factor_source = '2019 IPCC Refinement MAI Table 4.11', 
                removal_factor_notes = 'MAI values 9 - 10 for N and C America (assume similar to Pinus taeda). Wood density of 0.51. Added in BGB (x0.26)')]

#P. patula 
sdptv2_update[final_id == 'GTM_281' | final_id == 'MEX_20', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 3.7974825,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.578322059,
                removal_factor_source = '2019 IPCC Refinement MAI Table 4.11', 
                removal_factor_notes = 'MAI values 9 - 10 for N and C America (assume similar to Pinus taeda). Wood density of 0.45. Added in BGB (x0.26)')]

#P. strobus 
sdptv2_update[final_id == 'GTM_43' | final_id == 'GTM_283', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 2.700432,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.411251242,
                removal_factor_source = '2019 IPCC Refinement MAI Table 4.11', 
                removal_factor_notes = 'MAI values 9 - 10 for N and C America (assume similar to Pinus taeda). Wood density of 0.32. Added in BGB (x0.26)')]

#China 
sdptv2_update[final_id == 'CHN_10', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 2.54906568,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.855897075,
                removal_factor_source = '2006 FAO Planted Forest Assessment Table 6a', 
                removal_factor_notes = 'Average of P. massonis (MAI values 3 - 16). P. tabulaeformis (MAI values 3 - 7), P. sylvestris var mongolica (MAI values 2 - 9), P. koraiensis (MAI values 2 - 9), and P. yunnanensis (MAI values 2 - 9) for China. Added in BGB (x0.26)')]

#Trinidad and Tobago 
# P. caribaea
sdptv2_update[final_id == 'TTO_33', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 7.248528,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.415320337,
                removal_factor_source = '2001 Mean Annual Volume Increment of Selected Industrial Forest Plantation Species', 
                removal_factor_notes = 'MAI values 12 - 20 for Trinidad and Tobago (assume Pinus caribaea var hondurensis). Wood density of 0.51. Added in BGB (x0.26)')]


#Brazil
sdptv2_update[final_id == 'BRA_46', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 10.50090005,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.851061424,
                removal_factor_source = 'https://doi.org/10.1186/s13021-018-0106-4', 
                removal_factor_notes = 'MAI values 22 - 32 for Brazil (assume Pinus taeda). Average wood density of 0.35025. Average BEF of 1.81825. Average root ratio of 0.34635. Carbon fraction of 0.4536. (https://doi.org/10.1186/s13021-018-0106-4)')]

#North Korea 
sdptv2_update[final_id == 'PRK_10', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 10.50090005,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.956678109,
                removal_factor_source = 'https://doi.org/10.1080/21580103.2019.1596843', 
                removal_factor_notes = 'Assume average of Pinus specis in South Korea (see KOR_111, KOR_112, KOR_114, KOR_115)')]

#Identify rows which need updated removal factors 
sdptv2_empty <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr)]  #1099 rows which need updated removal factors (many mixed)
sdptv2_empty_unmix <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr) &  is.na(sdptv2_update$sciName2)]   #only 180 single class rows need updated removal factors

#STEP 13: GMELINA ARBOREA [3 rows updated]
#Guatemala 
sdptv2_update[final_id == 'GTM_2', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 6.1958925,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.163230567,
                removal_factor_source = '2000 FAO Global Forest Products Outlook Table 6', 
                removal_factor_notes = 'MAI values 12 -19 for Guatemala. Added in BGB (x0.26)')]

#Ecuador 
sdptv2_update[final_id == 'ECU_22', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 6.1958925,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.163230567,
                removal_factor_source = '2000 FAO Global Forest Products Outlook Table 6', 
                removal_factor_notes = 'MAI values 12 -19 (assume similar to Guatemala). Added in BGB (x0.26)')]

#Mexico
sdptv2_update[final_id == 'MEX_4', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 7.29516375,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 1.435684925,
                removal_factor_source = 'Fierros, M. (2012). Programa de Desarrollo de Plantaciones Forestales Comerciales: a 15 años de su creación. CONAFOR, Jalisco, México.', 
                removal_factor_notes = 'MAI values 13.6 -22.9 for Mexico. Added in BGB (x0.26)')]

#Identify rows which need updated removal factors 
sdptv2_empty <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr)]  #1096 rows which need updated removal factors (many mixed)
sdptv2_empty_unmix <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr) &  is.na(sdptv2_update$sciName2)]   #only 177 single class rows need updated removal factors

#STEP 14: ABIES [2 rows updated]
#North Korea 
sdptv2_update[final_id == 'PRK_0', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 2.26,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.452,
                removal_factor_source = 'https://doi.org/10.1080/21580103.2019.1596843', 
                removal_factor_notes = 'Assume similar to Abies sp. in South Korea (see KOR_116)')]

#Japan
sdptv2_update[final_id == 'JPN_0', ":="
              (removal_factor_Mg_AGC_BGC_ha_yr = 2.2,
                removal_factor_SD_Mg_AGC_BGC_ha_yr = 0.44,
                removal_factor_source = 'https://academic.oup.com/forestry/article/82/1/105/528043', 
                removal_factor_notes = 'Table 2 here suggests 2.2 t C/ha/yr in AGB and BGB across all Japan plantations')]

#Identify rows which need updated removal factors 
sdptv2_empty <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr)]  #1094 rows which need updated removal factors (many mixed)
sdptv2_empty_unmix <- sdptv2_update[is.na(sdptv2_update$removal_factor_Mg_AGC_BGC_ha_yr) &  is.na(sdptv2_update$sciName2)]   #only 175 single class rows need updated removal factors

#STEP 15: 

#_______________________________________________________________________________



sdptv2_update_unmix <- sdptv2_update[is.na(sdptv2_update$sciName2)]



sdptv2_empty_unmix_xGTM_MEX <- sdptv2_empty_unmix[iso3 != 'GTM']
sdptv2_empty_unmix_xGTM_MEX  <- sdptv2_empty_unmix_xGTM_MEX [iso3 != 'MEX']


nrow(sdptv2_empty_unmix[iso3 == 'GTM'])  #65
nrow(sdptv2_empty_unmix[iso3 == 'MEX'])  #66


#_______________________________________________________________________________

sdptv2_update[, removal_factor_note := NULL]

write.xlsx(sdptv2_update_unmix, 'C://Users/Melissa.Rose/OneDrive - World Resources Institute/Documents/Projects/sdpt/SPDT_v2.0_singleClass_updates_MR_072323.xlsx')


