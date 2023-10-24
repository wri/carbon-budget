#Author: Melissa Rose
#Date Modified: 08-14-2023
#Title: SDPTv2_utils
###############################################################################

rf_average <- function(ID, ID1, ID2, datatable){
  #Aboveground
  datatable[final_id == ID , ":="
                ( `agc (t C/ha/yr)` = mean(c(datatable[final_id == ID1, `agc (t C/ha/yr)`], 
                                             datatable[final_id == ID2, `agc (t C/ha/yr)`])), 
                  
                  agc_uncertainty = sqrt(((datatable[final_id == ID1, `agc (t C/ha/yr)`] * datatable[final_id == ID1, agc_uncertainty])^2)
                                         +((datatable[final_id == ID2, `agc (t C/ha/yr)`] * datatable[final_id == ID2, agc_uncertainty])^2))
                  /(datatable[final_id == ID1, `agc (t C/ha/yr)`] + datatable[final_id == ID2, `agc (t C/ha/yr)`]))]
  #Belowground 
  datatable[final_id == ID , ":="
                (`bgc (t C/ha/yr)` = 0.26 * `agc (t C/ha/yr)` ,
                  bgc_uncertainty = agc_uncertainty)]
  #combined
  datatable[final_id == ID , ":="
                (removal_factor_Mg_AGC_ha_yr = `agc (t C/ha/yr)`, 
                  removal_factor_SD_Mg_AGC_ha_yr = `agc (t C/ha/yr)` * agc_uncertainty, 
                  removal_factor_Mg_AGC_BGC_ha_yr.y = `agc (t C/ha/yr)` + `bgc (t C/ha/yr)`,
                  removal_factor_SD_Mg_AGC_BGC_ha_yr.y = (`agc (t C/ha/yr)` + `bgc (t C/ha/yr)`) * agc_uncertainty,
                  sd_source.y = 'Standard deviation calculated from combined standard deviations of input classes')]
}

rf_average_country <- function(id_value, datatable, country_name) {
  sciName_list <- as.data.table(t(remove_empty(as.data.frame(datatable[final_id == id_value, .(sciName1, sciName2, sciName3, sciName4, sciName5, sciName6, sciName7, sciName8, sciName9, sciName10, sciName11, sciName12, sciName13)]), which = 'cols')))
  if(ncol(sciName_list)>0){colnames(sciName_list)[1] = 'sciName'}
  
  sciName_list[, ":=" 
               (final_id = as.character(NA), 
                 `agc (t C/ha/yr)` = as.numeric(NA), 
                 removal_factor_SD_Mg_AGC_ha_yr = as.numeric(NA))]
  
  for(i in 1:(nrow(sciName_list)+1)){
    species <- sciName_list[i, 1]
    id <- datatable[country == country_name & sciName1 == paste0(species) & is.na(sciName2), final_id]
    if(length(id)>1){id <- id[1]}
    if(length(id)>0){sciName_list[i, 2] <- id}
  }
  
  for(i in 1:(nrow(sciName_list)+1)){
    id <- sciName_list[i, 2]
    agc <- datatable[final_id == paste0(id), `agc (t C/ha/yr)`]
    agc_sd <- datatable[final_id == paste0(id), removal_factor_SD_Mg_AGC_ha_yr] 
    sciName_list[final_id == id, `agc (t C/ha/yr)` := agc]
    sciName_list[final_id == id, removal_factor_SD_Mg_AGC_ha_yr := agc_sd]
  }
  
  if(country_name == 'Mexico'){
  sciName_list[is.na(sciName_list$final_id), ":="  
               ( sciName = 'average RF',
                 final_id = 'average RF', 
                 `agc (t C/ha/yr)` = 3.796803393,
                 removal_factor_SD_Mg_AGC_ha_yr = 0.846613629)]}
  
  if(country_name == 'Guatemala'){
    sciName_list[is.na(sciName_list$final_id), ":="  
                 ( sciName = 'average RF',
                   final_id = 'average RF', 
                   `agc (t C/ha/yr)` = 4.863677551,
                   removal_factor_SD_Mg_AGC_ha_yr = 1.519178923)]}
  
  
  
  sciName_list[, agc_sd_2 := (removal_factor_SD_Mg_AGC_ha_yr)^2]
  
  agc_avg <- mean(sciName_list[, `agc (t C/ha/yr)`])
  bgc_avg <- 0.26*agc_avg
  agc_avg_sd <- sqrt(sum(sciName_list[, agc_sd_2])/nrow(sciName_list))
  agc_avg_unc <- agc_avg_sd / agc_avg
  bgc_avg_unc <- agc_avg_unc
  agc_bgc_avg <- agc_avg + bgc_avg
  combined_unc <- agc_avg_unc
  agc_bgc_avg_sd <- agc_bgc_avg*combined_unc
  
  
  datatable[final_id == id_value, ':=' (
    `agc (t C/ha/yr)` = agc_avg, 
    agc_uncertainty = agc_avg_unc, 
    `bgc (t C/ha/yr)` = bgc_avg, 
    bgc_uncertainty = bgc_avg_unc, 
    removal_factor_Mg_AGC_ha_yr = agc_avg, 
    removal_factor_SD_Mg_AGC_ha_yr = agc_avg_sd, 
    removal_factor_Mg_AGC_BGC_ha_yr.y = agc_bgc_avg, 
    removal_factor_SD_Mg_AGC_BGC_ha_yr.y = agc_bgc_avg_sd, 
    removal_factor_source.y = 'mix', 
    sd_source.y = 'Standard deviation calculated from combined standard deviations of input classes',
    removal_factor_region = country_name, 
    removal_factor_notes.y = paste0('Assume average of ', word(toString(sciName_list[,sciName]), start = 0, end = -3), ' and ', word(toString(sciName_list[,sciName]), start = -2, end = -1), ' for ', country_name, '.')
  )]
}