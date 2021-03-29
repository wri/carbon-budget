:: Script to import all csvs created by Geotrellis zonal stats into tables in a local Postgres database
:: Lines must be uncommented according to the model being imported, e.g., standard, maxgain, soil_only, etc.
:: David Gibbs, david.gibbs@wri.org

FOR %%I IN (output\carbonflux_20210324_0439\iso\summary\*.csv) DO psql -d flux_model -U postgres -c "\copy standard_iso_summary_20210323 FROM %%I CSV HEADER DELIMITER e'\t'
FOR %%I IN (output\carbonflux_20210324_0439\iso\change\*.csv) DO psql -d flux_model -U postgres -c "\copy standard_iso_change_20210323 FROM %%I CSV HEADER DELIMITER e'\t'

::FOR %%I IN (output\soil_only\iso\summary\*.csv) DO psql -d flux_model -U postgres -c "\copy soil_only_iso_summary_20200904 FROM %%I CSV HEADER DELIMITER e'\t'
::FOR %%I IN (output\soil_only\iso\change\*.csv) DO psql -d flux_model -U postgres -c "\copy soil_only_iso_change_20200904 FROM %%I CSV HEADER DELIMITER e'\t'

::FOR %%I IN (output\maxgain\iso\summary\*.csv) DO psql -d flux_model -U postgres -c "\copy maxgain_iso_summary_20200921 FROM %%I CSV HEADER DELIMITER e'\t'
::FOR %%I IN (output\maxgain\iso\change\*.csv) DO psql -d flux_model -U postgres -c "\copy maxgain_iso_change FROM %%I CSV HEADER DELIMITER e'\t'

::FOR %%I IN (output\no_shifting_ag\iso\summary\*.csv) DO psql -d flux_model -U postgres -c "\copy no_shifting_ag_iso_summary_20200921 FROM %%I CSV HEADER DELIMITER e'\t'
::FOR %%I IN (output\no_shifting_ag\iso\change\*.csv) DO psql -d flux_model -U postgres -c "\copy no_shifting_ag_iso_change FROM %%I CSV HEADER DELIMITER e'\t'

::FOR %%I IN (output\convert_to_grassland\iso\summary\*.csv) DO psql -d flux_model -U postgres -c "\copy convert_to_grassland_iso_summary_20200921 FROM %%I CSV HEADER DELIMITER e'\t'
::FOR %%I IN (output\convert_to_grassland\iso\change\*.csv) DO psql -d flux_model -U postgres -c "\copy convert_to_grassland_iso_change FROM %%I CSV HEADER DELIMITER e'\t'

::FOR %%I IN (output\biomass_swap\iso\summary\*.csv) DO psql -d flux_model -U postgres -c "\copy biomass_swap_iso_summary_20200921 FROM %%I CSV HEADER DELIMITER e'\t'
::FOR %%I IN (output\biomass_swap\iso\change\*.csv) DO psql -d flux_model -U postgres -c "\copy biomass_swap_iso_change FROM %%I CSV HEADER DELIMITER e'\t'

::FOR %%I IN (output\US_removals\iso\summary\*.csv) DO psql -d flux_model -U postgres -c "\copy US_removals_iso_summary_20200921 FROM %%I CSV HEADER DELIMITER e'\t'
::FOR %%I IN (output\US_removals\iso\change\*.csv) DO psql -d flux_model -U postgres -c "\copy US_removals_iso_change FROM %%I CSV HEADER DELIMITER e'\t'

::FOR %%I IN (output\no_primary_gain\iso\summary\*.csv) DO psql -d flux_model -U postgres -c "\copy no_primary_gain_iso_summary_20200921 FROM %%I CSV HEADER DELIMITER e'\t'
::FOR %%I IN (output\no_primary_gain\iso\change\*.csv) DO psql -d flux_model -U postgres -c "\copy no_primary_gain_iso_change FROM %%I CSV HEADER DELIMITER e'\t'

::FOR %%I IN (output\legal_Amazon_loss\iso\summary\*.csv) DO psql -d flux_model -U postgres -c "\copy legal_Amazon_loss_iso_summary_20200921 FROM %%I CSV HEADER DELIMITER e'\t'
::FOR %%I IN (output\legal_Amazon_loss\iso\change\*.csv) DO psql -d flux_model -U postgres -c "\copy legal_Amazon_loss_iso_change_20200921 FROM %%I CSV HEADER DELIMITER e'\t'


