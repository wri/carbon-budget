### Code for running Hadoop clusters on the model endpoints
### then using the cumulative summing script to sum the endpoint and area by tcd threshold.
### Also, sample code for copying results from spot machine to s3 for two endpoints.

### git clone https://github.com/wri/gfw-annual-loss-processing

'''
For annual gain rate:
python annual_update.py --analysis-type annualGain --points-folder s3://gfw2-data/climate/carbon_model/model_output_tsv/20181116/annualGain_tcd2000/ --output-folder s3://gfw2-data/climate/carbon_model/model_output_Hadoop/raw/annualGain_tcd2000/20181120/ --polygons-folder s3://gfw2-data/alerts-tsv/country-pages/climate/tsvs/ --iterate-by points
python cumsum_hadoop_output.py --input s3://gfw2-data/climate/carbon_model/model_output_Hadoop/raw/annualGain_tcd2000/20181120/ --no-years --analysis-name annualGain


For cumulative gain rate:
python annual_update.py --analysis-type cumulGain --points-folder s3://gfw2-data/climate/carbon_model/model_output_tsv/20181116/cumulGain_tcd2000/ --output-folder s3://gfw2-data/climate/carbon_model/model_output_Hadoop/raw/cumulGain_tcd2000/20181120/ --polygons-folder s3://gfw2-data/alerts-tsv/country-pages/climate/tsvs/ --iterate-by points
python cumsum_hadoop_output.py --input s3://gfw2-data/climate/carbon_model/model_output_Hadoop/raw/cumulGain_tcd2000/20181120/ --no-years --analysis-name cumulGain
aws s3 cp /home/ubuntu/gfw-annual-loss-processing/2_Cumulate-Results-and-Create-API-Datasets/processing/XXXXXXXXXXXXXXXXXXXXXXXX/output.csv s3://gfw2-data/climate/carbon_model/model_output_Hadoop/processed/cumulGain_tcd2000/20181121/cumulGain_t_carbon_2001_15.csv


For net emissions:
python annual_update.py --analysis-type netEmis --points-folder s3://gfw2-data/climate/carbon_model/model_output_tsv/20181116/netEmis_tcd2000/ --output-folder s3://gfw2-data/climate/carbon_model/model_output_Hadoop/raw/netEmis_tcd2000/20181120/ --polygons-folder s3://gfw2-data/alerts-tsv/country-pages/climate/tsvs/ --iterate-by points
python cumsum_hadoop_output.py --input s3://gfw2-data/climate/carbon_model/model_output_Hadoop/raw/netEmis_tcd2000/20181120/ --no-years --analysis-name netEmis


For gross emissions:
python annual_update.py --analysis-type grossEmis --points-folder s3://gfw2-data/climate/carbon_model/model_output_tsv/20181119/grossEmis_tcd2000_tcl/ --output-folder s3://gfw2-data/climate/carbon_model/model_output_Hadoop/raw/grossEmis_tcd2000_tcl/20181120/ --polygons-folder s3://gfw2-data/alerts-tsv/country-pages/climate/tsvs/ --iterate-by points --dryrun
python cumsum_hadoop_output.py --input s3://gfw2-data/climate/carbon_model/model_output_Hadoop/raw/grossEmis_tcd2000_tcl/20181120/ --max-year 2015 --analysis-name grossEmis
# The cumsum for gross emissions takes a few minutes because there are so many more rows in it (for each year)
aws s3 cp /home/ubuntu/gfw-annual-loss-processing/2_Cumulate-Results-and-Create-API-Datasets/processing/357b9433-185e-4c2f-8659-ec613eb58250/output.csv s3://gfw2-data/climate/carbon_model/model_output_Hadoop/processed/grossEmis_tcd2000_tcl/20181121/grossEmis_t_CO2_2001_15.csv
'''


