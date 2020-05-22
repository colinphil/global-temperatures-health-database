import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
import datetime
import pandas as pd

class FilterDateFn(beam.DoFn):
  def process(self, element):
    country_record = element
    
    #establish earliest acceptable date to compare to
    #obtained from earliest date from Date entity table
    cutoffDate = datetime.date(1755,9,1)
    
    #get the date attribute
    dt = country_record.get('dt')
    
    #create date object from dt to compare to cutoff
    formatted_dt = pd.to_datetime(dt)
    
    #add record if after cutoffDate
    if formatted_dt > cutoffDate:
        return [country_record]
       
    #return empty list if before cutoff date
    return []

def run():
    PROJECT_ID = 'electric-spark-266716' # change to your project id

     # Project ID is required when using the BQ source
    options = {
     'project': PROJECT_ID
     }
    opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # Create beam pipeline using local runner
    p = beam.Pipeline('DirectRunner', options=opts)

    #create query to select all elements for cleansing
    sql = 'SELECT dt, AverageTemperature, AverageTemperatureUncertainty, Country \
    FROM kaggle_modeled.Country as x limit 20'
    
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)
    
    #read desired table from BigQuery
    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    
    #write inputs to input.txt
    query_results | 'Write input' >> WriteToText('input_country.txt')
    
    # apply ParDo to filter out dates 
    formatted_country_pcoll = query_results | 'Filter Dates' >> beam.ParDo(FilterDateFn())
    
    # display filtered countries
    formatted_country_pcoll | 'Write filtered dates' >> WriteToText('output_country.txt')
    
    #create new table in BigQuery
    dataset_id = 'kaggle_modeled'
    table_id = 'Country_Beam'
    schema_id = 'dt:DATE,AverageTemperature:FLOAT,AverageTemperatureUncertainty:FLOAT,Country:STRING'

    # write PCollection to new BQ table
    formatted_country_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                  table=table_id, 
                                                  schema=schema_id,
                                                  project=PROJECT_ID,
                                                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
     
    result = p.run()
    result.wait_until_finish()      

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)
    run()
    

  