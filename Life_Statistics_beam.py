import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
import datetime
import pandas as pd

class TransposeDateFn(beam.DoFn):
  def process(self, element):
    life_record = element
    
    #get the date,city attribute
    countryName = life_record.get('countryName')
    countryCode = life_record.get('countryCode')
    metric = life_record.get('metric')
    metricCode = life_record.get('metricCode')
    
    #contains name of year columns
    yearList = []
    recordList = []
    
    #obtain columns to be split
    for i in range(1960, 2016):
        yearList.append("yr_"+str(i))
    
    #iterate through each column
    for year in yearList:
        
        #get related statistic
        statistic = life_record.get(year)
        
        #convert year to date object
        year_date = year[3::] + '-01-01'
        
        #only add date if statistic is not null
        if statistic != None:
            recordList.append({'dt':year_date, 'countryName':countryName, 'countryCode':countryCode, 'metric':metric, \
                               'metricCode':metricCode, 'statistic': statistic})
    
    return [recordList]

#function to flatten our list
def generate_elements(elements):
    for element in elements:
        yield element
        
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
    sql = 'SELECT * FROM kaggle2_modeled.Life_Statistics limit 20'
    
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)
    
    #read desired table from BigQuery
    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    
    #write inputs to input.txt
    query_results | 'Write input' >> WriteToText('input_life.txt')
    
    # apply ParDo to transpose date columns
    transposed_dates_pcoll = query_results | 'Transpose Dates' >> beam.ParDo(TransposeDateFn())
    
    #flatten list to get individual records
    flatten_pcoll = transposed_dates_pcoll |'Flatten lists' >> beam.FlatMap(generate_elements)
    
    #write resulting PColleciton to output.txt
    flatten_pcoll | 'Write output' >> WriteToText('output_life_final.txt')
    
    #create new table in BigQuery
    dataset_id = 'kaggle2_modeled'
    table_id = 'Life_Statistics_Beam'
    schema_id = 'dt:DATE,countryName:STRING,countryCode:STRING, \
    metric:STRING,metricCode:STRING,statistic:FLOAT'

    # write PCollection to new BQ table
    flatten_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                  table=table_id, 
                                                  schema=schema_id,
                                                  project=PROJECT_ID,
                                                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                  batch_size=int(100))
     
    result = p.run()
    result.wait_until_finish()      

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)
    run()
    

  