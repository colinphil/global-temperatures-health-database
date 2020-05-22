import datetime, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class FilterDateFn(beam.DoFn):
  def process(self, element):
    #import within function for dataflow
    import datetime
    import pandas as pd
    city_record = element
    
    #establish earliest acceptable date to compare to
    #obtained from earliest date from Date entity table
    cutoffDate = datetime.date(1755,9,1)
    
    #get the date,city attribute
    dt = city_record.get('dt')
    city = city_record.get('City')
    print(city)
    
    #create date object from dt to compare to cutoff
    formatted_dt = pd.to_datetime(dt)
    
    #add record if after cutoffDate
    if formatted_dt > cutoffDate:
        key = (dt, city)
        print(key)
        city_tuple = (key,city_record)
        return [city_tuple]
    
    #return empty list if before cutoff date
    return []

class DedupCityRecordsFn(beam.DoFn):
    def process(self, element):
        
        #create object to sort by
        key, city_obj = element 
        
        #cast object as list to extract first record
        city_list = list(city_obj) 
        city_record = city_list[0] 
        return [city_record]  
           
def run():
    PROJECT_ID = 'electric-spark-266716' # change to your project id
    BUCKET = 'gs://global_surface_temperatures' # change to your bucket name
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'
    
     # Create and set your PipelineOptions.
    options = PipelineOptions(flags=None)

    # For Dataflow execution, set the project, job_name,
    # staging location, temp_location and specify DataflowRunner.
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = 'city-beam-dataflow'
    google_cloud_options.staging_location = BUCKET + '/staging'
    google_cloud_options.temp_location = BUCKET + '/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    
    # Create the Pipeline with the specified options.
    p = Pipeline(options=options)
  
    #create query to select all elements for cleansing
    sql = 'SELECT dt, AverageTemperature, AverageTemperatureUncertainty, City, Country, Latitude,\
     Longitude, major_city FROM kaggle_modeled.City as x'
    
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)
    
    #read desired table from BigQuery
    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    
    #write inputs to input.txt
    query_results | 'Write input' >> WriteToText(DIR_PATH +'input.txt')
    
    # apply ParDo to filter out dates 
    formatted_date_pcoll = query_results | 'Filter Dates' >> beam.ParDo(FilterDateFn())
    
    #write filtered dates to filtered.txt
    formatted_date_pcoll | 'Write filtered dates' >> WriteToText(DIR_PATH + 'filtered.txt')

    # group city records by (dt,city) tuple created
    grouped_city_pcoll = formatted_date_pcoll | 'Group by city, dt' >> beam.GroupByKey()
    
    # display grouped city records
    grouped_city_pcoll | 'Write group by' >> WriteToText(DIR_PATH +'grouped.txt')
    
    #remove duplicate city records
    distinct_city_pcoll = grouped_city_pcoll | 'Delete duplicate records' >> beam.ParDo(DedupCityRecordsFn())
    
    #write resulting PColleciton to output.txt
    distinct_city_pcoll | 'Write output' >> WriteToText(DIR_PATH + 'output.txt')
    
    #create new table in BigQuery
    dataset_id = 'kaggle_modeled'
    table_id = 'City_Beam_DF'
    schema_id = 'dt:DATE,AverageTemperature:FLOAT,AverageTemperatureUncertainty:FLOAT,\
    City:STRING,Country:STRING,Latitude:STRING,Longitude:STRING,major_city:INTEGER'

    # write PCollection to new BQ table
    distinct_city_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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
    

  