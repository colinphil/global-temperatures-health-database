import datetime, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class TransposeDateFn(beam.DoFn):
  def process(self, element):
    urban_record = element
    
    #get the date,city attribute
    countryName = urban_record.get('countryName')
    countryCode = urban_record.get('countryCode')
    metric = urban_record.get('metric')
    metricCode = urban_record.get('metricCode')
    
    #contains name of year columns
    yearList = []
    recordList = []
    
    #obtain columns to be split
    for i in range(1960, 2016):
        yearList.append("yr_"+str(i))
    
    #iterate through each column
    for year in yearList:
        
        #get related statistic
        statistic = urban_record.get(year)
        
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
    BUCKET = 'gs://global_surface_temperatures' # change to your bucket name
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'
    
     # Create and set your PipelineOptions.
    options = PipelineOptions(flags=None)

    # For Dataflow execution, set the project, job_name,
    # staging location, temp_location and specify DataflowRunner.
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = 'urban-growth-statistics-beam-dataflow'
    google_cloud_options.staging_location = BUCKET + '/staging'
    google_cloud_options.temp_location = BUCKET + '/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    
    # Create the Pipeline with the specified options.
    p = Pipeline(options=options)
  
    #create query to select all elements for cleansing
    sql = 'SELECT * FROM kaggle2_modeled.Urban_Growth_Statistics'
    
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)
    
    #read desired table from BigQuery
    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    
    #write inputs to input.txt
    query_results | 'Write input' >> WriteToText(DIR_PATH +'input_urban.txt')
    
    # apply ParDo to transpose date columns
    transposed_dates_pcoll = query_results | 'Transpose Dates' >> beam.ParDo(TransposeDateFn())
    
    #flatten list to get individual records
    flatten_pcoll = transposed_dates_pcoll |'Flatten lists' >> beam.FlatMap(generate_elements)
    
    #write resulting PColleciton to output.txt
    flatten_pcoll | 'Write output' >> WriteToText(DIR_PATH + 'output_urban_final.txt')
    
    #create new table in BigQuery
    dataset_id = 'kaggle2_modeled'
    table_id = 'Urban_Growth_Statistics_Beam_DF'
    schema_id = 'dt:DATE,countryName:STRING,countryCode:STRING, \
    metric:STRING,metricCode:STRING,statistic:FLOAT'

    # write PCollection to new BQ table
    flatten_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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
    

  