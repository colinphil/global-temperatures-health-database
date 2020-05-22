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
        import datetime
        import pandas as pd
        state_record = element

        #establish earliest acceptable date to compare to
        #obtained from earliest date from Date entity table
        cutoffDate = datetime.date(1755,9,1)

        #get the date attribute
        dt = state_record.get('dt')

        #create date object from dt to compare to cutoff
        formatted_dt = pd.to_datetime(dt)

        #add record if after cutoffDate
        if formatted_dt > cutoffDate:
            return [state_record]

        #return empty list if before cutoff date
        return []

           
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
    google_cloud_options.job_name = 'state-beam-dataflow'
    google_cloud_options.staging_location = BUCKET + '/staging'
    google_cloud_options.temp_location = BUCKET + '/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    
    # Create the Pipeline with the specified options.
    p = Pipeline(options=options)
  
    #create query to select all elements for cleansing
    sql = 'SELECT dt, AverageTemperature, AverageTemperatureUncertainty,State,Country \
    FROM kaggle_modeled.State as x'
    
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)
    
    #read desired table from BigQuery
    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    
    #write inputs to input.txt
    query_results | 'Write input' >> WriteToText(DIR_PATH +'input_state.txt')
    
    # apply ParDo to filter out dates 
    formatted_country_pcoll = query_results | 'Filter Dates' >> beam.ParDo(FilterDateFn())
    
    # display filtered countries
    formatted_country_pcoll | 'Write filtered dates' >> WriteToText(DIR_PATH + 'output_state.txt')
    
    #create new table in BigQuery
    dataset_id = 'kaggle_modeled'
    table_id = 'State_Beam_DF'
    schema_id = 'dt:DATE,AverageTemperature:FLOAT,AverageTemperatureUncertainty:FLOAT,State:STRING,Country:STRING'

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
    

  