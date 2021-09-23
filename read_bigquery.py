''' 
  read_bigquery.py is an Apache Beam batch pipeline for reading from a Google BigQuery batch 
  datasource and writing to a local text file.

  Requirements to run:

  1) a Google Cloud Platform (GCP) project
  2) a Google Cloud Storage (GCS) bucket to write to
  3) a BigQuery dataset and table to read from

  All of these in the same region to avoid errors.

  GCP project: GCP_PROJECT_NAME
  GCP / GCS region: GCS_REGION
  GCP Google Cloud Storage location for temp writing: GCS_TEMP_LOC
  BigQuery project, dataset and table:
    BQ_PROJECT_ID
    BQ_DATASET_ID
    BQ_TABLE_ID

  FROM clause in the SQL query: BQ_FROM
  Current date for the WHERE clause: CURR_DATE 
  
'''

# TODO: read these values from an .env file

GCP_PROJECT_NAME = 'changeme'
GCS_TEMP_LOC = 'changeme'
GCS_REGION = 'changeme'
BQ_PROJECT_ID = 'changeme'
BQ_DATASET_ID = 'changeme'
BQ_TABLE_ID = 'changeme'

CURR_DATE = '2021-08-09'
BQ_FROM = 'changeme'

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# from apache_beam import window
from apache_beam.io.gcp.internal.clients import bigquery


# Format the counts into a PCollection of strings.
def format_result(word, count):
    return '%s: %d' % (word, count)

def run():

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  beam_options = PipelineOptions(
    project=GCP_PROJECT_NAME, 
    temp_location=GCS_TEMP_LOC, 
    region=GCS_REGION)
  # pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=beam_options) as p:
    curr_date = CURR_DATE
    table_spec = bigquery.TableReference(
      projectId=BQ_PROJECT_ID,
      datasetId=BQ_DATASET_ID,
      tableId=BQ_TABLE_ID)
    
    sql_query = '''SELECT ts, measurement_value 
              FROM `{0}`
              WHERE ruuvi_tag_name = "Pakastin" AND measurement_type = "temperature" AND 
              DATE(_PARTITIONTIME) = "{1}" 
              ORDER BY ts ASC'''.format(BQ_FROM, curr_date)
    
    p | 'Query BQ table' >> beam.io.ReadFromBigQuery(query=sql_query, use_standard_sql=True) \
      | 'Transform rows to tuples with minutes datetime format' >> beam.FlatMap(lambda row: [ ( row['ts'].strftime('%Y-%m-%d %H:%M') , row['measurement_value'] )]) \
      | 'Group counts per minute' >> beam.GroupByKey() \
      | 'Average per minute' >> beam.FlatMap(lambda row: [ ( row[0] , sum(row[1])/len(row[1]) )]) \
      | 'Write' >> WriteToText('count3.txt') 

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()