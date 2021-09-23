''' 
  pubsub.py is an Apache Beam streaming pipeline for reading from a Google PubSub streaming 
  datasource and writing to a text file.

  It was implemented for a specific purpose so it cannot be directly used, but rather acts 
  as an example of how an Apache Beam streaming pipeline can work to read from PubSub and write
  to a file.


  On top of the bare minimum, it does the following transformations:

  1) Parse JSON into Python dicts
  2) Filters contents based on a dictionary key value pair
  3) Outputs a string so that it can be stored into a text file
  4) Windows the stream into 30 second increments / batches 

  The beef of the file is this pipeline:

      lines = p | beam.io.ReadFromPubSub(topic=topic).with_output_types(bytes) \
      | 'Decode bytes' >> beam.Map(lambda x: x.decode('utf-8')) \
      | 'Parse JSON' >> beam.Map(lambda x: json.loads(x)) \
      | 'Filter only Pakastin' >> beam.Filter(is_pakastin) \
      | 'Parse payload and stringify' >> beam.ParDo(ParsePubSubPayload()) \
      | 'Windowing in 30 sec increments' >> beam.WindowInto(window.FixedWindows(30)) \
      | 'FileIO Write to Files' >> fileio.WriteToFiles(path='gs://fridge-alerts-1337-3/beam/pubsub2.txt') \

  The module implements a few custom transformations / filters:

  Filters: is_pakastin - custom logic for a certain project
  Transformation: ParsePubSubPayload() - projects a subset of dictionary contents into a string
  Aux functions: LogPubSubContents() - an utility class / processing step to log elements for observability

  Requirements to run:

  1) a Google Cloud Platform (GCP) project
  2) a Google Cloud Storage (GCS) bucket to write to
  3) a GCP PubSub topic to read from

  All of these in the same region to avoid errors.

  GCP project: GCP_PROJECT_NAME
  GCP / GCS region: GCS_REGION
  GCP Google Cloud Storage location for temp writing: GCS_TEMP_LOC
  GCP PubSub topic to read from: GCS_PUBSUB_READ_TOPIC
  GCP GCS bucket location to write the text file to: TARGET_TEXT_FILE_GCS_LOCATION

'''

# TODO: read these values from an .env file

GCP_PROJECT_NAME = 'changeme'
GCS_TEMP_LOC = 'changeme'
GCS_REGION = 'changeme'
GCS_PUBSUB_READ_TOPIC = 'changeme'
TARGET_TEXT_FILE_GCS_LOCATION = 'changeme'

import argparse
import logging
import re
import json

import apache_beam as beam
from apache_beam import window
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# from apache_beam import window
from apache_beam.io.gcp.internal.clients import bigquery

class LogPubSubContents(beam.DoFn):
  def process(self, element):
        logging.info('Element: %s', element)
        logging.info('Element: %s', type(element))
        return [element]

class ParsePubSubPayload(beam.DoFn):
  def process(self, element):
    return ["{}, {}".format(element['ruuvi_tag_name'], element['measurement_value'])]

def run():

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  beam_options = PipelineOptions(
    project=GCP_PROJECT_NAME, 
    streaming=True,
    temp_location=GCS_TEMP_LOC, 
    region=GCS_REGION)

  with beam.Pipeline(options=beam_options) as p:    
    topic = 

    # INFO:root:Element: 
    # b'{
    #  "ruuvi_tag_id":"e84bf51b0ceb",
    # "ruuvi_tag_name":"Pakastin",
    # "measurement_type":"rssi",
    # "measurement_value":-68,
    # "tags":[{"key":"tag_name","value":"Pakastin"},{"key":"data_format","value":3}],
    # "ts":"2021-08-12T18:41:03.063Z"
    # }'

    def is_pakastin(element):
      return [element['ruuvi_tag_name'] == 'Pakastin']

    lines = p | beam.io.ReadFromPubSub(topic=topic).with_output_types(bytes) \
      | 'Decode bytes' >> beam.Map(lambda x: x.decode('utf-8')) \
      | 'Parse JSON' >> beam.Map(lambda x: json.loads(x)) \
      | 'Filter only Pakastin' >> beam.Filter(is_pakastin) \
      | 'Parse payload and stringify' >> beam.ParDo(ParsePubSubPayload()) \
      | 'Windowing in 30 sec increments' >> beam.WindowInto(window.FixedWindows(30)) \
      | 'FileIO Write to Files' >> fileio.WriteToFiles(path=TARGET_TEXT_FILE_GCS_LOCATION) \

    # | 'Log contents again' >> beam.ParDo(LogPubSubContents()) \

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()