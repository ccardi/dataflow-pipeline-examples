import apache_beam as beam
from apache_beam.runners import DataflowRunner
from apache_beam.runners import DirectRunner
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import DebugOptions
import argparse
import logging
from others.BQStorageIO import ReadBQStorageIO
from others.functions import ProcessEachOrder


def run(argv=None):
    # Setting up the Beam pipeline options.
    options = pipeline_options.PipelineOptions()
    #options.view_as(GoogleCloudOptions).job_name = 'read-bq-test'
    options.view_as(GoogleCloudOptions).project = 'pod-fr-retail'
    options.view_as(GoogleCloudOptions).region = 'europe-west1'
    options.view_as(GoogleCloudOptions).staging_location = 'gs://pod-fr-retail/bqdataflow/staging'
    options.view_as(GoogleCloudOptions).temp_location = 'gs://pod-fr-retail/bqdataflow/temp'
    #options.view_as(WorkerOptions).max_num_workers = 30
    #options.view_as(DebugOptions).experiments = ["no_use_multiple_sdk_containers"]
    with beam.Pipeline(DataflowRunner(),options=options) as p:
        (p
         | "ReadBQStorageIO Test " >> ReadBQStorageIO(
             project_id='pod-fr-retail',
             dataset_id='demo',
             table_id='virtualshop_orders',
             max_stream_count=300,
             selected_fields = ["orderId"],
             row_restriction='' #'state = "WA"'
             )
         | "Process orderId" >> beam.ParDo(ProcessEachOrder())
        )
        
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()