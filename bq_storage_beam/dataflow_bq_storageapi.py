import apache_beam as beam
from apache_beam.runners import DataflowRunner
from apache_beam.runners import DirectRunner
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import DebugOptions
import google.auth
import argparse
import logging


def create_sessions():
    from google.cloud import bigquery_storage_v1
    project_id='pod-fr-retail'
    client = bigquery_storage_v1.BigQueryReadClient()
    table = "projects/{}/datasets/{}/tables/{}".format(
          "pod-fr-retail", "demo", "virtualshop_orders"
    )
    requested_session = bigquery_storage_v1.types.ReadSession()
    requested_session.table = table
    requested_session.data_format = bigquery_storage_v1.types.DataFormat.AVRO
    requested_session.read_options.selected_fields.append("orderId")
    #requested_session.read_options.c = 'state = "WA"'
    modifiers = None
    parent = "projects/{}".format(project_id)
    session = client.create_read_session(
        parent=parent,
        read_session=requested_session,
        max_stream_count=30,
    )
    return session

def create_sessions_list():
    from google.cloud import bigquery_storage_v1
    project_id='pod-fr-retail'
    client = bigquery_storage_v1.BigQueryReadClient()
    table = "projects/{}/datasets/{}/tables/{}".format(
          "pod-fr-retail", "demo", "virtualshop_orders"
    )
    requested_session = bigquery_storage_v1.types.ReadSession()
    requested_session.table = table
    requested_session.data_format = bigquery_storage_v1.types.DataFormat.AVRO
    requested_session.read_options.selected_fields.append("orderId")
    #requested_session.read_options.row_restriction = 'state = "WA"'
    modifiers = None
    parent = "projects/{}".format(project_id)
    session = client.create_read_session(
        parent=parent,
        read_session=requested_session,
        max_stream_count=30,
    )

    readers=[]
    #print(session)
    for stream in session.streams:
        readers.append({'session':session,"streamName":stream.name})
    #print(readers)
    return readers

class bqstorage(beam.DoFn):
    def setup(self):
        from google.cloud import bigquery_storage_v1
        self.clientbq = bigquery_storage_v1.BigQueryReadClient()
    def process(self, element):
        logging.info('File to parse : %s', element)
        reader = self.clientbq.read_rows(element['streamName'])
        rows = reader.rows(element['session'])
        for row in rows:
            #print(row)
            yield row

class ProcessLineFromFile(beam.DoFn):
    def setup(self):
        print("Setup: put here initialization code, such as librabries import")
    def process(self, element):
        data = element
        print(element["orderId"])
        if "I thought the king had more affected" in element:
            logging.info('We found element: %s', element)
        yield data

def run(argv=None):
    
    project='pod-fr-retail'
    zone='europe-west1-c'
    region='europe-west1'
    worker_machine_type='n1-standard-1' #g1-small
    #num_workers=3
    max_num_workers=10
    temp_location = 'gs://pod-fr-retail/bqdataflow/temp'
    staging_location='gs://pod-fr-retail/bqdataflow/staging'

    # Setting up the Beam pipeline options.
    options = pipeline_options.PipelineOptions()
    #options.view_as(pipeline_options.StandardOptions).streaming = False
    _,options.view_as(GoogleCloudOptions).project = google.auth.default()
    options.view_as(GoogleCloudOptions).region = region
    options.view_as(GoogleCloudOptions).staging_location = staging_location
    options.view_as(GoogleCloudOptions).temp_location = temp_location
    options.view_as(WorkerOptions).worker_zone = zone
    #options.view_as(WorkerOptions).machine_type = worker_machine_type
    #options.view_as(WorkerOptions).num_workers = num_workers
    options.view_as(WorkerOptions).max_num_workers = max_num_workers
    #options.view_as(WorkerOptions).autoscaling_algorithm='NONE'
    options.view_as(SetupOptions).requirements_file='requirements.txt'
    #options.view_as(SetupOptions).save_main_session = True
    #options.view_as(DebugOptions).experiments = ['shuffle_mode=service,use_runner_v2']
    #options.view_as(SetupOptions).requirements_cache='/root/notebook/workspace/20200617'

    with beam.Pipeline(DirectRunner(),options=options) as p:
        sessions = create_sessions()
        (p
         | "Create " >> beam.Create(create_sessions)
         | "Map" >> beam.Map(lambda element: (element)
         | "Read orderId" >> beam.ParDo(bqstorage())
         | "Process orderId" >> beam.ParDo(ProcessLineFromFile())

         #| "Add or update Local inventory" >>  beam.ParDo(retailAddLocalInventory())
        )
        
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()