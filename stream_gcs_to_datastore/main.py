import argparse
import logging
import hashlib
import sys
import os
import json
import uuid
import apache_beam as beam

from apache_beam.runners import DataflowRunner
from apache_beam.runners import DirectRunner
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.options.pipeline_options import SetupOptions

from apache_beam.io.gcp.datastore.v1new.datastoreio import DeleteFromDatastore
from apache_beam.io.gcp.datastore.v1new.datastoreio import ReadFromDatastore
from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore
from apache_beam.io.gcp.datastore.v1new.types import Entity
from apache_beam.io.gcp.datastore.v1new.types import Key
from apache_beam.io.gcp.datastore.v1new.types import Query


class GetFileNameFromPubSubMessage(beam.DoFn):
    def process(self, element):
        data=json.loads(element.data)
        fileName="gs://"+data['bucket']+"/"+data['name']
        logging.info('File to parse : %s', fileName)
        yield fileName

class ProcessLineFromFile(beam.DoFn):
    def setup(self):
        print("Setup: put here initialization code, such as librabries import")
    def process(self, element):
        data = element
        if "I thought the king had more affected" in element:
            logging.info('We found element: %s', element)
        yield data

class EntityWrapper(object):
    """
    Create a Cloud Datastore entity from the given string.
    Namespace and project are taken from the parent key.
    """
    def __init__(self, kind, parent_key):
        self._kind = kind
        self._parent_key = parent_key

    def make_entity(self, content):
        """Create entity from given string."""
        key = Key([self._kind, str(uuid.uuid4())],
                    parent=self._parent_key)
        entity = Entity(key)
        entity.set_properties({'content': str(content)})
        return entity

def run(argv=None): 
    # Setting up the Beam pipeline options.
    options = PipelineOptions()
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(GoogleCloudOptions).project = 'pod-fr-retail'
    options.view_as(GoogleCloudOptions).region = 'europe-west1'
    options.view_as(GoogleCloudOptions).staging_location = 'gs://pod-fr-retail/poc-dataflow/dataflow/staging' 
    options.view_as(GoogleCloudOptions).temp_location = 'gs://pod-fr-retail/poc-dataflow/dataflow/temp_location'
    #options.view_as(WorkerOptions).num_workers = num_workers
    #options.view_as(WorkerOptions).worker_zone = 'europe-west1-c'
    #options.view_as(WorkerOptions).machine_type = 'n1-standard-1'
    options.view_as(WorkerOptions).max_num_workers = 40
    options.view_as(pipeline_options.StandardOptions).streaming = True
    kind= "testdataflow"
    ancestor_key = Key([kind, str(uuid.uuid4())], project="pod-fr-retail")
    
    with beam.Pipeline(DataflowRunner(),options=options) as p:
        readlines= (p
            | "GCS Nofification in PubSub" >> beam.io.ReadFromPubSub(
                subscription="projects/pod-fr-retail/subscriptions/dataflow-subscription"
                , with_attributes=True
                , timestamp_attribute="eventTime")
            | "Extract fileName from PubSub message" >> beam.ParDo(GetFileNameFromPubSubMessage()) 
            | 'Read file and stream line by line' >> beam.io.ReadAllFromText()
        )
        
        first_branch= (readlines
        | "Read lines, find , log 1" >> beam.ParDo(ProcessLineFromFile()))
        
        second_branch= (readlines
        | "Read lines" >> beam.ParDo(ProcessLineFromFile())
        | "Create Entity" >> beam.Map(EntityWrapper(kind, ancestor_key).make_entity)
        #| "Write Entity to Datastore" >> WriteToDatastore('pod-fr-retail',throttle_rampup=True, hint_num_workers=3)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
