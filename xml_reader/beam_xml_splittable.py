import argparse
import logging
import sys
import os
import apache_beam as beam

from apache_beam.options import pipeline_options
from apache_beam.runners import DataflowRunner
from apache_beam.runners import DirectRunner
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.filesystem import CompressionTypes

from apache_beam.transforms.core import RestrictionProvider
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.restriction_trackers import OffsetRangeTracker
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.options.pipeline_options import SetupOptions

def find_pattern_location(file_obj, start_position, pattern, file_move):
            file_obj.seek(start_position)
            file_chunk_size=len(pattern)
            chunk_data = file_obj.read(file_chunk_size)
            if pattern in chunk_data:  # _id document signature
                pattern_position=start_position+ chunk_data.find(pattern)
                if file_move==-1:pattern_position=pattern_position+file_chunk_size
                return pattern_position
            else:
                start_position = start_position + (file_move)
                return find_pattern_location(file_obj, start_position, pattern, file_move)

class XMLRestrictionProvider(beam.transforms.core.RestrictionProvider):
        
    def initial_restriction(self, gcsFileName):
        from apache_beam.io.filesystems import FileSystems
        from apache_beam.io.filesystem import CompressionTypes
    
        file_obj=FileSystems.open(gcsFileName,compression_type=CompressionTypes.UNCOMPRESSED)
        file_obj.seek(0,2)
        total_bytes=file_obj.tell()
        pattern=b'</%b>' % 'Order'.encode('utf8')
        partition_stop=total_bytes-len(pattern)
        partition_start=0
        file_move=-1
        last_cursor=find_pattern_location(file_obj, partition_stop, pattern,file_move)
        pattern=b'<%b>' % 'Order'.encode('utf8')
        partition_start=0
        file_move=1
        first_cursor=find_pattern_location(file_obj, partition_start, pattern,file_move)
        print(str(first_cursor)+'--'+str(last_cursor))
        return OffsetRange(first_cursor, last_cursor)
    
    def split(self, gcsFileName, restriction):
        from apache_beam.io.filesystems import FileSystems
        from apache_beam.io.filesystem import CompressionTypes
        file_obj=FileSystems.open(gcsFileName,compression_type=CompressionTypes.UNCOMPRESSED)
        file_obj.seek(0,2)
        total_bytes=file_obj.tell()
        split_size = int(total_bytes/1000)
        pattern=b'<%b>' % 'Order'.encode('utf8')
        i = restriction.start
        #print(restriction.start)
        #print(restriction.stop)
        while i < restriction.stop-split_size:
            cursor_stop = find_pattern_location(file_obj, i+split_size, pattern, 1)
            yield OffsetRange(i, cursor_stop)
            #print(str(i) +'--'+ str(cursor_stop))
            i = cursor_stop
        yield OffsetRange(i, restriction.stop)
    
    def restriction_size(self, gcsFileName, restriction):
        return restriction.size()
    
    def create_tracker(self, restriction):
        return OffsetRestrictionTracker(restriction)


class ReadXML(beam.DoFn):
    
    def __init__(self, gcsFileName):
        self.gcsFileName = gcsFileName
        
    def setup(self):
        from apache_beam.io.filesystems import FileSystems
        from apache_beam.io.filesystem import CompressionTypes
        import xmltodict
        import json
        self.xmltodict=xmltodict
        self.json=json
        self.file_obj=FileSystems.open(self.gcsFileName,compression_type=CompressionTypes.UNCOMPRESSED)
        self.file_obj.seek(0,2)
        self.total_bytes=self.file_obj.tell()
        
    def process(self, element, xml_list_name
                , number_of_partition
                , tracker=beam.DoFn.RestrictionParam(XMLRestrictionProvider())):
       
        file_obj=self.file_obj
        cr=tracker.current_restriction()
        cursor_start,cursor_stop=cr.start,cr.stop
        print('parsing:'+ str(cursor_start)+'--'+str(cursor_stop))
        file_obj.seek(cursor_start)
        while tracker.try_claim(file_obj.tell()):
            read=file_obj.read(cursor_stop-cursor_start).decode('ascii')
            #logging.info('cursor:'+str(cursor_start)+'--'+str(cursor_stop))
            try:
                jsonChunkDumps=self.json.dumps(self.xmltodict.parse('<root>'+read+'</root>',attr_prefix=""))
                jsonChunkLoads=self.json.loads(jsonChunkDumps)    
                for element in jsonChunkLoads['root'][xml_list_name]:
                    #print(element)
                    yield element
            except Exception as e: 
                logging.info('cursor:'+str(cursor_start)+'--'+str(cursor_stop))
                print(e)

class ReadElement(beam.DoFn):
    def process(self, element):
        data = element
        yield data
            
def run(argv=None):
    
    #xml config
    gcsFileName="gs://xlarge.xml"
    xml_list_name = 'Order'
    number_of_partition = None
    if number_of_partition==None:
        file_obj=FileSystems.open(gcsFileName,compression_type=CompressionTypes.UNCOMPRESSED)
        file_obj.seek(0,2)
        total_bytes=file_obj.tell()
        number_of_partition= int(total_bytes/1000000)
    
    #pipeline config
    project='Your-Project-ID'
    zone='europe-west1-c'
    region='europe-west1'
    worker_machine_type='n1-standard-1'
    #num_workers=40
    max_num_workers=40
    temp_location = 'gs://poc-dataflow/dataflow/temp_location'
    staging_location='gs://poc-dataflow/dataflow/staging'       
    # Setting up the Beam pipeline options.
    options = PipelineOptions()
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(GoogleCloudOptions).project = project
    options.view_as(GoogleCloudOptions).region = region
    options.view_as(GoogleCloudOptions).staging_location = staging_location
    options.view_as(GoogleCloudOptions).temp_location = temp_location
    #options.view_as(WorkerOptions).num_workers = num_workers
    #options.view_as(WorkerOptions).worker_zone = zone
    #options.view_as(WorkerOptions).machine_type = worker_machine_type
    #options.view_as(WorkerOptions).max_num_workers = max_num_workers
    #options.view_as(pipeline_options.StandardOptions).streaming = False
    
    
  
    with beam.Pipeline(DataflowRunner(),options=options) as p:
        (p
            | "Read Text file" >> beam.Create([gcsFileName])
            | "Read XML" >> beam.ParDo(ReadXML(gcsFileName),xml_list_name,number_of_partition)
            | "Reshuffle XML" >> beam.Reshuffle()
            | "Read Element" >> beam.ParDo(ReadElement()) 
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
