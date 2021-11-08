import argparse
import logging
import os
import apache_beam as beam
from apache_beam.options import pipeline_options
from apache_beam.runners import DataflowRunner
from apache_beam.runners import DirectRunner
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.options.pipeline_options import SetupOptions

def Cursors(number_of_partition):
    return [('last',i,i+1) if i==number_of_partition-1 else ('others',i,i+1) for i in range(0,number_of_partition)]

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
        
       
        self.find_pattern_location=find_pattern_location
        
    def process(self, element, xml_tag_name, number_of_partition):
        file_obj=self.file_obj
        total_bytes=self.total_bytes
        find_pattern_location=self.find_pattern_location
        partition_chunk = total_bytes/number_of_partition
        pattern_start= b'<%b>' % xml_tag_name.encode('utf8')
        pattern_end=b'</%b>' % xml_tag_name.encode('utf8')
        partition_start = int(element[1]*partition_chunk)
        partition_stop = int(element[2]*partition_chunk)
        
        pattern = pattern_start
        file_move=1 
        if element[0]=='last':
            partition_stop = total_bytes-len(pattern_end)
            file_move=-1
            pattern = pattern_end
        
        cursor_start = find_pattern_location(file_obj, partition_start, pattern ,file_move)
        cursor_stop = find_pattern_location(file_obj, partition_stop, pattern,file_move)
        chunk = cursor_stop-cursor_start
        file_obj.seek(cursor_start)
        read=file_obj.read(chunk).decode('ascii')
        jsonChunkDumps=self.json.dumps(self.xmltodict.parse('<root>'+read+'</root>',attr_prefix=""))
        jsonChunkLoads=self.json.loads(jsonChunkDumps)
        for element in jsonChunkLoads['root'][xml_tag_name]:
            yield element

class ReadElement(beam.DoFn):
    def process(self, element):
        data = element
        yield data
            
def run(argv=None):
    
    #xml config
    gcsFileName="gs://pod-fr-retail-kaggle/xlarge.xml"
    xml_tag_name = 'Order'
    number_of_partition = None
    if number_of_partition==None:
        file_obj=FileSystems.open(gcsFileName,compression_type=CompressionTypes.UNCOMPRESSED)
        file_obj.seek(0,2)
        total_bytes=file_obj.tell()
        number_of_partition= int(total_bytes/1000000)
    
    #pipeline config
    project='pod-fr-retail'
    zone='europe-west1-c'
    region='europe-west1'
    worker_machine_type='n1-standard-1'
    num_workers=40
    #max_num_workers=20
    temp_location = 'gs://pod-fr-retail-kering/poc-dataflow/dataflow/temp_location'
    staging_location='gs://pod-fr-retail-kering/poc-dataflow/dataflow/staging'       
    # Setting up the Beam pipeline options.
    options = PipelineOptions()
    #options.view_as(SetupOptions).save_main_session = True
    options.view_as(GoogleCloudOptions).project = project
    options.view_as(GoogleCloudOptions).region = region
    options.view_as(GoogleCloudOptions).staging_location = staging_location
    options.view_as(GoogleCloudOptions).temp_location = temp_location
    options.view_as(WorkerOptions).num_workers = num_workers
    #options.view_as(WorkerOptions).worker_zone = zone
    #options.view_as(WorkerOptions).machine_type = worker_machine_type
    #options.view_as(WorkerOptions).max_num_workers = max_num_workers
    #options.view_as(pipeline_options.StandardOptions).streaming = False
    
    
  
    with beam.Pipeline(DataflowRunner(),options=options) as p:
        (p
            | "Create Cursors" >> beam.Create(Cursors(number_of_partition))
            | "Read XML" >> beam.ParDo(ReadXML(gcsFileName),xml_tag_name,number_of_partition)
            | "Reshuffle XML" >> beam.Reshuffle()
            | "Read Element" >> beam.ParDo(ReadElement()) 
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
