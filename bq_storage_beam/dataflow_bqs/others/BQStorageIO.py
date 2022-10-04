import apache_beam as beam
import logging

def create_bqs_session(project_id, dataset_id, table_id, max_stream_count:int, selected_fields, row_restriction):
    from google.cloud import bigquery_storage_v1
    client = bigquery_storage_v1.BigQueryReadClient()
    requested_session = bigquery_storage_v1.types.ReadSession()
    requested_session.table =  "projects/{}/datasets/{}/tables/{}".format(
          project_id, dataset_id, table_id
    )
    requested_session.data_format = bigquery_storage_v1.types.DataFormat.AVRO
    requested_session.read_options.selected_fields = selected_fields
    requested_session.read_options.row_restriction = row_restriction
    session = client.create_read_session(
        parent="projects/{}".format(project_id),
        read_session=requested_session,
        max_stream_count=max_stream_count,
    )
    stream_names=[]
    for stream in session.streams:
        stream_names.append(stream.name)
    return  stream_names

class ReadBQstorage(beam.DoFn):
    def setup(self):
        from google.cloud import bigquery_storage_v1
        self.clientbq = bigquery_storage_v1.BigQueryReadClient()
    def process(self, element):
        logging.info('Stream to read: %s', element)
        reader = self.clientbq.read_rows(element)
        rows = reader.rows()
        for row in rows:
            yield row

class ReadBQStorageIO(beam.PTransform):
  def __init__(self, project_id, dataset_id, table_id, max_stream_count, selected_fields, row_restriction):
    super().__init__()
    self._sessions = create_bqs_session(
        project_id=project_id, 
        dataset_id=dataset_id, 
        table_id=table_id, 
        max_stream_count=max_stream_count,
        selected_fields=selected_fields,
        row_restriction=row_restriction
    )

  def expand(self, pcoll):
    pcoll_bqs = (pcoll
    | "Create Sessions" >> beam.Create(self._sessions, reshuffle=True)
    | "Read BQStorage" >> beam.ParDo(ReadBQstorage())
    )
    return pcoll_bqs