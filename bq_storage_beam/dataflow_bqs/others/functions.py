import apache_beam as beam
import logging

class ProcessEachOrder(beam.DoFn):
    def setup(self):
        print("Setup: put here initialization code, such as librabries import")
    def process(self, element):
        data = element
        #print(element["orderId"])
        #if "I thought the king had more affected" in element:
        #    logging.info('We found element: %s', element)
        yield data