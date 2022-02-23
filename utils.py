from datetime import datetime
from dateutil import parser
import apache_beam as beam

def parse_lines(element):
    return element.split(",")

def filter_transactionAmount(element):
    return float(element[3]) > 20.0

def filter_timestamp(element):
    dt_format1 = "%Y-%m-%d %H:%M:%S %Z"
    dt_format2 = "%Y"
    return datetime.strptime(element[0], dt_format1) >= datetime.strptime('2010', dt_format2)

class format_timestamp(beam.DoFn):
    def process(self, element):
        str_format = "%Y-%m-%d"
        date = str(parser.parse(element[0]).strftime(str_format))
        amount = float(element[3])
        yield [date, amount]

@beam.ptransform_fn
def compute_output_fn(pcoll):
    return (
        pcoll
        | "ParseLines" >> beam.Map(parse_lines)
        | "Filter transaction_amount>20" >> beam.Filter(filter_transactionAmount)
        | "Filter timestamp<2010" >> beam.Filter(filter_timestamp)
        | "Format_timestamp" >> beam.ParDo(format_timestamp())
        | "Sum amount" >> beam.GroupBy(lambda x: x[0]).aggregate_field(lambda x: float(x[1]), sum, 'total_amount')
        | "FormatOutput" >> beam.Map(lambda element: ", ".join(map(str, element)))
        )