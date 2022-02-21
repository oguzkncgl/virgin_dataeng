import argparse
import requests
import logging
import csv
import pandas as pd
from datetime import datetime

import apache_beam as beam
from apache_beam.dataframe.io import read_csv

from utils import format_schema_to_dict, output_to_file

def filter_transactionAmount(transaction):
    return float(transaction['transaction_amount']) > 20.0

def filter_timestamp(transaction):
    return datetime.strptime(transaction['timestamp'], '%Y-%m-%d %H:%M:%S %Z') >= datetime.strptime('2010', '%Y')


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input")
    parser.add_argument("--output")
    args, beam_args = parser.parse_known_args(argv)

    with beam.Pipeline(argv=beam_args) as p:
        transactions = (
            p
            | 'ReadCsvFile' >> beam.io.ReadFromText(args.input, skip_header_lines=1)
            | "ParseFile" >> beam.Map(lambda x: format_schema_to_dict(x))
            | "Filter transaction_amount>20.0" >> beam.Filter(filter_transactionAmount)
            | "Filter timestamp<2010" >> beam.Filter(filter_timestamp)
            | "Sum value" >> beam.GroupBy(lambda x: x['timestamp']).aggregate_field(lambda x: float(x['transaction_amount']), sum, 'total_quantity')
            | "WriteOutput" >> beam.io.WriteToText("./outputs/result", file_name_suffix=".csv")
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)
    run()
