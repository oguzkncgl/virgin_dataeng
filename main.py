import argparse
import logging
import apache_beam as beam
from utils import compute_output_fn

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input")
    parser.add_argument("--output")
    args, beam_args = parser.parse_known_args(argv)

    with beam.Pipeline(argv=beam_args) as p:
        lines = (
            p
            | "ReadCsvFile" >> beam.io.ReadFromText(args.input, skip_header_lines=1)
            | "formatted_date" >> compute_output_fn()
            | "WriteOutput" >> beam.io.WriteToText("./outputs/result", file_name_suffix=".csv", header='date, total_amount')
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
