import argparse
import datetime
import json
import logging
import sys
import io

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import pvalue
from google.cloud import bigquery
from apache_beam.pvalue import TaggedOutput


class JobOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--project_id",
            type=str,
            help="project ID of GCP project",
            default=None
        )
        parser.add_argument(
            "--input_subscription",
            type=str,
            help="The Cloud Pub/Sub topic to read from.\n"
            '"projects/<PROJECT_NAME>/subscriptions/<SUBSCRIPTION_NAME>".',
        )
        parser.add_argument(
            "--window_size",
            type=float,
            help="File's window size in number of half minutes.",
            default=1
        )
        parser.add_argument(
            "--bigquery_dataset",
            type=str,
            help="Bigquery Dataset to write raw casino data",
        )
        parser.add_argument(
            "--bigquery_tables",
            nargs="+",
            help="Bigquery Tables [transactions, users] to write raw casino data",
        )


class SplitTables(beam.DoFn):
    """Spliting nested JSON into 2 separate jsons:
    users and their transactions, based on uid
    """
    def process(self, element, *args, **kwargs):
        element = json.loads(element.decode('utf-8'))
        users_data = element.pop('user_info')
        users_data['uid'] = element['uid']
        transactions_data = element
        yield users_data
        yield transactions_data


class GroupWindowsIntoBatches(beam.PTransform):
    """A composite transform that groups Pub/Sub messages based on publish
    time and outputs a list of dictionaries.
    """

    def __init__(self, window_size):
        # Convert minutes into seconds.
        self.window_size = int(window_size * 30)

    def expand(self, pcoll):
        return (
            pcoll
            # Assigns window info to each Pub/Sub message.
            | "Window into Fixed Intervals" >> beam.WindowInto(window.FixedWindows(self.window_size))
            | "Add Dummy Key" >> beam.Map(lambda elem: (None, elem))
            | "Groupby" >> beam.GroupByKey()
            | "Abandon Dummy Key" >> beam.MapTuple(lambda _, val: val)
        )

class SplitBatch(beam.DoFn):
    """Split entire batch to ones based on a keyword
    requuired before writing it to separate bigquery tables
    """
    def __init__(self, keyword='user_name'):
        self.keyword = keyword

    def process(self, tables):
        users_table = [i for i in tables if self.keyword in i]
        transactions_table = [i for i in tables if self.keyword not in i]
        return TaggedOutput('users', users_table), TaggedOutput('transactions', transactions_table)

class WriteToBigQuery(beam.DoFn):
    """Detects the schema automatically, creates table if needed
    appends to existing table.
    Writing batches according to choosed bg_table
    """
    def __init__(self, bq_dataset, bq_tables, project_id):
        self.bq_dataset = bq_dataset
        self.bq_tables = bq_tables
        self.project_id = project_id

    def start_bundle(self):
        self.client = bigquery.Client()

    def process(self, table):
        print(table)
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
            ],
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        table_id = f"{self.bq_dataset}.{self.bq_tables}"
        print(table_id)
        try:
            load_job = self.client.load_table_from_json(
                table,
                table_id,
                job_config=job_config
                )  # Make an API request.
            load_job.result()  # Waits for the job to complete.

        except Exception as error:
            logging.info(f'Error: {error} with loading data to Bigquery')

def run(argv):
    parser = argparse.ArgumentParser()
    pipeline_args = parser.parse_known_args(argv)[1]
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True, streaming=True)
    options = pipeline_options.view_as(JobOptions)
    with beam.Pipeline(options=pipeline_options) as pipeline:
        transformed = (
            pipeline
            | "Read PubSub Messages" >> beam.io.ReadFromPubSub(subscription=options.input_subscription)
            | 'Transform into 2 separate tables' >> beam.ParDo(SplitTables())
            | f"Window into: {options.window_size}m" >> GroupWindowsIntoBatches(options.window_size)
            | 'SplitBatch into 2 separate batches' >> beam.ParDo(SplitBatch(keyword='user_name')).with_outputs()
        )
        transformed.transactions | "Write first Raw Data to Big Query" >> beam.ParDo(WriteToBigQuery(project_id=options.project_id, bq_dataset=options.bigquery_dataset, bq_tables=options.bigquery_tables[0]))
        transformed.users | "Write second Raw Data to Big Query" >> beam.ParDo(WriteToBigQuery(project_id=options.project_id, bq_dataset=options.bigquery_dataset, bq_tables=options.bigquery_tables[1]))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run(sys.argv)
