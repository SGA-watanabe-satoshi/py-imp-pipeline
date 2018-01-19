#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""A update classification datastore workflow."""

from __future__ import absolute_import

import argparse
import logging
import json
import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.gcp.internal.clients import bigquery

logger = logging.getLogger(__name__)

def getSchemea():
    schema = bigquery.TableSchema()

    field = bigquery.TableFieldSchema()
    field.name = 'report_id'
    field.type = 'string'
    field.mode = 'required'
    schema.fields.append(field)

    return schema

def parse_pubsub(line):
    record = json.load(line)
    return record

def run(argv=None):

    parser = argparse.ArgumentParser()
    parser.add_argument('--subscription',
                        dest='subscription',
                        required=True,
                        help='Pub/Sub subscription to read from')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    subscription = known_args.subscription

    logger.debug(pipeline_options)

    p = beam.Pipeline(options=pipeline_options)

    ( p | 'Read from PubSub' >> beam.io.ReadStringsFromPubSub(subscription=subscription) | beam.WindowInto(window.FixedWindows(20))
        | 'JSON to Dict' >> bema.Map(parse_pubsub),
        | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            known_args.output,
            schema=getSchemea(),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )

    p.run()
    

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run()