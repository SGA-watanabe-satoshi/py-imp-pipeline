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

class BQColumnJsonStruct:
    def __init__(self, tableFieldSchema, jsonPath):
        self.__tableFieldSchema = tableFieldSchema
        self.__jsonPath = jsonPath

    def getFieldSchema(self):
        return self.__tableFieldSchema

    def getJsonPath(self):
        return self.__jsonPath

class BQColumnJsonMap():
    def __init__(self):
        self.__dict = {}

    def add(self, field_name, field_type, field_mode, jsonPath):
        field = bigquery.TableFieldSchema()
        field.name = field_name
        field.type = field_type
        field.mode = field_mode
        self.__dict[field_name] = BQColumnJsonStruct(field, jsonPath)

    def getSchema(self):
        schema = bigquery.TableSchema()
        field = bigquery.TableFieldSchema()
        for v in self.__dict.values():
            schema.fields.append(v.getFieldSchema())
        return schema

    def jsonToDict(self, line):
        dic = {}
        for k, v in self.__dict.items():
            dic[k] = self.__getValue(json.loads(jsonStr), v.getJsonPath())
        return dic

    def __getValue(self, target, path):
        value = target[path.pop(0)]
        if len(path) > 0 and value is not None:
            return self.getValue(value, path)
        return value

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

    placement_event = BQColumnJsonMap()
    placement_event.add('report_id','string','required',['id'])

    ( p | 'Read from PubSub' >> beam.io.ReadStringsFromPubSub(subscription=subscription) | beam.WindowInto(window.FixedWindows(20))
        | 'JSON to Dict' >> bema.Map(placement_event.jsonToDict),
        | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            known_args.output,
            schema=placement_event.getSchemea(),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )
    
    p.run()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run()