from __future__ import absolute_import
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

input_filename = 'gs://raw_source_files/Customers/STG_CLIC_CUSTADDRORG_SAMPLE1.src'
output_table = 'automatic-asset-253215:STAGE.STG_CLIC_CUSTADDRORG'
table_schema ='FILE_SET_DATE:TIMESTAMP,TABLE_ID:INT64,UNIQUE_ROW_IDENTIFIER:INT64,INDUCTION_BATCH_NBR:INT64,FORMAT_NUMBER:INT64,BUSINESS_DATE:TIMESTAMP,ROW_STATUS_CODE:STRING,EDW_BATCH_NBR:INT64,ISSUE_LIST:STRING,ROW_ID:STRING, ROW_CREATED_DATE:STRING,ROW_UPDATED_DATE:STRING,HSN_ACCT_NUM:STRING,ACCOUNT_TYPE:STRING,ADDRESS_NAME:STRING,DISABLE_CLEANSING_FLAG:STRING,ORG_EXT_ROW_ID:STRING,ADDRESS_LINE_1:STRING,ADDRESS_LINE_2:STRING,ADDRESS_TYPE_CODE:STRING,CITY:STRING,COUNTRY:STRING,STATE:STRING,ZIP_CODE:STRING,FRAUD_BAD_ACCT_FLAG:STRING,SHIP_TO_FIRST_NAME:STRING,SHIP_TO_LAST_NAME:STRING,AGENT_VERIFIED_ADDRESS:STRING'

dataflow_options = ['--project=automatic-asset-253215','--job_name=ingest-to-stg-clic-custaddrorg']
dataflow_options.append('--temp_location=gs://raw_source_files/Customers/temp')
dataflow_options.append('--staging_location=gs://raw_source_files/Customers/temp/stg')
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)

options.view_as(StandardOptions).runner='dataflow'

class Split(beam.DoFn):

    def process(self, element):
        FILE_SET_DATE,TABLE_ID,UNIQUE_ROW_IDENTIFIER,INDUCTION_BATCH_NBR,FORMAT_NUMBER,BUSINESS_DATE,ROW_STATUS_CODE,EDW_BATCH_NBR,ISSUE_LIST,ROW_ID,ROW_CREATED_DATE,ROW_UPDATED_DATE,HSN_ACCT_NUM,ACCOUNT_TYPE,ADDRESS_NAME,DISABLE_CLEANSING_FLAG,ORG_EXT_ROW_ID,ADDRESS_LINE_1,ADDRESS_LINE_2,ADDRESS_TYPE_CODE,CITY,COUNTRY,STATE,ZIP_CODE,FRAUD_BAD_ACCT_FLAG,SHIP_TO_FIRST_NAME,SHIP_TO_LAST_NAME,AGENT_VERIFIED_ADDRESS = element.split("|")

        return [{
            'FILE_SET_DATE':FILE_SET_DATE,
            'TABLE_ID':TABLE_ID,
            'UNIQUE_ROW_IDENTIFIER':UNIQUE_ROW_IDENTIFIER,
            'INDUCTION_BATCH_NBR':INDUCTION_BATCH_NBR,
            'FORMAT_NUMBER':FORMAT_NUMBER,
            'BUSINESS_DATE':BUSINESS_DATE,
            'ROW_STATUS_CODE':ROW_STATUS_CODE,
            'EDW_BATCH_NBR':EDW_BATCH_NBR,
            'ISSUE_LIST':ISSUE_LIST,
            'ROW_ID':ROW_ID,
            'ROW_CREATED_DATE':ROW_CREATED_DATE,
            'ROW_UPDATED_DATE':ROW_UPDATED_DATE,
            'HSN_ACCT_NUM':HSN_ACCT_NUM,
            'ACCOUNT_TYPE':ACCOUNT_TYPE,
            'ADDRESS_NAME':ADDRESS_NAME,
            'DISABLE_CLEANSING_FLAG':DISABLE_CLEANSING_FLAG,
            'ORG_EXT_ROW_ID':ORG_EXT_ROW_ID,
            'ADDRESS_LINE_1':ADDRESS_LINE_1,
            'ADDRESS_LINE_2':ADDRESS_LINE_2,
            'ADDRESS_TYPE_CODE':ADDRESS_TYPE_CODE,
            'CITY':CITY,
            'COUNTRY':COUNTRY,
            'STATE':STATE,
            'ZIP_CODE':ZIP_CODE,
            'FRAUD_BAD_ACCT_FLAG':FRAUD_BAD_ACCT_FLAG,
            'SHIP_TO_FIRST_NAME':SHIP_TO_FIRST_NAME,
            'SHIP_TO_LAST_NAME':SHIP_TO_LAST_NAME,
            'AGENT_VERIFIED_ADDRESS':AGENT_VERIFIED_ADDRESS,          
            }]

def run():

    p = beam.Pipeline(options=options)
    (p
        |'Read from GCS' >> beam.io.ReadFromText(input_filename)
        |'ParseCSV' >> beam.ParDo(Split())
        |'Write to BigQuery' >> beam.io.WriteToBigQuery(
    output_table,
    schema=table_schema,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )
    p.run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
