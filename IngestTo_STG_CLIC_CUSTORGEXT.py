from __future__ import absolute_import
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

input_filename = 'gs://raw_source_files/Customers/VU_STG_CLIC_CUSTSORGEXT_SAMPLE1.src'
output_table = 'automatic-asset-253215:STAGE.STG_CLIC_CUSTSORGEXT'
table_schema ='FILE_SET_DATE:TIMESTAMP,TABLE_ID:INT64,UNIQUE_ROW_IDENTIFIER:INT64,INDUCTION_BATCH_NBR:INT64,FORMAT_NUMBER:INT64,BUSINESS_DATE:TIMESTAMP,ROW_STATUS_CODE:STRING,EDW_BATCH_NBR:INT64,ISSUE_LIST:STRING,ROW_ID:STRING, ROW_CREATED_DATE:STRING, ROW_UPDATED_DATE:STRING, HSN_ACCT_NUM:STRING, PRIMARY_PHONE_NUM:STRING, ACCOUNT_TYPE:STRING, BILLING_ADDRESS:STRING, BILLING_CONTACT:STRING, SHIPPING_ADDRESS:STRING, SHIPPING_CONTACT:STRING, BILL_SHIP_ADDR_SYNC_FLAG:STRING, PRIMARY_PHONE_TYPE:STRING, ALT_PHONE1_NUMBER:STRING, ALT_PHONE1_TYPE:STRING, ALT_PHONE2_NUMBER:STRING, ALT_PHONE2_TYPE:STRING, GUEST_CUSTOMER_FLAG:STRING, GUID:STRING, MARKET_PLACE_ID:STRING, MARKET_PLACE_CUSTID:STRING'

dataflow_options = ['--project=automatic-asset-253215','--job_name=ingest-to-stg-clic-custorgext']
dataflow_options.append('--temp_location=gs://raw_source_files/Customers/temp')
dataflow_options.append('--staging_location=gs://raw_source_files/Customers/temp/stg')
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)

options.view_as(StandardOptions).runner='dataflow'

class Split(beam.DoFn):

    def process(self, element):
        FILE_SET_DATE,TABLE_ID,UNIQUE_ROW_IDENTIFIER,INDUCTION_BATCH_NBR,FORMAT_NUMBER,BUSINESS_DATE,ROW_STATUS_CODE,EDW_BATCH_NBR,ISSUE_LIST,ROW_ID, ROW_CREATED_DATE, ROW_UPDATED_DATE, HSN_ACCT_NUM, PRIMARY_PHONE_NUM, ACCOUNT_TYPE, BILLING_ADDRESS, BILLING_CONTACT, SHIPPING_ADDRESS, SHIPPING_CONTACT, BILL_SHIP_ADDR_SYNC_FLAG, PRIMARY_PHONE_TYPE, ALT_PHONE1_NUMBER, ALT_PHONE1_TYPE, ALT_PHONE2_NUMBER, ALT_PHONE2_TYPE, GUEST_CUSTOMER_FLAG, GUID, MARKET_PLACE_ID, MARKET_PLACE_CUSTID = element.split("|")

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
            'PRIMARY_PHONE_NUM':PRIMARY_PHONE_NUM,
            'ACCOUNT_TYPE':ACCOUNT_TYPE, 
            'BILLING_ADDRESS':BILLING_ADDRESS, 
            'BILLING_CONTACT':BILLING_CONTACT, 
            'SHIPPING_ADDRESS':SHIPPING_ADDRESS, 
            'SHIPPING_ADDRESS':SHIPPING_CONTACT, 
            'BILL_SHIP_ADDR_SYNC_FLAG':BILL_SHIP_ADDR_SYNC_FLAG, 
            'PRIMARY_PHONE_TYPE':PRIMARY_PHONE_TYPE, 
            'ALT_PHONE1_NUMBER':ALT_PHONE1_NUMBER, 
            'ALT_PHONE1_TYPE':ALT_PHONE1_TYPE, 
            'ALT_PHONE2_NUMBER':ALT_PHONE2_NUMBER, 
            'ALT_PHONE2_TYPE':ALT_PHONE2_TYPE, 
            'GUEST_CUSTOMER_FLAG':GUEST_CUSTOMER_FLAG, 
            'GUID':GUID, 
            'MARKET_PLACE_ID':MARKET_PLACE_ID, 
            'MARKET_PLACE_CUSTID':MARKET_PLACE_CUSTID,
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
