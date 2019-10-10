from __future__ import absolute_import
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

input_filename = 'gs://raw_source_files/Customers/VU_STG_CLIC_CUSTSCONTACT_SAMPLE1.src'
output_table = 'automatic-asset-253215:STAGE.STG_CLIC_CUSTSCONTACT'
table_schema ='FILE_SET_DATE:TIMESTAMP,TABLE_ID:INT64,UNIQUE_ROW_IDENTIFIER:INT64,INDUCTION_BATCH_NBR:INT64,FORMAT_NUMBER:INT64,BUSINESS_DATE:TIMESTAMP,ROW_STATUS_CODE:STRING,EDW_BATCH_NBR:INT64,ISSUE_LIST:STRING,ROW_ID:STRING, ROW_CREATED_DATE:STRING, ROW_UPDATED_DATE:STRING, HSN_ACCT_NUM:STRING, ACCOUNT_TYPE:STRING, FIRST_NAME:STRING, LAST_NAME:STRING, SUPRESS_EMAIL_FLAG:STRING, SEND_NEWS_FLAG:STRING, SUPRESS_CALL_FLAG:STRING, SUPRESS_MAIL_FLAG:STRING, EMAIL_ADDRESS:STRING, HSN_EMP_ID:STRING, ORG_EXT_ROW_ID:STRING, CREDIT_CARD_ROW_ID:STRING, SEX_M_F:STRING, ABUSIVE_RETURNS_COUNT:STRING, ABUSIVE_RETURNS_FLAG:STRING, DIRECT_DEBIT_FLAG:STRING, CREDIT_CARD_ONLY:STRING, CUSTOMER_SINCE_DATE:STRING, FLEX_PAY_LIMIT_FLAG:STRING, LAST_ORDER_DATE:STRING, LAST_PAYMENT_TYPE:STRING, LAST_REFUND_DATE:STRING, LAST_RETURN_DATE:STRING, MEDIA_TYPE:STRING, NAT_TAX_EXEMPT_FLAG:STRING, NO_FLEXPAY_FLAG:STRING, NO_ON_AIR_FLAG:STRING, NO_REFUND_FLAG:STRING, NO_SHIP_ADDR_FLAG:STRING, PREFER_SHIP_METHOD:STRING, ST_TAX_EXEMPT_FLAG:STRING, TOTAL_ORDERS:STRING, TOTAL_REFUND_ITEMS:STRING, TOTAL_PURCHASE_AMT:STRING, TOTAL_REFUND_AMT:STRING, YTD_KASH_WON_AMT:STRING, TOTAL_EXCHANGE_COUNT:STRING, CALL_TAG_COUNT:STRING, EMPTY_BOX_COUNT:STRING, LAST_EMAIL_DATE:STRING, EMAIL_SOURCE_CODE:STRING, AUTOSHIP_CUST_FLAG:STRING, PROMO_FLAG:STRING'

dataflow_options = ['--project=automatic-asset-253215','--job_name=ingest-to-stg-clic-custscontact']
dataflow_options.append('--temp_location=gs://raw_source_files/Customers/temp')
dataflow_options.append('--staging_location=gs://raw_source_files/Customers/temp/stg')
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)

options.view_as(StandardOptions).runner='dataflow'

class Split(beam.DoFn):

    def process(self, element):
        FILE_SET_DATE,TABLE_ID,UNIQUE_ROW_IDENTIFIER,INDUCTION_BATCH_NBR,FORMAT_NUMBER,BUSINESS_DATE,ROW_STATUS_CODE,EDW_BATCH_NBR,ISSUE_LIST,ROW_ID, ROW_CREATED_DATE, ROW_UPDATED_DATE, HSN_ACCT_NUM, ACCOUNT_TYPE, FIRST_NAME, LAST_NAME, SUPRESS_EMAIL_FLAG, SEND_NEWS_FLAG, SUPRESS_CALL_FLAG, SUPRESS_MAIL_FLAG, EMAIL_ADDRESS, HSN_EMP_ID, ORG_EXT_ROW_ID, CREDIT_CARD_ROW_ID, SEX_M_F, ABUSIVE_RETURNS_COUNT, ABUSIVE_RETURNS_FLAG, DIRECT_DEBIT_FLAG, CREDIT_CARD_ONLY, CUSTOMER_SINCE_DATE, FLEX_PAY_LIMIT_FLAG, LAST_ORDER_DATE, LAST_PAYMENT_TYPE, LAST_REFUND_DATE, LAST_RETURN_DATE, MEDIA_TYPE, NAT_TAX_EXEMPT_FLAG, NO_FLEXPAY_FLAG, NO_ON_AIR_FLAG, NO_REFUND_FLAG, NO_SHIP_ADDR_FLAG, PREFER_SHIP_METHOD, ST_TAX_EXEMPT_FLAG, TOTAL_ORDERS, TOTAL_REFUND_ITEMS, TOTAL_PURCHASE_AMT, TOTAL_REFUND_AMT, YTD_KASH_WON_AMT, TOTAL_EXCHANGE_COUNT, CALL_TAG_COUNT, EMPTY_BOX_COUNT, LAST_EMAIL_DATE, EMAIL_SOURCE_CODE, AUTOSHIP_CUST_FLAG, PROMO_FLAG = element.split("|")

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
            'ROW_ID': ROW_ID,
            'ROW_CREATED_DATE': ROW_CREATED_DATE,
            'ROW_UPDATED_DATE': ROW_UPDATED_DATE,
            'HSN_ACCT_NUM': HSN_ACCT_NUM,
            'ACCOUNT_TYPE': ACCOUNT_TYPE,
            'FIRST_NAME': FIRST_NAME,
            'LAST_NAME': LAST_NAME,
            'SUPRESS_EMAIL_FLAG': SUPRESS_EMAIL_FLAG,
            'SEND_NEWS_FLAG': SEND_NEWS_FLAG,
            'SUPRESS_CALL_FLAG': SUPRESS_CALL_FLAG,
            'SUPRESS_MAIL_FLAG': SUPRESS_MAIL_FLAG,
            'EMAIL_ADDRESS': EMAIL_ADDRESS,
            'HSN_EMP_ID': HSN_EMP_ID,
            'ORG_EXT_ROW_ID': ORG_EXT_ROW_ID,
            'CREDIT_CARD_ROW_ID': CREDIT_CARD_ROW_ID,
            'SEX_M_F': SEX_M_F,
            'ABUSIVE_RETURNS_COUNT': ABUSIVE_RETURNS_COUNT,
            'ABUSIVE_RETURNS_FLAG': ABUSIVE_RETURNS_FLAG,
            'DIRECT_DEBIT_FLAG': DIRECT_DEBIT_FLAG,
            'CREDIT_CARD_ONLY': CREDIT_CARD_ONLY,
            'CUSTOMER_SINCE_DATE': CUSTOMER_SINCE_DATE,
            'FLEX_PAY_LIMIT_FLAG': FLEX_PAY_LIMIT_FLAG,
            'LAST_ORDER_DATE': LAST_ORDER_DATE,
            'LAST_PAYMENT_TYPE': LAST_PAYMENT_TYPE,
            'LAST_REFUND_DATE': LAST_REFUND_DATE,
            'LAST_RETURN_DATE': LAST_RETURN_DATE,
            'MEDIA_TYPE': MEDIA_TYPE,
            'NAT_TAX_EXEMPT_FLAG': NAT_TAX_EXEMPT_FLAG,
            'NO_FLEXPAY_FLAG': NO_FLEXPAY_FLAG,
            'NO_ON_AIR_FLAG': NO_ON_AIR_FLAG,
            'NO_REFUND_FLAG': NO_REFUND_FLAG,
            'NO_SHIP_ADDR_FLAG': NO_SHIP_ADDR_FLAG,
            'PREFER_SHIP_METHOD': PREFER_SHIP_METHOD,
            'ST_TAX_EXEMPT_FLAG': ST_TAX_EXEMPT_FLAG,
            'TOTAL_ORDERS': TOTAL_ORDERS,
            'TOTAL_REFUND_ITEMS': TOTAL_REFUND_ITEMS,
            'TOTAL_PURCHASE_AMT': TOTAL_PURCHASE_AMT,
            'TOTAL_REFUND_AMT': TOTAL_REFUND_AMT,
            'YTD_KASH_WON_AMT': YTD_KASH_WON_AMT,
            'TOTAL_EXCHANGE_COUNT': TOTAL_EXCHANGE_COUNT,
            'CALL_TAG_COUNT': CALL_TAG_COUNT,
            'EMPTY_BOX_COUNT': EMPTY_BOX_COUNT,
            'LAST_EMAIL_DATE': LAST_EMAIL_DATE,
            'EMAIL_SOURCE_CODE': EMAIL_SOURCE_CODE,
            'AUTOSHIP_CUST_FLAG': AUTOSHIP_CUST_FLAG,
            'PROMO_FLAG': PROMO_FLAG,
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