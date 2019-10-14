from __future__ import absolute_import
import logging
import apache_beam as beam
from apache_beam.io.gcp import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

# input_table = 'automatic-asset-253215:customer_dataset.customer_data'
output_table = 'automatic-asset-253215:CORE.IM_CUSTOMER_ATTRIBUTE_REF_NEW'
dataflow_options = {'--project=automatic-asset-253215', '--job_name=xfm-vustgclic-custsorgext-to-imcustomerattributeref',
                    '--temp_location=gs://raw_source_files/Customers/temp',
                    '--staging_location=gs://raw_source_files/Customers/temp/stg'}
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)

options.view_as(StandardOptions).runner = 'dataflow'


def run():
    read_query = """SELECT
  CAST(a.HSN_ACCT_NUM AS INT64) AS CUSTOMER_ID,
  CAST(a.ROW_CREATED_DATE AS TIMESTAMP) AS SOURCE_CREATE_DT,
  a.PRIMARY_PHONE_NUM AS PRIMARY_PHONE_NUMBER,
  CAST(SUBSTR(a.PRIMARY_PHONE_NUM,1,3) AS INT64) AS DERIVED_AREA_CODE,
  CAST(a.BILL_SHIP_ADDR_SYNC_FLAG AS INT64) AS BILLSHIP_ADDR_SYNC_IND,
  a.GUEST_CUSTOMER_FLAG AS GUEST_CODE
FROM
  `automatic-asset-253215.STAGE.STG_CLIC_CUSTSORGEXT` a"""

    p = beam.Pipeline(options=options)
    custorg_data = (p
                    | 'Read from STG_CLIC_CUSTSORGEXT' >> beam.io.Read(
                                                beam.io.BigQuerySource(query=read_query, use_standard_sql=True))
                    )

    derived_data = (p
                    | 'Creating Derived Columns' >> beam.Create(
                [{'CUSTOMER_KEY': '12345', 'PRIMARY_ADDRESS_KEY': '999999999999', 'BILLING_ADDRESS_KEY': '999999999999',
                  'ETL_SOURCE_SYSTEM': 'CLIC', 'PURGED_IND': '0', 'MERGED_IND': '0', 'TEST_CUSTOMER_IND': '0',
                  'BLOCK_CURRENT_IND': '0', 'BLOCK_LIFETIME_IND': '0', 'IDCENTRIC_INDIVIDUAL_ID': '999999999999',
                  'IDCENTRIC_HOUSEHOLD_ID': '999999999999',
                  'IDCENTRIC_ADDRESS_ID': '999999999999', 'VOID_IND': '0', 'UPD_BATCH_NBR': '20191014050400',
                  'INS_BATCH_NBR': '20191014050400', 'MKTADDR_ADDRESS_KEY': '999999999999', 'PRIVACY_IND': '0'}])
                )

    merged_data = (
        (custorg_data, derived_data)
        | beam.Flatten()
        | 'Write to IM_CUSTOMER_ATTRIBUTE_REF' >> beam.io.WriteToBigQuery(
                                                        output_table,
                                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)

    )
    p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
