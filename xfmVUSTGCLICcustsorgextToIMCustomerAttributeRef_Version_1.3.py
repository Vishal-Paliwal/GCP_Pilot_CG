from __future__ import absolute_import
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions


output_table = 'automatic-asset-253215:CORE.IM_CUSTOMER_ATTRIBUTE_REF'
dataflow_options = {'--project=automatic-asset-253215',
                    '--job_name=xfm-vustgclic-custsorgext-to-imcustomerattributeref',
                    '--temp_location=gs://raw_source_files/Customers/temp',
                    '--staging_location=gs://raw_source_files/Customers/temp/stg'}
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)
options.view_as(StandardOptions).runner = 'dataflow'


class LeftJoin(beam.PTransform):
    """This PTransform performs a left join given source_pipeline_name, source_data,
     join_pipeline_name, join_data, common_key constructors"""

    def __init__(self, src_pipeline, CustOrg_ID, join_pipeline, IMCust_ID, common_key):
        self.join_pipeline = join_pipeline
        self.CustOrg_ID = CustOrg_ID
        self.src_pipeline = src_pipeline
        self.IMCust_ID = IMCust_ID
        self.common_key = common_key

    def expand(self, pcolls):
        def _format_as_common_key_tuple(data_dict, common_key):
            return data_dict[common_key], data_dict

        """This part here below starts with a python dictionary comprehension in case you 
        get lost in what is happening :-)"""
        return ({pipeline_name: pcoll | 'Convert to ({0}, object) for {1}'
                .format(self.common_key, pipeline_name) >> beam.Map(_format_as_common_key_tuple, self.common_key)
                 for (pipeline_name, pcoll) in pcolls.items()}
                | 'CoGroupByKey {0}'.format(pcolls.keys()) >> beam.CoGroupByKey()
                | 'Unnest Cogrouped' >> beam.ParDo(UnnestCoGrouped(), self.src_pipeline,
                                                   self.join_pipeline)
                )


class UnnestCoGrouped(beam.DoFn):
    """This DoFn class unnests the CogroupBykey output and emits """

    def process(self, input_element, src_pipeline, join_pipeline):
        group_key, grouped_dict = input_element
        join_dictionary = grouped_dict[join_pipeline]
        source_dictionaries = grouped_dict[src_pipeline]
        for source_dictionary in source_dictionaries:
            try:
                source_dictionary.update(join_dictionary[0])
                yield source_dictionary
            except IndexError:  # found no join_dictionary
                yield source_dictionary


def run():
    p = beam.Pipeline(options=options)
    src_pipeline = 'CustOrg_ID'
    CustOrg_ID = (p
                  | 'Reading HSN_ID' >> beam.io.Read(
                                      beam.io.BigQuerySource(
                                       query="""SELECT HSN_ACCT_NUM AS CUSTOMER_ID 
                                       FROM `automatic-asset-253215.STAGE.STG_CLIC_CUSTSORGEXT`""",
                                       use_standard_sql=True))
                  )
    join_pipeline = 'IMCust_ID'
    IMCust_ID = (p
                 | 'Reading Cust_ID' >> beam.io.Read(
                                      beam.io.BigQuerySource(
                                       query="""SELECT CUSTOMER_ID 
                                       FROM `automatic-asset-253215.CORE.IM_CUSTOMER_ATTRIBUTE_REF`""",
                                       use_standard_sql=True))
                 )
    common_key = 'CUSTOMER_ID'
    pipelines_dictionary = {src_pipeline: CustOrg_ID, join_pipeline: IMCust_ID}

    read_query = """SELECT
          CAST(a.HSN_ACCT_NUM AS INT64) AS CUSTOMER_ID,
          CAST(a.ROW_CREATED_DATE AS TIMESTAMP) AS SOURCE_CREATE_DT,
          a.PRIMARY_PHONE_NUM AS PRIMARY_PHONE_NUMBER,
          CAST(SUBSTR(a.PRIMARY_PHONE_NUM,1,3) AS INT64) AS DERIVED_AREA_CODE,
          CASE WHEN LENGTH(a.BILL_SHIP_ADDR_SYNC_FLAG) = 0 THEN 0
          ELSE CAST(a.BILL_SHIP_ADDR_SYNC_FLAG AS INT64) 
          END AS BILLSHIP_ADDR_SYNC_IND,
          a.GUEST_CUSTOMER_FLAG AS GUEST_CODE,
          a.GUID AS DIGITAL_CUSTOMER_ID,
          a.MARKET_PLACE_ID AS MARKET_PLACE_ID,
          a.MARKET_PLACE_CUSTID AS MARKET_PLACE_CUST_ID,
          '0' AS CUSTOMER_KEY,
          '999999999999' AS PRIMARY_ADDRESS_KEY,
          '999999999999' AS BILLING_ADDRESS_KEY,
          'CLIC' AS ETL_SOURCE_SYSTEM,
          '0' AS PURGED_IND,
          '0' AS MERGED_IND,
          '0' AS TEST_CUSTOMER_IND,
          '0' AS BLOCK_CURRENT_IND,
          '0' AS BLOCK_LIFETIME_IND, 
          '999999999999' AS IDCENTRIC_INDIVIDUAL_ID,
          '999999999999' AS IDCENTRIC_HOUSEHOLD_ID,
          '999999999999' AS IDCENTRIC_ADDRESS_ID,
          '0' AS VOID_IND,
          CAST(FORMAT_DATETIME('%Y%m%d%H%M%S', CURRENT_DATETIME()) AS INT64) AS UPD_BATCH_NBR,
          CAST(FORMAT_DATETIME('%Y%m%d%H%M%S', CURRENT_DATETIME()) AS INT64) AS INS_BATCH_NBR,
          '999999999999' AS MKTADDR_ADDRESS_KEY,
          '0' AS PRIVACY_IND
        FROM
          `automatic-asset-253215.STAGE.STG_CLIC_CUSTSORGEXT` a
       -- WHERE CAST(a.HSN_ACCT_NUM AS INT64) NOT IN ({})
       """

    join_data = (pipelines_dictionary
                 | 'Left join' >> LeftJoin(src_pipeline, CustOrg_ID, join_pipeline, IMCust_ID, common_key)
                 | 'Combine' >> beam.combiners.ToList()
                 # | 'Reading query' >> beam.Map(lambda x: read_query.format(','.join(map(lambda x: '"' + x + '"', x))))
                 | 'Read from STG_CLIC_CUSTSORGEXT' >> beam.io.Read(
                                      beam.io.BigQuerySource(query=read_query, use_standard_sql=True))
                 | 'Write to IM_CUSTOMER_ATTRIBUTE_REF' >> beam.io.WriteToBigQuery(
                                       output_table,
                                       write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                       create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)

                 )
    p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
