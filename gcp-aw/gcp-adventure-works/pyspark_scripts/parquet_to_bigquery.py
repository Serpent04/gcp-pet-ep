import pyspark

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, ShortType, DecimalType, BooleanType
import pyspark.sql.functions as F

def import_df(dir, schema):
    return spark.read\
          .schema(schema)\
          .parquet(f'{import_bucket}/{dir}')

def drop_guid_and_modification_dt_columns(df):
    return  df.drop('rowguid')\
              .drop('modifieddate')   


def export_to_parquet(df, dir):
    return df.write\
        .parquet(f'{export_bucket}/{dir}', mode='overwrite')

def export_to_bigquery(df, table):
    return df.write\
        .format('bigquery')\
        .option('table',f'gcp-adventure-works.{table}')\
        .mode('overwrite')\
        .save()


temp_bucket = 'aw-pyspark-temp-vk'

import_bucket = 'gs://aw-data/preprocessed/'
export_bucket = 'gs://aw-data/processed/'

spark = SparkSession \
       .builder \
       .getOrCreate()

spark.conf.set('temporaryGcsBucket', temp_bucket)

# ------------------------------
# Declare schemas
# ------------------------------

country_region_currency_schema = StructType([
    StructField('countryregioncode', StringType(), True),
    StructField('currencycode', StringType(), True),
    StructField('modifieddate', TimestampType(), True)
    ])

credit_card_schema = StructType([
    StructField('creditcardid', IntegerType(), True),
    StructField('cardtype', StringType(), True),
    StructField('cardnumber', StringType(), True),
    StructField('expmonth', ShortType(), True),
    StructField('expyear', ShortType(), True),
    StructField('modifieddate', TimestampType(), True)
    ])

currency_schema = StructType([
    StructField('currencycode', StringType(), True),
    StructField('name', StringType(), True),
    StructField('modifieddate', TimestampType(), True)
    ])

currency_rate_schema = StructType([
    StructField('currencyrateid', IntegerType(), True),
    StructField('currencyratedate', TimestampType(), True),
    StructField('fromcurrencycode', StringType(), True),
    StructField('tocurrencycode', StringType(), True),
    StructField('averagerate', DecimalType(38, 18), True),
    StructField('endofdayrate', DecimalType(38, 18), True),
    StructField('modifieddate', TimestampType(), True)
    ])

customer_schema = StructType([
    StructField('customerid', IntegerType(), True),
    StructField('personid', IntegerType(), True),
    StructField('storeid', IntegerType(), True),
    StructField('territoryid', IntegerType(), True),
    StructField('rowguid', StringType(), True),
    StructField('modifieddate', TimestampType(), True)
    ])

sales_order_detail_schema = StructType([
    StructField('salesorderid', IntegerType(), True),
    StructField('salesorderdetailid', IntegerType(), True),
    StructField('carriertrackingnumber', StringType(), True),
    StructField('orderqty', ShortType(), True),
    StructField('productid', IntegerType(), True),
    StructField('specialofferid', IntegerType(), True),
    StructField('unitprice', DecimalType(38, 18), True),
    StructField('unitpricediscount', DecimalType(38, 18), True),
    StructField('rowguid', StringType(), True),
    StructField('modifieddate', TimestampType(), True)
    ])

sales_order_header_schema = StructType([
    StructField('salesorderid', IntegerType(), True),
    StructField('revisionnumber', ShortType(), True),
    StructField('orderdate', TimestampType(), True),
    StructField('duedate', TimestampType(), True),
    StructField('shipdate', TimestampType(), True),
    StructField('status', ShortType(), True),
    StructField('onlineorderflag', BooleanType(), True),
    StructField('purchaseordernumber', StringType(), True),
    StructField('accountnumber', StringType(), True),
    StructField('customerid', IntegerType(), True),
    StructField('salespersonid', IntegerType(), True),
    StructField('territoryid', IntegerType(), True),
    StructField('billtoaddressid', IntegerType(), True),
    StructField('shiptoaddressid', IntegerType(), True),
    StructField('shipmethodid', IntegerType(), True),
    StructField('creditcardid', IntegerType(), True),
    StructField('creditcardapprovalcode', StringType(), True),
    StructField('currencyrateid', IntegerType(), True),
    StructField('subtotal', DecimalType(38, 18), True),
    StructField('taxamt', DecimalType(38, 18), True),
    StructField('freight', DecimalType(38, 18), True),
    StructField('totaldue', DecimalType(38, 18), True),
    StructField('comment', StringType(), True),
    StructField('rowguid', StringType(), True),
    StructField('modifieddate', TimestampType(), True)
    ])

sales_order_header_sales_reason_schema = StructType([
    StructField('salesorderid', IntegerType(), True),
    StructField('salesreasonid', IntegerType(), True),
    StructField('modifieddate', TimestampType(), True)
    ])

sales_reason_schema = StructType([
    StructField('salesreasonid', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('reasontype', StringType(), True),
    StructField('modifieddate', TimestampType(), True)
    ])

sales_person_schema = StructType([
    StructField('businessentityid', IntegerType(), True),
    StructField('territoryid', IntegerType(), True),
    StructField('salesquota', DecimalType(38, 18), True),
    StructField('bonus', DecimalType(38, 18), True),
    StructField('commissionpct', DecimalType(38, 18), True),
    StructField('salesytd', DecimalType(38, 18), True),
    StructField('saleslastyear', DecimalType(38, 18), True),
    StructField('rowguid', StringType(), True),
    StructField('modifieddate', TimestampType(), True)
    ])

sales_territory_schema = StructType([
    StructField('territoryid', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('countryregioncode', StringType(), True),
    StructField('group', StringType(), True),
    StructField('salesytd', DecimalType(38, 18), True),
    StructField('saleslastyear', DecimalType(38, 18), True),
    StructField('costytd', DecimalType(38, 18), True),
    StructField('costlastyear', DecimalType(38, 18), True),
    StructField('rowguid', StringType(), True),
    StructField('modifieddate', TimestampType(), True)
    ])

special_offer_schema = StructType([
    StructField('specialofferid', IntegerType(), True),
    StructField('description', StringType(), True),
    StructField('discountpct', DecimalType(38, 18), True),
    StructField('type', StringType(), True),
    StructField('category', StringType(), True),
    StructField('startdate', TimestampType(), True),
    StructField('enddate', TimestampType(), True),
    StructField('minqty', IntegerType(), True),
    StructField('maxqty', IntegerType(), True),
    StructField('rowguid', StringType(), True),
    StructField('modifieddate', TimestampType(), True)
    ])

special_offer_product_schema = StructType([
    StructField('specialofferid', IntegerType(), True),
    StructField('productid', IntegerType(), True),
    StructField('rowguid', StringType(), True),
    StructField('modifieddate', TimestampType(), True)
    ])

store_schema = StructType([
    StructField('businessentityid', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('salespersonid', IntegerType(), True),
    StructField('demographics', StringType(), True),
    StructField('rowguid', StringType(), True),
    StructField('modifieddate', TimestampType(), True)
    ])

address_schema = StructType([
    StructField('addressid', IntegerType(), True),
    StructField('addressline1', StringType(), True),
    StructField('addressline2', StringType(), True),
    StructField('city', StringType(), True),
    StructField('stateprovinceid', IntegerType(), True),
    StructField('postalcode', StringType(), True),
    StructField('spatiallocation', StringType(), True),
    StructField('rowguid', StringType(), True),
    StructField('modifieddate', TimestampType(), True)
    ])

stateprovince_schema = StructType([
    StructField('stateprovinceid', IntegerType(), True),
    StructField('stateprovincecode', StringType(), True),
    StructField('countryregioncode', StringType(), True),
    StructField('isonlystateprovinceflag', BooleanType(), True),
    StructField('name', StringType(), True),
    StructField('territoryid', IntegerType(), True),
    StructField('rowguid', StringType(), True),
    StructField('modifieddate', TimestampType(), True)
    ])

ship_method_schema = StructType([
    StructField('shipmethodid', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('shipbase', DecimalType(38, 18), True),
    StructField('shiprate', DecimalType(38, 18), True),
    StructField('rowguid', StringType(), True),
    StructField('modifieddate', TimestampType(), True)
    ])

product_schema = StructType([
    StructField('productid', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('productnumber', StringType(), True),
    StructField('makeflag', BooleanType(), True),
    StructField('finishedgoodsflag', BooleanType(), True),
    StructField('color', StringType(), True),
    StructField('safetystocklevel', ShortType(), True),
    StructField('reorderpoint', ShortType(), True),
    StructField('standardcost', DecimalType(38, 18), True),
    StructField('listprice', DecimalType(38, 18), True),
    StructField('size', StringType(), True),
    StructField('sizeunitmeasurecode', StringType(), True),
    StructField('weightunitmeasurecode', StringType(), True),
    StructField('weight', DecimalType(8, 2), True),
    StructField('daystomanufacture', IntegerType(), True),
    StructField('productline', StringType(), True),
    StructField('class', StringType(), True),
    StructField('style', StringType(), True),
    StructField('productsubcategoryid', IntegerType(), True),
    StructField('productmodelid', IntegerType(), True),
    StructField('sellstartdate', TimestampType(), True),
    StructField('sellenddate', TimestampType(), True),
    StructField('discontinueddate', TimestampType(), True),
    StructField('rowguid', StringType(), True),
    StructField('modifieddate', TimestampType(), True)
    ])

# ------------------------------
# Import tables
# ------------------------------

country_region_currency_df = import_df('sales/countryregioncurrency', country_region_currency_schema)
credit_card_df = import_df('sales/creditcard', credit_card_schema)
currency_df = import_df('sales/currency', currency_schema)
currency_rate_df = import_df('sales/currencyrate', currency_rate_schema)
customer_df = import_df('sales/customer', customer_schema)
sales_order_detail_df = import_df('sales/salesorderdetail', sales_order_detail_schema)
sales_order_header_df = import_df('sales/salesorderheader', sales_order_header_schema)
sales_order_header_sales_reason_df = import_df('sales/salesorderheadersalesreason', sales_order_header_sales_reason_schema)
sales_reason_df = import_df('sales/salesreason', sales_reason_schema)
sales_person_df = import_df('sales/salesperson', sales_person_schema)
sales_territory_df = import_df('sales/salesterritory', sales_territory_schema)
special_offer_df = import_df('sales/specialoffer', special_offer_schema)
special_offer_product_df = import_df('sales/specialofferproduct', special_offer_product_schema)
store_df = import_df('sales/store', store_schema)
address_df = import_df('person/address', address_schema)
stateprovince_df = import_df('person/stateprovince', stateprovince_schema)
ship_method_df = import_df('purchasing/shipmethod', ship_method_schema)
product_df = import_df('production/product', product_schema)

# -------------------------------
# CLEAN DATA
# -------------------------------

customer_df = drop_guid_and_modification_dt_columns(customer_df)
sales_order_detail_df = drop_guid_and_modification_dt_columns(sales_order_detail_df)
sales_order_header_df = drop_guid_and_modification_dt_columns(sales_order_header_df)
sales_order_header_sales_reason_df = drop_guid_and_modification_dt_columns(sales_order_header_sales_reason_df)
sales_reason_df = drop_guid_and_modification_dt_columns(sales_reason_df)
sales_person_df = drop_guid_and_modification_dt_columns(sales_person_df)
sales_territory_df = drop_guid_and_modification_dt_columns(sales_territory_df)
special_offer_df = drop_guid_and_modification_dt_columns(special_offer_df)
special_offer_product_df = drop_guid_and_modification_dt_columns(special_offer_product_df)
store_df = drop_guid_and_modification_dt_columns(store_df)
address_df = drop_guid_and_modification_dt_columns(address_df)
stateprovince_df = drop_guid_and_modification_dt_columns(stateprovince_df)
ship_method_df = drop_guid_and_modification_dt_columns(ship_method_df)
product_df = drop_guid_and_modification_dt_columns(product_df)

# -------------------------------
# Transformation
# -------------------------------

sales_reason_df = sales_reason_df\
    .withColumnRenamed('name', 'reasonname')

sales_territory_df = sales_territory_df\
    .withColumnRenamed('name', 'region')\
    .withColumnRenamed('group', 'zone')

store_df = store_df\
    .withColumnRenamed('name', 'storename')

ship_method_df = ship_method_df\
    .withColumnRenamed('name', 'shipmethodname')

product_df = product_df\
    .withColumnRenamed('name', 'productname')

stateprovince_df = stateprovince_df.withColumnRenamed('name', 'statename')\
    .select('stateprovinceid', 'stateprovincecode', 'countryregioncode', 'statename')

address_with_state_df = address_df\
    .join(F.broadcast(stateprovince_df),
                                address_df['stateprovinceid'] == stateprovince_df['stateprovinceid'], how='left')\
                        .select('addressid', 'addressline1', 'addressline2',
                                'city', 'stateprovincecode', 'countryregioncode',
                                'statename', 'postalcode', 'spatiallocation')

sales_reason_fact_df = sales_order_header_sales_reason_df\
    .join(F.broadcast(sales_reason_df),
                                sales_order_header_sales_reason_df['salesreasonid'] == sales_reason_df['salesreasonid'], how='left')\
                        .select(F.col('salesorderid').alias('id'),'reasonname', 'reasontype')
# -------------------------------
# Enrich fact table
# -------------------------------

sales_fact_df = sales_order_header_df\
    .withColumn('status',
                F.when(F.col('status') == 1, 'In process')\
                .when(F.col('status') == 2, 'Approved')\
                .when(F.col('status') == 3, 'Backordered')\
                .when(F.col('status') == 4, 'Rejected')\
                .when(F.col('status') == 5, 'Shipped')\
                .when(F.col('status') == 6, 'Cancelled'))\
    .join(F.broadcast(sales_reason_fact_df),
          sales_order_header_df['salesorderid'] == sales_reason_fact_df['id'], how='left')\
    .join(F.broadcast(sales_territory_df),
          sales_order_header_df['territoryid'] == sales_territory_df['territoryid'], how='left')\
    .join(F.broadcast(address_with_state_df),
          sales_order_header_df['shiptoaddressid'] == address_with_state_df['addressid'], how='left')\
    .join(F.broadcast(ship_method_df),
          sales_order_header_df['shipmethodid'] == ship_method_df['shipmethodid'], how='left')\
    .join(F.broadcast(currency_rate_df),
          sales_order_header_df['currencyrateid'] == currency_rate_df['currencyrateid'], how='left')\
    .withColumn('region', F.when(F.col('region').isin(['Southwest', 'Southeast', 'Northwest',
                                                       'Northeast', 'Central']), F.lit('United States')).otherwise(F.col('region')))\
    .fillna({'reasonname': 'Unknown', 'reasontype': 'Unknown'})

sales_fact_df = sales_fact_df\
    .withColumn('subtotal', F.when(F.col('averagerate').isNull(), F.col('subtotal')).otherwise(F.col('subtotal') * F.col('averagerate')))\
    .withColumn('taxamt', F.when(F.col('averagerate').isNull(), F.col('taxamt')).otherwise(F.col('taxamt') * F.col('averagerate')))\
    .withColumn('freight', F.when(F.col('averagerate').isNull(), F.col('freight')).otherwise(F.col('freight') * F.col('averagerate')))\
    .withColumn('totaldue', F.when(F.col('averagerate').isNull(), F.col('totaldue')).otherwise(F.col('totaldue') * F.col('averagerate')))

sales_fact_df = sales_fact_df\
    .select('salesorderid', 'orderdate', 'duedate', 'shipdate', 'status', 
            'reasonname','reasontype', 'onlineorderflag',
            'accountnumber', 'customerid', F.col('region').alias('saleregion'),
            F.col('zone').alias('salezone'), F.col('city').alias('shiptocity'), F.col('statename').alias('shiptostate'), 
            'shipmethodname', 'subtotal', 'taxamt', 'freight', 'totaldue')

# -------------------------------
# EXPORT TO GCS
# -------------------------------

export_to_parquet(country_region_currency_df, 'sales/countryregioncurrency')
export_to_parquet(credit_card_df, 'sales/creditcard')
export_to_parquet(currency_df, 'sales/currency')
export_to_parquet(currency_rate_df, 'sales/currencyrate')
export_to_parquet(customer_df, 'sales/customer')
export_to_parquet(sales_order_detail_df, 'sales/salesorderdetail')
export_to_parquet(sales_fact_df, 'sales/salesfact')
export_to_parquet(sales_order_header_df, 'sales/salesorderheader')
export_to_parquet(sales_order_header_sales_reason_df, 'sales/salesorderheadersalesreason')
export_to_parquet(sales_reason_df, 'sales/salesreason')
export_to_parquet(sales_person_df, 'sales/salesperson')
export_to_parquet(sales_territory_df, 'sales/salesterritory')
export_to_parquet(special_offer_df, 'sales/specialoffer')
export_to_parquet(special_offer_product_df, 'sales/specialofferproduct')
export_to_parquet(store_df, 'sales/store')
export_to_parquet(address_with_state_df, 'person/address')
export_to_parquet(ship_method_df, 'purchasing/shipmethod')
export_to_parquet(product_df, 'production/product')

# -------------------------------
# EXPORT TO BIGQUERY
# -------------------------------

export_to_bigquery(country_region_currency_df, 'sales.countryregioncurrency')
export_to_bigquery(credit_card_df, 'sales.creditcard')
export_to_bigquery(currency_df, 'sales.currency')
export_to_bigquery(currency_rate_df, 'sales.currencyrate')
export_to_bigquery(customer_df, 'sales.customer')
export_to_bigquery(sales_order_detail_df, 'sales.salesorderdetail')
export_to_bigquery(sales_order_header_df, 'sales.salesorderheader')
export_to_bigquery(sales_order_header_sales_reason_df, 'sales.salesorderheadersalesreason')
export_to_bigquery(sales_reason_df, 'sales.salesreason')
export_to_bigquery(sales_fact_df, 'sales.salesfact')
export_to_bigquery(sales_person_df, 'sales.salesperson')
export_to_bigquery(sales_territory_df, 'sales.salesterritory')
export_to_bigquery(special_offer_df, 'sales.specialoffer')
export_to_bigquery(special_offer_product_df, 'sales.specialofferproduct')
export_to_bigquery(store_df, 'sales.store')
export_to_bigquery(address_with_state_df, 'person.address')
export_to_bigquery(ship_method_df, 'purchasing.shipmethod')
export_to_bigquery(product_df, 'production.product')


spark.stop()