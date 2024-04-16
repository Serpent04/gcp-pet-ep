import pyspark

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, ShortType, DecimalType, BooleanType
import pyspark.sql.functions as F

def import_df(dir, schema):
    return spark.read\
          .schema(schema)\
          .parquet(f'{import_bucket}/{dir}')

def drop_guid(df):
    return  df.drop('rowguid')


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
sales_person_df = import_df('sales/salesperson', sales_person_schema)
sales_territory_df = import_df('sales/salesterritory', sales_territory_schema)
special_offer_df = import_df('sales/specialoffer', special_offer_schema)
special_offer_product_df = import_df('sales/specialofferproduct', special_offer_product_schema)
address_df = import_df('person/address', address_schema)
stateprovince_df = import_df('person/stateprovince', stateprovince_schema)
ship_method_df = import_df('purchasing/shipmethod', ship_method_schema)

# -------------------------------
# CLEAN DATA
# -------------------------------

customer_df = drop_guid(customer_df)
sales_order_detail_df = drop_guid(sales_order_detail_df)
sales_order_header_df = drop_guid(sales_order_header_df)
sales_person_df = drop_guid(sales_person_df)
sales_territory_df = drop_guid(sales_territory_df)
special_offer_df = drop_guid(special_offer_df)
special_offer_product_df = drop_guid(special_offer_product_df)
address_df = drop_guid(address_df)
stateprovince_df = drop_guid(stateprovince_df)
ship_method_df = drop_guid(ship_method_df)

# -------------------------------
# Enrich tables
# -------------------------------

sales_fact_df = sales_order_header_df\
    .withColumn('status_name',
                F.when(F.col('status') == 1, 'In process') \
                .when(F.col('status') == 2, 'Approved')\
                .when(F.col('status') == 3, 'Backordered')\
                .when(F.col('status') == 4, 'Rejected')\
                .when(F.col('status') == 5, 'Shipped')\
                .when(F.col('status') == 6, 'Cancelled'))

stateprovince_df = stateprovince_df.withColumnRenamed('name', 'statename')\
    .select('stateprovinceid', 'stateprovincecode', 'countryregioncode', 'statename')

address_with_state_df = address_df\
    .join(F.broadcast(stateprovince_df),
                             address_df['stateprovinceid'] == stateprovince_df['stateprovinceid'],
                             'left')\
                        .select('addressid', 'addressline1', 'addressline2',
                                'city', 'stateprovincecode', 'countryregioncode',
                                'statename', 'postalcode', 'spatiallocation',
                                'modifieddate')

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
export_to_parquet(sales_person_df, 'sales/salesperson')
export_to_parquet(sales_territory_df, 'sales/salesterritory')
export_to_parquet(special_offer_df, 'sales/specialoffer')
export_to_parquet(special_offer_product_df, 'sales/specialofferproduct')
export_to_parquet(address_with_state_df, 'person/address')
export_to_parquet(ship_method_df, 'purchasing/shipmethod')

# -------------------------------
# EXPORT TO BIGQUERY
# -------------------------------

export_to_bigquery(country_region_currency_df, 'sales.countryregioncurrency')
export_to_bigquery(credit_card_df, 'sales.creditcard')
export_to_bigquery(currency_df, 'sales.currency')
export_to_bigquery(currency_rate_df, 'sales.currencyrate')
export_to_bigquery(customer_df, 'sales.customer')
export_to_bigquery(sales_order_detail_df, 'sales.salesorderdetail')
export_to_bigquery(sales_fact_df, 'sales.salesfact')
export_to_bigquery(sales_person_df, 'sales.salesperson')
export_to_bigquery(sales_territory_df, 'sales.salesterritory')
export_to_bigquery(special_offer_df, 'sales.specialoffer')
export_to_bigquery(special_offer_product_df, 'sales.specialofferproduct')
export_to_bigquery(address_with_state_df, 'person.address')
export_to_bigquery(ship_method_df, 'purchasing.shipmethod')


spark.stop()
