import pyspark
import google
import google.cloud

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType , TimestampType, ShortType, DecimalType, BooleanType
from google.cloud.secretmanager import SecretManagerServiceClient

def import_df(dtable, schema):
    return spark.read \
    .format('jdbc') \
    .schema(schema) \
    .option('url', jdbc_url) \
    .option('dbtable', dtable) \
    .option('driver', jdbc_driver)\
    .load()

def export_df(df, dir):
    return df.write.parquet(f'{export_bucket}/{dir}', mode='overwrite')

secret_client = SecretManagerServiceClient()
secret_name = 'projects/874629723717/secrets/psql-secret/versions/1'
response = secret_client.access_secret_version(request={'name': secret_name})

SQL_HOST = 'localhost'
SQL_PORT = '5432'
DB_NAME = 'Adventureworks'

SQL_USERNAME = 'postgres'
SQL_PASSWORD = response.payload.data.decode('UTF-8')

export_bucket = 'gs://aw-data/preprocessed'

jdbc_url = f'jdbc:postgresql://{SQL_HOST}:{SQL_PORT}/{DB_NAME}?user={SQL_USERNAME}&password={SQL_PASSWORD}'
jdbc_driver = 'org.postgresql.Driver'

spark = SparkSession.builder \
    .getOrCreate()

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

country_region_currency_df = import_df('sales.countryregioncurrency', country_region_currency_schema)
credit_card_df = import_df('sales.creditcard', credit_card_schema)
currency_df = import_df('sales.currency', currency_schema)
currency_rate_df = import_df('sales.currencyrate', currency_rate_schema)
customer_df = import_df('sales.customer', customer_schema)
sales_order_detail_df = import_df('sales.salesorderdetail', sales_order_detail_schema)
sales_order_header_df = import_df('sales.salesorderheader', sales_order_header_schema)
sales_person_df = import_df('sales.salesperson', sales_person_schema)
sales_territory_df = import_df('sales.salesterritory', sales_territory_schema)
special_offer_df = import_df('sales.specialoffer', special_offer_schema)
special_offer_product_df = import_df('sales.specialofferproduct', special_offer_product_schema)
address_df = import_df('person.address', address_schema)
stateprovince_df = import_df('person.stateprovince', stateprovince_schema)
ship_method_df = import_df('purchasing.shipmethod', ship_method_schema)

# ------------------------------
# Export tables
# ------------------------------

export_df(country_region_currency_df, 'sales/countryregioncurrency')
export_df(credit_card_df, 'sales/creditcard')
export_df(currency_df, 'sales/currency')
export_df(currency_rate_df, 'sales/currencyrate')
export_df(customer_df, 'sales/customer')
export_df(sales_order_detail_df, 'sales/salesorderdetail')
export_df(sales_order_header_df, 'sales/salesorderheader')
export_df(sales_person_df, 'sales/salesperson')
export_df(sales_territory_df, 'sales/salesterritory')
export_df(special_offer_df, 'sales/specialoffer')
export_df(special_offer_product_df, 'sales/specialofferproduct')
export_df(address_df, 'person/address')
export_df(stateprovince_df, 'person/stateprovince')
export_df(ship_method_df, 'purchasing/shipmethod')

spark.stop()