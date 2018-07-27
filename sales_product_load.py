import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ['TempDir', 'TABLE_NAME', 'SCHEMA_NAME', 'REDSHIFT_DB_NAME', 'CATALOG_CONNECTION_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)

datasource = glueContext.create_dynamic_frame.from_catalog(
    database = args['SCHEMA_NAME'],
    table_name = args['TABLE_NAME'],
    transformation_ctx = "datasource"
)

########## PYSPARK DATAFRAME TRANSFORMS ##########

#Convert to PySpark Data Frame
sourcedata = datasource.toDF()

split_col = split(sourcedata["quarter"], " ")
sourcedata = sourcedata.withColumn("quarter_new", split_col.getItem(0))
sourcedata = sourcedata.withColumn("profit", col("revenue")*col("gross_margin"))

# Convert back to Glue Dynamic Frame
datasource = DynamicFrame.fromDF(sourcedata, glueContext, "datasource")


applymapping = ApplyMapping.apply(
    frame = datasource,
    mappings = [
        ("retailer_country", "string", "retailer_country", "varchar(20)"), 
        ("order_method_type", "string", "order_method_type", "varchar(15)"), 
        ("retailer_type", "string", "retailer_type", "varchar(30)"),
        ("product_line", "string", "product_line", "varchar(30)"), 
        ("product_type", "string", "product_type", "varchar(30)"), 
        ("product", "string", "product", "varchar(50)"), 
        ("year", "string", "year", "varchar(4)"), 
        ("quarter_new", "string", "quarter", "varchar(2)"), 
        ("revenue", "string", "revenue", "numeric"), 
        ("quantity", "string", "quantity", "integer"), 
        ("gross_margin", "string", "gross_margin", "decimal(15,10)"), 
        ("profit", "double", "profit", "numeric")
    ], 
    transformation_ctx = "applymapping"
)

dropnullfields = DropNullFields.apply(frame = applymapping, transformation_ctx = "dropnullfields")

### datasink (loading) using spark ####

datasink = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = dropnullfields,
    catalog_connection = args['CATALOG_CONNECTION_NAME'],
    connection_options = {
        "dbtable": "{}.{}".format(args['SCHEMA_NAME'], args['TABLE_NAME']),
        "database": args['REDSHIFT_DB_NAME']
	},
    redshift_tmp_dir = args["TempDir"],
    transformation_ctx = "datasink")













