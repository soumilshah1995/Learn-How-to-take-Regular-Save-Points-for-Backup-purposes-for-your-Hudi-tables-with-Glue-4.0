try:
    import sys
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
    from pyspark.sql.functions import *
    from awsglue.utils import getResolvedOptions
    from pyspark.sql.types import *
    from datetime import datetime, date
    import boto3
    from functools import reduce
    from pyspark.sql import Row

    import uuid
    from faker import Faker
except Exception as e:
    print("Modules are missing : {} ".format(e))

job_start_ts = datetime.now()
ts_format = '%Y-%m-%d %H:%M:%S'

spark = (SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
         .config('spark.sql.hive.convertMetastoreParquet', 'false') \
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
         .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true').getOrCreate())

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()



global faker
faker = Faker()


class DataGenerator(object):

    @staticmethod
    def get_data():
        return [
            (
                uuid.uuid4().__str__(),
                faker.name(),
                faker.random_element(elements=('IT', 'HR', 'Sales', 'Marketing')),
                faker.random_element(elements=('CA', 'NY', 'TX', 'FL', 'IL', 'RJ')),
                str(faker.random_int(min=10000, max=150000)),
                str(faker.random_int(min=18, max=60)),
                str(faker.random_int(min=0, max=100000)),
                str(faker.unix_time()),
                faker.email(),
                faker.credit_card_number(card_type='amex'),
                faker.date()
            ) for x in range(100)
        ]


data = DataGenerator.get_data()
columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts", "email", "credit_card",
           "date"]

spark_df = spark.createDataFrame(data=data, schema=columns)

# ============================== Settings =======================================
db_name = "hudidb"
table_name = "employees"
recordkey = 'emp_id'
precombine = "ts"
PARTITION_FIELD = 'state'
path = "s3://hudi-demos-emr-serverless-project-soumil/tmp/"
method = 'bulk_insert'
table_type = "COPY_ON_WRITE"
# ====================================================================================

hudi_part_write_config = {
    'className': 'org.apache.hudi',

    'hoodie.table.name': table_name,
    'hoodie.datasource.write.table.type': table_type,
    'hoodie.datasource.write.operation': method,
    'hoodie.bulkinsert.sort.mode': "NONE",
    'hoodie.datasource.write.recordkey.field': recordkey,
    'hoodie.datasource.write.precombine.field': precombine,

    'hoodie.datasource.hive_sync.mode': 'hms',
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.use_jdbc': 'false',
    'hoodie.datasource.hive_sync.support_timestamp': 'false',
    'hoodie.datasource.hive_sync.database': db_name,
    'hoodie.datasource.hive_sync.table': table_name,

}

spark_df.write.format("hudi").options(**hudi_part_write_config).mode("append").save(path)

# =========================Stored procedures ========================================


try:
    query_show_commits = f"call show_commits('{db_name}.{table_name}', 5)"
    spark_df_commits = spark.sql(query_show_commits)
    commits = list(map(lambda row: row[0], spark_df_commits.collect()))

    query_save_point = f"call create_savepoint('{db_name}.{table_name}', '{commits[0]}')"
    execute_save_point = spark.sql(query_save_point)

    show_check_points_query = f"call show_savepoints('{db_name}.{table_name}')"
    show_check_points_query_df = spark.sql(show_check_points_query)

    print(f"""
    ************STATS*************
    query {query_show_commits}
    spark_df {spark_df_commits.show()}
    commits {commits}
    Latest commit: {commits[0]}
    
    ############# Save Points ###########
    query:  {query_save_point}
    save_point {execute_save_point.show()}
    
    ############# SHOW CHECK POINT ###########
    query:  {show_check_points_query}
    show_check_points_query_df {show_check_points_query_df.show()}
    *******************************
    """)

except Exception as e:
    print("Error : {} ".format(e))



