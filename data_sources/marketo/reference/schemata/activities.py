from pyspark.sql.types import *

activities_schema = StructType(
    [
        StructField("marketoGUID", IntegerType(), nullable=False),
        StructField("leadId", IntegerType(), nullable=False),
        StructField("activityDate", TimestampType()),
        StructField("activityTypeId", IntegerType()),
        StructField("campaignId", StringType()),
        StructField("primaryAttributeValueId", StringType()),
        StructField("primaryAttributeValue", StringType()),
        StructField("attributes", StringType()),
    ]
)
