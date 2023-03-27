from pyspark.sql.types import *

emails = StructType(
    [
        StructField("autoCopyToText", BooleanType(), True),
        StructField("createdAt", StringType(), True),
        StructField("description", StringType(), True),
        StructField(
            "folder",
            StructType(
                [
                    StructField("folderName", StringType(), True),
                    StructField("type", StringType(), True),
                    StructField("value", LongType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "fromEmail",
            StructType(
                [
                    StructField("type", StringType(), True),
                    StructField("value", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "fromName",
            StructType(
                [
                    StructField("type", StringType(), True),
                    StructField("value", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("id", LongType(), True),
        StructField("isOpenTrackingDisabled", BooleanType(), True),
        StructField("name", StringType(), True),
        StructField("operational", BooleanType(), True),
        StructField("preHeader", StringType(), True),
        StructField("publishToMSI", BooleanType(), True),
        StructField(
            "replyEmail",
            StructType(
                [
                    StructField("type", StringType(), True),
                    StructField("value", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("status", StringType(), True),
        StructField(
            "subject",
            StructType(
                [
                    StructField("type", StringType(), True),
                    StructField("value", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("template", LongType(), True),
        StructField("textOnly", BooleanType(), True),
        StructField("updatedAt", StringType(), True),
        StructField("url", StringType(), True),
        StructField("version", LongType(), True),
        StructField("webView", BooleanType(), True),
        StructField("workspace", StringType(), True),
    ]
)
