from pyspark.sql.types import *

email_templates = StructType(
    [
        StructField("createdAt", StringType, True),
        StructField("description", StringType, True),
        StructField(
            "folder",
            StructType(
                [
                    StructField("folderName", StringType, True),
                    StructField("type", StringType, True),
                    StructField("value", LongType, True),
                ]
            ),
            True,
        ),
        StructField("id", LongType, True),
        StructField("name", StringType, True),
        StructField("status", StringType, True),
        StructField("updatedAt", StringType, True),
        StructField("url", StringType, True),
        StructField("version", LongType, True),
        StructField("workspace", StringType, True),
    ]
)
