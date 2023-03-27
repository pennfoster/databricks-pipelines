from pyspark.sql.types import *

channels = StructType(
    [
        StructField("applicableProgramType", StringType(), True),
        StructField("createdAt", StringType(), True),
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField(
            "progressionStatuses",
            ArrayType(
                StructType(
                    [
                        StructField("hidden", BooleanType(), True),
                        StructField("name", StringType(), True),
                        StructField("step", LongType(), True),
                        StructField("success", BooleanType(), True),
                        StructField("type", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("updatedAt", StringType(), True),
    ]
)
