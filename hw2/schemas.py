from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Users schema matches hw2/generate_data_json.py (field: signup_date)
user_schema = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("signup_date", StringType(), nullable=True),
        StructField("plan", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("marketing_opt_in", BooleanType(), nullable=True),
    ]
)

# Items schema: tags is an array of strings
item_schema = StructType(
    [
        StructField("item_id", IntegerType(), nullable=False),
        StructField("category", StringType(), nullable=True),
        StructField("tags", ArrayType(StringType()), nullable=True),
    ]
)

# Events schema mirrors nested JSON
events_schema = StructType(
    [
        StructField("ts", StringType(), nullable=True),
        StructField("event", StringType(), nullable=True),
        StructField("user_id", IntegerType(), nullable=True),
        StructField("item_id", IntegerType(), nullable=True),
        StructField(
            "context",
            StructType(
                [
                    StructField("country", StringType(), nullable=True),
                    StructField("device", StringType(), nullable=True),
                    StructField("locale", StringType(), nullable=True),
                    StructField("session_id", StringType(), nullable=True),
                ]
            ),
            nullable=True,
        ),
        StructField(
            "props",
            StructType(
                [
                    StructField("price", DoubleType(), nullable=True),
                    StructField("payment_method", StringType(), nullable=True),
                    StructField("dwell_ms", IntegerType(), nullable=True),
                ]
            ),
            nullable=True,
        ),
        StructField(
            "exp",
            StructType(
                [StructField("ab_group", StringType(), nullable=True)]
            ),
            nullable=True,
        ),
    ]
)
