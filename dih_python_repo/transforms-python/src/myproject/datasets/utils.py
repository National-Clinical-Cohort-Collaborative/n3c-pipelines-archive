from pyspark.sql import functions as F


def identity(x):
    return x


def lower_case_cols(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())
    return df


def parse_date_from_payload_name(df, payload_col, new_col_name):
    # Look for 8 digits at the end of the filepath representing payload date
    df = df.withColumn("parsed_payload_date", F.regexp_extract(F.col(payload_col), "(?i)(\d{8})(.*)(\.zip)$", 1))
    # Handle either yyyyMMDD or MMDDyyyy format
    df = df.withColumn(
        "processed_payload_date",
        F.when(F.regexp_extract(F.col(payload_col), "(202.)\d{4}", 1) == F.lit(""), F.concat(F.col("parsed_payload_date").substr(5,4), F.col("parsed_payload_date").substr(1,4)))\
        .otherwise(F.col("parsed_payload_date"))
    )
    # Convert to date
    df = df.withColumn(
        new_col_name,
        F.to_date(df["processed_payload_date"], 'yyyyMMdd')
    ).drop("parsed_payload_date").drop("processed_payload_date")

    return df
