from pyspark.sql import functions as F
from transforms.api import transform, Input, Output, configure
from source_cdm_utils.parse import get_newest_csv_payload
import logging
from pyspark.sql.types import StringType, IntegerType, DateType, StructType, StructField

'''

Unions the manifest files from sites and performs the following actions on the columns:

"site_abbrev", - preseve for downstream site id lookups
"site_name", - preseve for downstream site id lookups
"contact_name", - remove
"contact_email", - remove
"naaccr_version", - preserve
"data_lag_months", - preserve
"naaccr_extract_date" - preserve

'''

approved_schema_with_datetypes = StructType([
    StructField("site_abbrev", StringType(), True),
    StructField("site_name", StringType(), True),
    StructField("naaccr_version", IntegerType(), True),
    StructField("data_lag_months", IntegerType(), True),
    StructField("naaccr_extract_date", DateType(), True),
    ])


logger = logging.getLogger()

naaccr_path = "/UNITE/N3Clinical PPRL Raw Incoming Data/NAACCR/"

# to do - replace this with lookup table of cols: site name, tenant, site ID, cancer pprl type
naaccr_files = [
    ("Site_349_NAACCR_manifest", 349),
    ("Site_179_NAACCR_manifest", 179),
    ("Site_126_NAACCR_manifest", 126),  # contains nulls for N3C_PATIENT_ID
    ("Site_220_NAACCR_manifest", 220),
]

inputs = {site_file[0]: Input(naaccr_path + site_file[0]) for site_file in naaccr_files}


@configure(profile=["EXECUTOR_MEMORY_LARGE", "NUM_EXECUTORS_8"])
@transform(
    **inputs,
    unioned_latest_naaccr_manifest_data=Output(
        "/UNITE/NAACCR Logic - RWD Pipeline - N3Clinical/datasets/NAACCR_manifest_N3Clinical"
    )
)
def my_compute_function(unioned_latest_naaccr_manifest_data, ctx, **all_dfs):
    def parse_csv_latest(payload_input, regex):
        # Get relevant csv files from the payload
        files_df = payload_input.filesystem().files(regex=regex)

        # Parse the LATEST CSV file into a dataset
        newest_file = get_newest_csv_payload(files_df)
        logging.info("newest file")
        logging.info(newest_file)

        hadoop_path = payload_input.filesystem().hadoop_path
        logging.info("hadoop path")
        logging.info(hadoop_path)

        # Create the full path for the newest file
        file_path = f"{hadoop_path}/{newest_file}"
        logging.info("file path")
        logging.info(file_path)

        # Read the CSV file into a DataFrame
        df = (
            ctx.spark_session.read.format("csv")
            .option("header", "true")
            .option("delimiter", ",")
            .option("inferSchema", True)
            .load(file_path)
        )

        return df

    approved_schema_cols = [f.name for f in approved_schema_with_datetypes.fields]

    def parse_date_column(col):
        return F.coalesce(
            F.to_date(col, "dd-MMM-yyyy"),
            F.to_date(col, "yyyy-MM-dd"),
            F.to_date(col, "yyyyMMdd"),
            F.to_date(col, "MM/dd/yyyy")
            )

    def create_empty_union_df(spark):
        schema_with_extra = approved_schema_with_datetypes \
            .add("data_partner_id", StringType()) \
            .add("payload", StringType())
        return spark.createDataFrame([], schema_with_extra)

    unioned_latest_naaccr_manifest = create_empty_union_df(ctx.spark_session)

    logging.info("before loop")
    for site in naaccr_files:
        source_df = all_dfs[site[0]]
        dp = site[1]
        regex = "(?i).*NAACCR_manifest.*\\.csv"
        # ensure we parse only the latest csv file from RI
        latest_csv_df = parse_csv_latest(source_df, regex)

        logging.info("selected schemas")
        logging.info(latest_csv_df.printSchema())
        logging.info(approved_schema_cols)

        # Add missing columns that are only present in approved_cols_df with null values (ensures NAACCR dataframe
        # schema stays consistent)
        for col in approved_schema_cols:
            if col not in latest_csv_df.columns:
                latest_csv_df = latest_csv_df.withColumn(
                    col, F.lit(None).cast(StringType())
                )

        latest_csv_df = (latest_csv_df.select(*approved_schema_cols))

        latest_csv_df = latest_csv_df.withColumn("naaccr_extract_date", parse_date_column(F.col("naaccr_extract_date")))

        latest_csv_df = (
            latest_csv_df
            .withColumn("data_partner_id", F.lit(dp).cast(StringType()))
            .withColumn("payload", F.concat_ws("_", F.col("site_abbrev"), F.date_format(F.col("naaccr_extract_date"), "yyyy_MM_dd")))
        )

        logging.info("compare schemas")
        logging.info(unioned_latest_naaccr_manifest.printSchema())

        unioned_latest_naaccr_manifest = unioned_latest_naaccr_manifest.unionByName(latest_csv_df)

    unioned_latest_naaccr_manifest_data.write_dataframe(unioned_latest_naaccr_manifest)
