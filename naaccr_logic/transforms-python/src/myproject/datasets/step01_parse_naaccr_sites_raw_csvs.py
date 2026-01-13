from pyspark.sql import functions as F
from transforms.api import transform, Input, Output, configure
from source_cdm_utils.parse import get_newest_csv_payload
import logging
from pyspark.sql import types as T
from pyspark.sql.types import StringType, IntegerType, DateType, StructType, StructField
from functools import reduce

"""
OVERVIEW
This step parses the latest csv file from each NAACCR site.
The resulting tabular datasets for all sites are then unioned together, selecting
only columns that are approved for usage and fix data types using data dictionary

TO DO:
- use manifest to verify versions match for schemas used
    naaccr_manifest=Input("ri.foundry.main.dataset.553c417b-8ba1-4123-bf99-3b17c6756a46")
"""

# note that this would also work with values as "integer", "date", "string" too as in Source CDM Common Utils .cast() is used
type_mappings = {
    "digits": IntegerType(),
    "text": StringType(),
    "date": DateType(),
    "mixed": IntegerType(),
    "alpha": StringType(),
    "numeric": IntegerType(),
}
empty_dict = {}

logger = logging.getLogger()

naaccr_path = "/UNITE/N3Clinical PPRL Raw Incoming Data/NAACCR/"

# to do - replace this with lookup table of cols: site name, tenant, site ID, cancer pprl type
naaccr_files = [
    ("Site_349_NAACCR", 349),
    ("Site_179_NAACCR", 179),
    ("Site_126_NAACCR", 126),  # contains nulls for N3C_PATIENT_ID
    ("Site_220_NAACCR", 220),
]

inputs = {site_file[0]: Input(naaccr_path + site_file[0]) for site_file in naaccr_files}


@configure(profile=["EXECUTOR_MEMORY_LARGE", "NUM_EXECUTORS_8"])
@transform(
    **inputs,
    unioned_latest_naaccr_data=Output(
        "/UNITE/NAACCR Logic - RWD Pipeline - N3Clinical/datasets/AllSites_N3Clinical_NAACCR_Data"
    ),
    approved_cols_df=Input(
        "ri.foundry.main.dataset.15e186b3-cbee-4cc0-85e0-fcec6be62752"
    ),
    naaccr_data_dictionary=Input(
        "ri.foundry.main.dataset.615fb648-7f0d-40e7-b2ca-78c2ebc553a0"
    ),
    naaccr_manifest=Input("/UNITE/NAACCR Logic - RWD Pipeline - N3Clinical/datasets/NAACCR_manifest_N3Clinical"
    ),
)
def my_compute_function(
    unioned_latest_naaccr_data, naaccr_data_dictionary, approved_cols_df, naaccr_manifest, ctx, **all_dfs
):
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
        # if len(df.columns) == 1:
        #     df = (
        #         ctx.spark_session.read.format("csv")
        #         .option("header", "true")
        #         .option("delimiter", "|")
        #         .option("inferSchema", True)
        #         .load(file_path)
        #     )
        # columns_to_check = [c for c in df.columns if c != 'N3C_PATIENT_ID']

        # # Build a sum of isNotNull() for all target columns using reduce
        # non_null_expr = reduce(
        #     lambda acc, c: acc + F.col(c).isNotNull().cast("int"),
        #     columns_to_check,
        #     F.lit(0)  # Start from literal 0
        # )

        # # Keep only rows where at least one of those columns is non-null
        # df_filtered = df.filter(non_null_expr > 0)

        # Write the DataFrame to the output dataset
        # return df_filtered
        return df

    approved_cols_df = approved_cols_df.dataframe()
    approved_schema_cols = approved_cols_df.columns
    approved_schema = StructType(
        [StructField(col, StringType(), True) for col in approved_schema_cols]
    )

    unioned_latest_naaccr = ctx.spark_session.createDataFrame(
        [], approved_schema
    ).withColumn("data_partner_id", F.lit(None).cast(StringType()))

    logging.info("before loop")
    logging.info(approved_cols_df)
    for site in naaccr_files:
        source_df = all_dfs[site[0]]
        dp = site[1]
        regex = "(?i).*NAACCR.*\\.csv"
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

        latest_csv_df = latest_csv_df.select(*approved_schema_cols).withColumn(
            "data_partner_id", F.lit(dp)
        )

        logging.info("compare schemas")
        logging.info(unioned_latest_naaccr.printSchema())
        unioned_latest_naaccr = unioned_latest_naaccr.unionByName(latest_csv_df)

    # Read the data dictionary and manifest file into a DataFrame
    data_dict_df = naaccr_data_dictionary.dataframe()
    naaccr_manifest = naaccr_manifest.dataframe()

    # Join the unioned DataFrame with the data dictionary to apply the correct data types
    data_dict_df = data_dict_df.withColumnRenamed(
        "Data_Item_Number", "column_name"
    ).withColumnRenamed("Data_Type", "data_type")

    data_dict = {
        f'N{row["column_name"]}': type_mappings.get(row["data_type"], StringType())
        for row in data_dict_df.collect()
    }

    # Cast datatypes
    for col_name, data_type in data_dict.items():
        if col_name in unioned_latest_naaccr.columns:
            if isinstance(data_type, DateType):
                unioned_latest_naaccr = unioned_latest_naaccr.withColumn(col_name,
                F.when(
                    F.col(col_name).rlike("-"),
                    F.to_date(F.col(col_name), "yyyy-MM-dd")
                ).otherwise(
                    F.to_date(F.col(col_name), "yyyyMMdd")))
            else:
                unioned_latest_naaccr = unioned_latest_naaccr.withColumn(col_name, 
                F.col(col_name).cast(data_type)
                )

    filtered_df = unioned_latest_naaccr.filter(F.col("N3C_PATIENT_ID").isNotNull())
    # add payload from manifest file to naaccr data
    filtered_df = filtered_df.join(naaccr_manifest.select("data_partner_id", "payload"), on="data_partner_id", how="left")
    unioned_latest_naaccr_data.write_dataframe(filtered_df)
