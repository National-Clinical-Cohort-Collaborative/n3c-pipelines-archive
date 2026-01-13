'''

The get_newest_payload() and extract_filenames() functions are not great and should be re-written someday.
- tschwab

I have updated get_newest_payload() and added a new function to verify the most recent file is being unzipped
- kbradwell

'''

import os
import tempfile
import shutil
from zipfile import ZipFile
from pyspark.sql import functions as F, types as T
from pyspark.sql import DataFrame


# Read and write 100 MB chunks
CHUNK_SIZE = 100 * 1024 * 1024


def unzipLatest(foundryZip, regex, foundryOutput):
    fs = foundryZip.filesystem()
    files_df = fs.files(regex=regex)
    newest_file = get_newest_payload(files_df)
    unzip(foundryZip, foundryOutput, newest_file)


def unzip(foundryInput, foundryOutput, filename):
    inputFS = foundryInput.filesystem()
    outputFS = foundryOutput.filesystem()

    # Create a temp file to pass to zip library, because it needs to be able to .seek()
    with tempfile.NamedTemporaryFile() as temp:
        # Copy contents of file from Foundry into temp file
        with inputFS.open(filename, 'rb') as newest:
            shutil.copyfileobj(newest, temp)
            temp.flush()

        # For each file in the zip, unzip and add it to output dataset
        zipObj = ZipFile(temp.name)
        for filename in zipObj.namelist():
            with outputFS.open(filename, 'wb') as out:
                input_file = zipObj.open(filename)
                data = input_file.read(CHUNK_SIZE)
                while data:
                    out.write(data)
                    data = input_file.read(CHUNK_SIZE)


def extract_filenames(ctx, zip_file, regex, payload_filename):
    # Get the paths and determine the newest one
    fs = zip_file.filesystem()
    files_df = fs.files(regex=regex)
    newest_file = get_newest_payload(files_df)
    files_df = files_df.withColumn("newest_payload", F.when(F.col("path") == newest_file, F.lit(True)).otherwise(F.lit(False)))

    # Get just the filename, not the path
    get_basename = F.udf(lambda x: os.path.basename(x), T.StringType())
    ctx.spark_session.udf.register("get_basename", get_basename)
    files_df = files_df.withColumn("payload", get_basename(F.col("path")))

    # Select the needed data and repartition to a single file
    result = files_df.select("payload", "newest_payload")
    result = result.coalesce(1)

    # Write the result
    payload_filename.write_dataframe(result)


# Given a filesystem dataframe containing payload zip files, return payload information for the files
def get_info_on_payloads(files_df):

    # Extract payload date and remove underscores
    files_df = files_df.withColumn(
        "processed_date",
        F.regexp_replace(F.regexp_extract(F.col("path"), "(?i)(\\d{8}|\\d{4}_\\d{2}_\\d{2})(.*)(\\.zip)$", 1), "_", "")
    )

    # Handle either yyyyMMDD or MMDDyyyy format
    files_df = files_df.withColumn(
        "processed_date",
        F.when(F.regexp_extract(F.col("processed_date"), "(202.|203.)\\d{4}", 1) == F.lit(""), F.concat(F.col("processed_date")\
                .substr(5, 4), F.col("processed_date").substr(1,4)))\
        .otherwise(F.col("processed_date"))
    )

    # If site submitted multiple files on the same day (e.g. "payload_20201015.zip" and "payload_20201015_1.zip", extract the increment
    files_df = files_df.withColumn("same_date_increment", F.regexp_extract(F.col("path"), "(?i)(\\d{8}|\\d{4}_\\d{2}_\\d{2})(.*)(\\.zip)$", 2))

    # Sort by processed payload date, then by increment, then by modified time and grab the most recent payload
    files_df = files_df.orderBy(["processed_date", "same_date_increment", "modified"], ascending=False)

    return files_df


# Given a filesystem dataframe containing payload zip files, return the most recent payload name
def get_newest_payload(files_df):
    files_info = get_info_on_payloads(files_df)
    newest_file_path = files_info.head().path

    return newest_file_path


# Given a dataframe containing payload zip file names, independently verify the most recent payload name is the latest
def verify_newest_payload_is_latest(ctx, zip_file, regex, qc_filename):
    # Get the paths and determine the newest one
    fs = zip_file.filesystem()
    files_df = fs.files(regex=regex)

    files_info = get_info_on_payloads(files_df)

    # Check if the date format in the given column is valid
    def check_date_format(df: DataFrame, column_name: str, date_format: str) -> DataFrame:
        return df.withColumn("is_valid_date", F.to_date(F.col(column_name), date_format).isNotNull())

    newest_file = files_info.head().processed_date
    newest_file_path = get_newest_payload(files_df)

    files_info = check_date_format(files_info, "processed_date", "yyyyMMdd")
    files_info = files_info.withColumn("newest_payload", F.lit(newest_file)) \
                           .withColumn("newest_payload_path", F.lit(newest_file_path))


    qc_dataset = files_info.groupBy("newest_payload", "newest_payload_path").agg(
        F.collect_list("processed_date").alias("all_processed_dates"),
        F.max("processed_date").alias("latest_date"),
        F.collect_list("is_valid_date").alias("all_date_validations")
    )

    # Check if the latest date is verified and there are no invalid dates
    qc_dataset = qc_dataset.withColumn(
        "latest_date_check_passed",
        F.when(
            (~F.array_contains(F.col("all_date_validations"), False)) &
            (F.col("latest_date") == F.col("newest_payload")),
            True
        ).otherwise(False)
    )

    # Write the result
    qc_filename.write_dataframe(qc_dataset)
