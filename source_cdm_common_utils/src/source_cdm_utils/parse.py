import csv
from pyspark.sql import Row, functions as F, types as T
import logging

header_col = "__is_header__"
errorCols = ["row_number", "error_type", "error_details"]
ErrorRow = Row(header_col, *errorCols)

logger = logging.getLogger()


def required_parse(filename_input, payload_input, domain, clean_output, error_output, all_cols, required_cols):
    parse(filename_input, payload_input, domain, clean_output, error_output, all_cols, required_cols)


def optional_parse(filename_input, payload_input, domain, clean_output, error_output, all_cols, required_cols):
    parse(filename_input, payload_input, domain, clean_output, error_output, all_cols, required_cols)


def cached_parse(filename_input, payload_input, domain, clean_output, error_output, all_cols, required_cols):
    regexPattern = "(?i).*" + domain + "\\.csv"
    fs = payload_input.filesystem()
    files_df = fs.files(regex=regexPattern)

    if files_df.count() > 0:
        parse(filename_input, payload_input, domain, clean_output, error_output, all_cols, required_cols)
    else:
        clean_output.abort()
        error_output.abort()


def metadata_parse(payload_input, filename, clean_output, error_output, all_cols, required_cols):
    regex = "(?i).*" + filename + "\\.csv"
    clean_df, error_df = parse_csv(payload_input, regex, all_cols, required_cols)

    clean_output.write_dataframe(clean_df)
    error_output.write_dataframe(error_df)


# API above, functionality below


def parse(filename_input, payload_input, domain, clean_output, error_output, all_cols, required_cols):
    regex = "(?i).*" + domain + "\\.csv"
    clean_df, error_df = parse_csv(payload_input, regex, all_cols, required_cols)

    payload_filename = filename_input.dataframe().where(F.col("newest_payload") == True).head().payload  # noqa
    clean_df = clean_df.withColumn("payload", F.lit(payload_filename))

    clean_output.write_dataframe(clean_df)
    error_output.write_dataframe(error_df)


def parse_csv(payload_input, regex, all_cols, required_cols):
    # Get the correct file from the unzipped payload
    files_df = payload_input.filesystem().files(regex=regex)

    # Parse the CSV into clean rows and error rows
    parser = CsvParser(payload_input, all_cols, required_cols)
    result_rdd = files_df.rdd.flatMap(parser)

    # The idea behind caching here was that it would make sure Spark didn't parse the CSV twice, once for the clean
    # rows and once for the error rows. However some CSVs are greater than 100G, and this line always caused an OOM
    # for those. After removing this line, we could parse the 100G even with a small profile.
    # result_rdd = result_rdd.cache()

    # Separate into good and bad rows
    if isinstance(all_cols, dict):
        clean_df = rddToDf(result_rdd, "clean", T.StructType([T.StructField(col, T.StringType()) for col in all_cols]))
        # Apply the .cast() method to each column to set the appropriate data type
        for col, dtype in all_cols.items():
            clean_df = clean_df.withColumn(col, F.col(col).cast(dtype))
    else:
        clean_df = rddToDf(result_rdd, "clean", T.StructType([T.StructField(col, T.StringType()) for col in all_cols]))
    error_df = rddToDf(result_rdd, "error", T.StructType([T.StructField(col, T.StringType()) for col in errorCols]))

    # Return
    return (clean_df, error_df)


def parse_csv_latest(payload_input, regex, schema, out, ctx):

    # Get relevant csv files from the payload
    files_df = payload_input.filesystem().files(regex=regex)

    # Parse the LATEST CSV file into a dataset
    newest_file = get_newest_csv_payload(files_df)
    logging.info('newest file')
    logging.info(newest_file)

    hadoop_path = payload_input.filesystem().hadoop_path
    logging.info('hadoop path')
    logging.info(hadoop_path)

    # Create the full path for the newest file
    file_path = f"{hadoop_path}/{newest_file}"
    logging.info('file path')
    logging.info(file_path)

    # Read the CSV file into a DataFrame
    df = (
        ctx.spark_session.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .schema(schema)
        .load(file_path)
    )

    # Write the DataFrame to the output dataset
    out.write_dataframe(df)


def rddToDf(inputRdd, rowType, schema):
    # Filter by type and get the rows
    resultRdd = inputRdd.filter(lambda row: row[0] == rowType)
    resultRdd = resultRdd.map(lambda row: row[1])

    if resultRdd.isEmpty():
        # Convert to DF using the given schema
        resultDf = resultRdd.toDF(schema)
    else:
        # Convert to DF using the RDD Rows' schema
        resultDf = resultRdd.toDF()

        # Drop the header row - get only the data rows. This is needed to ensure we get the right schema.
        resultDf = resultDf.filter(resultDf[header_col] == False).drop(header_col)

    return resultDf


class CsvParser():
    def __init__(self, rawInput, all_cols, required_cols):
        self.rawInput = rawInput
        self.all_cols = all_cols
        self.required_cols = required_cols

    def __call__(self, csvFilePath):
        try:
            dialect = self.determineDialect(csvFilePath)
        except Exception as e:
            yield ("error", ErrorRow(False, "0", "Could not determine the CSV dialect", repr(e)))
            return

        with self.rawInput.filesystem().open(csvFilePath.path, errors='ignore') as csvFile:
            csvReader = csv.reader(csvFile, dialect=dialect)
            yield from self.parseHeader(csvReader)
            yield from self.parseFile(csvReader)

    def determineDialect(self, csvFilePath):
        with self.rawInput.filesystem().open(csvFilePath.path, errors='ignore') as csvFile:
            dialect = csv.Sniffer().sniff(csvFile.readline(), delimiters=",|")

        return dialect

    def parseHeader(self, csvReader):
        header = next(csvReader)
        header = [x.strip().strip("\ufeff").upper() for x in header]
        header = [*filter(lambda col: col, header)]  # Remove empty headers

        self.CleanRow = Row(header_col, *header)
        self.expected_num_fields = len(header)

        yield ("clean", self.CleanRow(True, *header))
        yield ("error", ErrorRow(True, "", "", ""))

        warningDetails = {
            "all columns": self.all_cols,
            "required columns": self.required_cols,
            "header": header
        }

        # Throw warning for every column in the required schema but not in the header
        for col in self.required_cols:
            if col not in header:
                message = f"Header did not contain required column `{col}`"
                yield ("error", ErrorRow(False, "0", message, str(warningDetails)))

        # Throw warning for every column in the header but not in the schema
        for col in header:
            if col not in self.all_cols:
                message = f"Header contained unexpected extra column `{col}`"
                yield ("error", ErrorRow(False, "0", message, str(warningDetails)))

    def parseFile(self, csvReader):
        i = 0
        while True:
            i += 1

            nextError = False
            try:
                row = next(csvReader)
            except StopIteration:
                break
            except Exception as e:
                nextError = [str(i), "Unparsable row", repr(e)]

            # Hit an error parsing
            if nextError:
                yield ("error", ErrorRow(False, *nextError))

            # Properly formatted row
            elif len(row) == self.expected_num_fields:
                yield ("clean", self.CleanRow(False, *row))

            # Ignore empty rows/extra newlines
            elif not row:
                continue

            # Improperly formatted row
            else:
                message = f"Incorrect number of fields. Expected {str(self.expected_num_fields)} but found {str(len(row))}."
                yield ("error", ErrorRow(False, str(i), message, str(row)))


# Given a filesystem dataframe containing payload csv files, return payload information for the files
def get_info_on_csv_payloads(files_df):
    # Extract payload date and remove underscores
    files_df = files_df.withColumn(
        "processed_date",
        F.regexp_replace((F.regexp_replace(F.regexp_extract(F.col("path"),
        "(?i)(\\d{8}|\\d{4}_\\d{2}_\\d{2}|\\d{4}-\\d{2}-\\d{2})(.*)(\\.csv)$", 1), "_", "")),
         "-", "")
        )

    # Handle either yyyyMMDD or MMDDyyyy format
    files_df = files_df.withColumn(
        "processed_date",
        F.when(F.regexp_extract(F.col("processed_date"), "(202.|203.)\\d{4}", 1) == F.lit(""),
               F.concat(F.col("processed_date").substr(5, 4), F.col("processed_date").substr(1, 4)))
        .otherwise(F.col("processed_date"))
    )

    # Convert processed_date to date type
    files_df = files_df.withColumn(
        "processed_date",
        F.to_date(F.col("processed_date"), "yyyyMMdd")
    )

    # If site submitted multiple files on the same day (e.g. "payload_20201015.zip" and "payload_20201015_1.zip", extract the increment
    files_df = files_df.withColumn("same_date_increment", F.regexp_extract(F.col("path"), "(?i)(\\d{8}|\\d{4}_\\d{2}_\\d{2}|\\d{4}-\\d{2}-\\d{2})(.*)(\\.csv)$", 2))

    # Sort by processed payload date, then by increment, then by modified time and grab the most recent payload
    files_df = files_df.orderBy(["processed_date", "same_date_increment", "modified"], ascending=False)

    return files_df


# Given a filesystem dataframe containing payload zip files, return the most recent payload name
def get_newest_csv_payload(files_df):
    files_info = get_info_on_csv_payloads(files_df)
    newest_file_path = files_info.head().path

    return newest_file_path
