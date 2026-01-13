from transforms.api import transform, Input, Output
import tempfile
import shutil
import zipfile


@transform(
    processed=Output("/UNITE/Data Ingestion & OMOP Mapping/raw_data/TriNetX/Site 264/site_264_trinetx_raw_unzipped"),
    zip_file=Input("/UNITE/Data Ingestion & OMOP Mapping/raw_data/TriNetX/Site 264/site_264_trinetx_raw_zipped"),
)
def unzip(processed, zip_file):
    fs = zip_file.filesystem()

    # Grab name of zipped file, which should be the only file in the dataset
    fs_generator = fs.ls(regex=".*uab_trinetx.*\.zip")
    zip_file_status = next(fs_generator)
    zip_filename = zip_file_status.path

    # Create a temp file to pass to zip library
    with tempfile.NamedTemporaryFile() as t:
        # Copy contents of file from Foundry into temp file
        with fs.open(zip_filename, 'rb') as f:
            shutil.copyfileobj(f, t)
            t.flush()

        z = zipfile.ZipFile(t.name)
        # For each file in the zip, unzip and add it to output dataset
        for filename in z.namelist():
            with processed.filesystem().open(filename, 'wb') as out:
                input_file = z.open(filename)
                CHUNK_SIZE = 100 * 1024 * 1024  # Read and write 100 MB chunks
                data = input_file.read(CHUNK_SIZE)
                while data:
                    out.write(data)
                    data = input_file.read(CHUNK_SIZE)
