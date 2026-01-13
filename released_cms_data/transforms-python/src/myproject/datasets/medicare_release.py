from transforms.api import configure
from transforms.api import transform, Input, Output, Markings
from myproject.datasets import utils


def transform_generator(datasets):
    transforms = []

    for dataset in datasets:
        inputName = dataset

        if dataset == "cms_person_id_to_n3c_person_id":
            inputName = "staged_cms_mapping_table"
        
        @configure(profile=['NUM_EXECUTORS_32'])
        @transform(
            out=Output("/UNITE/[PPRL] Centers for Medicare & Medicaid Services (CMS) Release/datasets/Medicare Datasets - OMOP format/{dataset}".format(dataset=dataset.lower())),
            df=Input("/UNITE/[PPRL] CMS Data & Repository/pipeline/staging/{inputName}".format(inputName=inputName.lower()),
                     stop_propagating=Markings(
                        ["39cd44f0-da82-467b-b8f7-7ff5af27cf22"],
                        on_branches=utils.release_branches)
                     )
        )
        def compute_function(out, df, dataset=dataset):
            out.write_dataframe(df.dataframe(), output_format='parquet')

        transforms.append(compute_function)

    return transforms


TRANSFORMS = transform_generator(utils.MEDICARE_DATASETS_TO_RELEASE)
