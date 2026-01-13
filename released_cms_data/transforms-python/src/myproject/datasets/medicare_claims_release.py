from transforms.api import configure
from transforms.api import transform, Input, Output, Markings
from myproject.datasets import utils


def transform_generator(datasets):
    transforms = []

    for dataset in datasets:
        @configure(profile=['NUM_EXECUTORS_32'])
        @transform(
            out=Output("/UNITE/[PPRL] Centers for Medicare & Medicaid Services (CMS) Release/datasets/Medicare Datasets - Claims format/{dataset}".format(dataset=dataset.lower())),
            df=Input("/UNITE/[PPRL] CMS Data & Repository/raw/staging/{dataset}".format(dataset=dataset.lower()),
                     stop_propagating=Markings(
                        ["39cd44f0-da82-467b-b8f7-7ff5af27cf22", "af9a47f1-9af3-4a1b-bdf0-a108f077475e"],
                        on_branches=utils.release_branches)
                     )
        )
        def compute_function(out, df, dataset=dataset):
            out.write_dataframe(df.dataframe(), output_format='parquet')

        transforms.append(compute_function)

    return transforms


TRANSFORMS = transform_generator(utils.MEDICARE_CLAIMS_DATASETS_TO_RELEASE)
