from transforms.api import configure
from transforms.api import transform, Input, Output, Markings
from myproject.datasets import utils


def transform_generator(domains):
    transforms = []

    for domain in domains:
        @configure(profile=['NUM_EXECUTORS_16'])
        @transform(
            out=Output("/UNITE/Safe Harbor/transform/{domain}".format(domain=domain)),
            df=Input(
                "/UNITE/Safe Harbor/transform_staging/{domain}".format(domain=domain),
                stop_propagating=Markings(
                    [
                        "2f2690b6-e4be-4873-852d-c2f1a6a61740",
                        "032e4f9b-276a-48dc-9fb8-0557136ffe47",
                    ],
                    on_branches=utils.release_branches)
            )
        )
        def compute_function(out, df, domain=domain):
            out.write_dataframe(df.dataframe(), output_format='parquet', options={'noho': "true"})

        transforms.append(compute_function)

    return transforms


TRANSFORMS = transform_generator(utils.DOMAINS + utils.DATASETS_TO_RELEASE)
