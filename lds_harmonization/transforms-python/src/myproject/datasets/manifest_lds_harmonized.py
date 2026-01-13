from transforms.api import transform_df, Input, Output, configure

LINTER_APPLIED_PROFILES = [
    "KUBERNETES_NO_EXECUTORS",
    "DRIVER_MEMORY_MEDIUM",
]


@configure(profile=LINTER_APPLIED_PROFILES)
@transform_df(
    Output("ri.foundry.main.dataset.f6ca51b5-458f-4232-ae00-355931b45e36"),
    my_input=Input("ri.foundry.main.dataset.b5caf32e-d26c-4505-b7ad-020f3331c5a3"),
)
def my_compute_function(my_input):
    if "contact_email" in my_input.columns:
        my_input = my_input.drop("contact_email")
    return my_input
