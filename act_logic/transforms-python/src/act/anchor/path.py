"""
This is a very precisely created file, do not change it. It was created to trick Foundry Templates into giving us the
path of the root folder of the deployed template. In generate-anchor.py, we use the anchor path defined in path.py to
create a dummy anchor dataset at the root of the project. Then when a new instance of the template is deployed, this
anchor path is automatically replaced with the path of the anchor dataset in the deployed template. Then to get the
root, we simply remove the name "anchor". Finally, we can use this root path in the rest of the repo. Doing this
allowed us to massively de-duplicate repeated code, in some steps reducing the number of lines of code by more than 90%.
"""

anchor = "ri.foundry.main.dataset.92760b59-b443-42fd-bd7a-17f31dd55912"
root = anchor[:-len("anchor")]
transform = root + "transform/"
metadata = root + "metadata/"
union_staging = root + "union_staging/"

input_zip = "ri.foundry.main.dataset.fed64f44-a6be-408e-b28e-08c630dece60"
site_id = 'ri.foundry.main.dataset.3ec1824c-3a28-4c82-89e1-2bedf1ae21ca'
all_ids = "ri.foundry.main.dataset.139c5cf1-475f-4055-b90c-2a0e699808b0"
mapping = "ri.foundry.main.dataset.b115f62a-07f6-4c75-8681-ee525923206a"
vocab = "ri.foundry.main.dataset.44f389c6-f4e4-4a01-9331-a05f5d2b6de7"
concept = "ri.foundry.main.dataset.82451eb6-4185-4ff6-9d0b-5ad33d89f3f4"

mapping_overrides = "ri.foundry.main.dataset.a43d3ec0-4bba-4af0-99f6-e8d1f0f3da8c"
