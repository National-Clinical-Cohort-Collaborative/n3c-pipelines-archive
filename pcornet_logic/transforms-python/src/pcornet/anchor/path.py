"""

This is a very precisely created file, take care if changing it.

It was created to trick Marketplace into giving us the path of the root folder of the deployed Installation. 

In the dummy file 'repo_path.py', we temporarily save the path of the source repo to allow accessing it as a variable.
This is derived from several lines of code added to build.gradle (project level) that trick Marketplace into not 
recognizing it as a random path outside of transform I/O that would otherwise get <redacted>. 

Then when a new instance of the Marketplace is deployed, this path is automatically replaced with the path in the 
installed product. Finally, to get the root, we simply remove the final repo name, and we can use this root path 
in the rest of the repo. Doing this allowed us to de-duplicate repeated code, and allow highly shared datasets or paths
to be set in a single location.

"""

from ..repo_path import repo_path
import logging

# derive the current installation path from the gradle.properties via gradle task
this_repo_path = repo_path
root = "/".join(list(this_repo_path.split('/')[0:5]))
root_project = "/".join(list(this_repo_path.split('/')[0:3]))
logging.warn(repo_path)
logging.warn(root)
logging.warn(root_project)

transform = root + "/transform/"
metadata = root + "/metadata/"
union_staging = root + "/union_staging/"

input_zip = "ri.foundry.main.dataset.d0a8a7ff-6b5f-4982-a2a6-bec3ac34da8a"
site_id = "ri.foundry.main.dataset.351cb4fa-1ed6-4628-8512-463c9184fd13"
all_ids = "ri.foundry.main.dataset.3332ae36-a617-4ec4-bfde-b4d5a3cc6fbd"
mapping = "ri.foundry.main.dataset.d92383f5-ee45-404a-9f12-a527b14f5bef"
vocab = "ri.foundry.main.dataset.0e1acd60-6eeb-49e1-9189-1b8a6221ac29"
concept = "ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772"

covid_test_loinc_map = "ri.foundry.main.dataset.c3aebef7-8250-47f0-be8b-d0f1fb46d17a"
