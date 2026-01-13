'''

This is a very precisely created file, take care if changing it.

It was created to trick Marketplace into giving us the path of the root folder of the deployed Installation.

In the dummy file 'repo_path.py', we temporarily save the path of the source repo to allow accessing it as a variable.

This is derived from several lines of code added to build.gradle (project level) that trick Marketplace into not
recognizing it as a random path outside of transform I/O that would otherwise get <redacted>.

Then when a new instance of the Marketplace is deployed, this path is automatically replaced with the path in the
installed product. Finally, to get the root, we simply remove the final repo name, and we can use this root path
in the rest of the repo. Doing this allowed us to massively de-duplicate repeated code, in some steps reducing
the number of lines of code by more than 90%.

'''
from ..repo_path import repo_path
import logging

this_repo_path = repo_path
root = "/".join(list(this_repo_path.split('/')[0:-2]))
root_project = "/".join(list(this_repo_path.split('/')[0:3]))

'''

ACCESSING DYNAMIC PATHS

In gradle.properties we find a variable that dynamically obtains the repo path
/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/logic/ACT Logic
The gradle task in build.gradle saves this repo path to a file called repo_path

We therefore expect root to be /UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411,
which we then add subfolders to below

root_project is therefore /UNITE/Data Ingestion & OMOP Mapping
In order to be more robust to folder changes and compatible with Marketplace this is not used for
defining input. We print it just for logging purposes

'''

logging.warn(repo_path)
logging.warn(root)
logging.warn(root_project)

transform = root + "/transform/"
metadata = root + "/metadata/"
union_staging = root + "/union_staging/"


input_zip = "ri.foundry.main.dataset.33f9fc28-0d69-48a6-853d-c1ccf5635b34"
site_id = "ri.foundry.main.dataset.c470080d-08fd-4761-a713-1948696db2b1"
all_ids = "ri.foundry.main.dataset.4d4cf17b-9dfb-48e8-bb19-4f62960b75ec"
vocab = "ri.foundry.main.dataset.0e1acd60-6eeb-49e1-9189-1b8a6221ac29"
concept = "ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772"
concept_relationship = "ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71"
