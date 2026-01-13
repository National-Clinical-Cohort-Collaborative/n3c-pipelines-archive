from ..repo_path import repo_path
import logging
this_repo_path = repo_path
root = "/".join(list(this_repo_path.split('/')[0:-2]))
root_project = "/".join(list(this_repo_path.split('/')[0:3]))
logging.warn(repo_path)
logging.warn(root)
logging.warn(root_project)
transform = root_project + "/datasets/"