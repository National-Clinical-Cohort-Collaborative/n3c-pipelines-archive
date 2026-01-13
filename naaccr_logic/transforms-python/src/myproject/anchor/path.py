"""
This is a very precisely created file, take care if changing it.

In the dummy file 'repo_path.py', we temporarily save the path of the source repo to allow accessing it as a variable.
This is derived from several lines of code added to build.gradle (project level).

To get the root, we simply remove the final repo name, and we can use this root path 
in the rest of the repo. Doing this allowed us to de-duplicate repeated code, and allow highly shared datasets or paths
to be set in a single location.
"""

from ..repo_path import repo_path

# derive the current installation path from the gradle.properties via gradle task
this_repo_path = repo_path
root = "/".join(list(this_repo_path.split("/")[0:5]))
root_project = "/".join(list(this_repo_path.split("/")[0:3]))


transform = root + "/transform/"
