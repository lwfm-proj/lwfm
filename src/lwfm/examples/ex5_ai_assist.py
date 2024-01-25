import os
from lwfm.base import Site
# create a blank file, then create a local Site and put the file to it with 
# metadata foo=bar

# Create a blank file
file_path = "/tmp/blank_file.txt"
with open(file_path, "w") as file:
    pass

# Create a local Site
site = Site("local")

# Put the file to the Site with metadata foo=bar
site.put(file_path, metadata={"foo": "bar"})
