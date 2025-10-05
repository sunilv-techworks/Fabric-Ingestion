# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "63da7895-db51-4ecb-b43f-6761395b79ad",
# META       "default_lakehouse_name": "Bronze",
# META       "default_lakehouse_workspace_id": "64abe8bb-34b3-4d9a-b901-f47a9eb3359a",
# META       "known_lakehouses": [
# META         {
# META           "id": "63da7895-db51-4ecb-b43f-6761395b79ad"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import os
from pathlib import Path

# Create folder structure
folders = [
    "/lakehouse/default/Files/d365/delta",
    "/lakehouse/default/Files/d365/csv"
]

# Create the folders
for folder in folders:
    folder_path = Path(folder)
    folder_path.mkdir(parents=True, exist_ok=True)
    print(f"Created folder: {folder}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
