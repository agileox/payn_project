# payn_project
## Project Description:
This is a project to learn how to flatten JSON file with 1.2 Million rows, process it and transform to make sure PII data has been masked properly and ingest the data into local postgresql for next dashboarding activities.

## Method:
- Read and flatten JSON file
- Clean names by split first name and last name
- Normalize gender
- Mask credit card (keep last 4 digits), address street and address zip for keeping PII data safe
- Push final dataFrame into PostgreSQL

## Tools:
+ Fedora 43
+ PySpark 3.11 (spark)
+ Free Jupyter Notebook
+ PostgreSQL 18
+ Superset

### Jupyter Notebook
![jupyter](images/jupyter_project_folder.png "project folder")
![pyspark](images/jupyter_pyspark.png "pyspark script")

## Logs & Journaling:
1. Installed jupyter notebook - successfully setup & configure Version: 7.4.7
2. Installed spark 3.5.3 - setup & configure | challenges mismatch version
3. Installed py 3.11 (spark) 
4. Start to learn to get notebook to read json (/home/agileox/Project/payn_project/data/cc_sample_transaction.json)
5. Managed to ingest the kaggel data into postgresql 18 that also has been successfully installed on my Fedora 43
6. Installed Superset to run on my Fedora 43
7. To explore to push the data into Superset
8. Start exploring on the dashboarding
