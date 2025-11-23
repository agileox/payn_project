# payn_project
## Project Desc:
This is a project to learn how to flatten JSON file with 1.2 Million rows, process it and transform to make sure PII data has been masked properly and ingest the data into local postgresql for next dashboarding activities.

## Method:
- Read and flatten JSON file
- Clean names by split first name and last name
- Normalize gender
- Mask credit card (keep last 4 digits), address street and address zip for keeping PII data safe
- Push final dataFrame into PostgreSQL

## Tools
+ Fedora 43
+ PySpark 3.11 (spark)
+ Free Jupyter Notebook
+ PostgreSQL 18
+ Superset
