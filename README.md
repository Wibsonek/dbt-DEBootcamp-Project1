# Project description

The project focused on building a data pipeline using publicly available CSV data from a Brazilian e-commerce platform. The dataset included product, sales, customer, and transaction information.

The data was initially loaded into a locally created SQLite database. From there, a DuckDB instance was used to import the SQLite tables. Data transformation and modeling were carried out using DBT, following a layered approach (staging, intermediate, marts). The resulting tables served as the basis for two dashboards developed in Power BI.

All pipeline tasks, including data loading, transformation, and table creation, were orchestrated and scheduled using Apache Airflow.

# Project solution Schema

![MainSchema](https://github.com/user-attachments/assets/90b843db-66e2-4a46-b2ec-cdb851e51206)


To allow PowerBI to connect to DuckDB I installed the MotherDuck connector

## DBT

I used DBT as the tool for the Dashboard data creation.
DBT builds two tables:

- mart_customer_data.sql - presents the customer related data
- mart_seller_data.sql - presents the seller related data

### DBT Schema

![DBTSchema](https://github.com/user-attachments/assets/e841a6d9-3c1b-437d-aad2-a36587df0e9c)

## Airflow

![AirflowSchema](https://github.com/user-attachments/assets/58db6abc-816e-4e30-b3a2-c965f2c81e78)

My Solution uses airflow for the task orchestration.

### Task description

- SQLite snapshot
    - Snapshot creation of the SQLite DB and saving it to a specified folder
- Delete SQLite tables
    - Delete all tables inside the SQLite DB
- Recreate SQLite Tables from CSV-Files
    - Creation of new Tables in the SQLite DB, from CSV-files
- Load SQLite to DuckDB
    - Loading of the SQLite DB Tables to to DuckDB
- DBT Seed
    - Seed creation in DBT from CSV-File
- DBT Run
    - Creation of the final tables used in Power BI for the Dashboards

### Airflow Task schema of the DAG


## PowerBI

![PowerBI_1](https://github.com/user-attachments/assets/c683dc37-086d-42f8-b864-edeed05829c2)

![PowerBI_2](https://github.com/user-attachments/assets/bc9a71b7-16ef-412e-b153-837424605555)

![PowerBI_3](https://github.com/user-attachments/assets/f4a1df62-3cd0-499e-b1f7-3fae61bc9fe9)


# Project Summary

This project involved building a data engineering pipeline from scratch, with a focus on practical application and tool integration. The main objective was to understand and implement the core components of a modern data workflow using open-source technologies.

**Tools and Technologies Used:**

- **WSL (Windows Subsystem for Linux):** Used as the base environment for running Linux-based tools on a Windows system. Enabled smooth integration of DBT and Airflow.
- **Airflow:** Set up one DAG consisting of multiple tasks. Configuration was carried out manually. Faced and resolved installation and environment-related issues during setup on WSL.
- **DBT (Data Build Tool):** First practical use after prior theoretical exposure. Implemented standard three-layer structure (staging, intermediate, marts). Used DBT seeds, snapshots, and configured `profiles.yml`. Initial setup required resolving dependency conflicts.
- **DuckDB:** Used for the first time. Integrated with Power BI using the DuckDB connector to allow for analytical access via a lightweight columnar database.
- **SQLite:** Created and populated a local SQLite database from CSV files. Defined table schemas manually, including table deletion and recreation workflows.
- **Python VENV:** Used Python’s native virtual environments for dependency management, instead of Conda which was previously used.

**Outcome:**

The pipeline was successfully implemented using the listed tools. The project served to build foundational knowledge in data orchestration, transformation, and environment management using standard open-source solutions.
