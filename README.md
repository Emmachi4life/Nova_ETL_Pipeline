# Nova Cloud ELT Modernization — GCS to BigQuery Pipeline

An end-to-end ELT (Extract, Load, Transform) data pipeline built with Apache Airflow, Google Cloud Storage, and BigQuery, processing over 1 million global health records into country-specific, access-controlled analytical views.

## Business Problem

A global organization receives a single CSV file containing health statistics for every country. Two requirements drive the design:

- Each country's data must be isolated — a country's health ministry should only be able to access its own records, not the global file.
- Analysts need a fast way to identify diseases for which no vaccine or treatment is currently available.

Analyzing a single 1M+ row CSV directly is inefficient and doesn't support this kind of access control, so the data needs to be loaded into a cloud data warehouse and split into country-level, purpose-built views.

## Architecture

```
Global CSV File
      │
      ▼
Google Cloud Storage (landing zone)
      │
      ▼
Apache Airflow (orchestration)
      │
      ▼
BigQuery Staging Dataset (raw load, preserved for auditability)
      │
      ▼
BigQuery Transform Dataset (country-specific tables)
      │
      ▼
BigQuery Reporting Dataset (country-specific views, filtered by vaccine availability)
```

## Tools & Technologies

- **Google Cloud Storage (GCS)** — centralized landing zone for the raw CSV
- **BigQuery** — serverless data warehouse for staging, transformation, and reporting layers
- **Apache Airflow** — workflow orchestration, scheduling, and monitoring
- **Python** — DAG definitions using the Airflow Google provider operators

## Pipeline Steps

1. **`check_file_exists`** (`GCSObjectExistenceSensor`) — confirms the source CSV has landed in GCS before proceeding.
2. **`load_csv_to_bq`** (`GCSToBigQueryOperator`) — loads the raw CSV into a BigQuery staging table, with schema autodetection.
3. **`create_table_<country>`** (`BigQueryInsertJobOperator`, x7) — creates a country-specific table in the transform dataset for each target country.
4. **`create_view_<country>_table`** (`BigQueryInsertJobOperator`, x7) — creates a reporting view per country, filtered to diseases with no available vaccine/treatment, exposing only the columns relevant to business users (year, disease name, category, prevalence rate, incidence rate).
5. **`success_task`** — marks the pipeline complete once all tables and views are built.

## Sample Output

Query against one of the generated reporting views:

```sql
SELECT * FROM `your-project-id.reporting_dataset.usa_view` LIMIT 10;
```

| year | disease_name         | disease_category | prevalence_rate | incidence_rate |
|------|----------------------|-------------------|------------------|-----------------|
| 2007 | Parkinson's Disease  | Autoimmune        | 18.44            | 6.39            |
| 2023 | Leprosy              | Autoimmune        | 3.05             | 9.09            |
| 2003 | COVID-19             | Autoimmune        | 3.55             | 9.49            |

## Screenshots

See `/screenshots` for:
- Successful Airflow DAG run (Graph view, all tasks green)
- BigQuery query output confirming country-level filtered data

## How to Run

1. Place the source CSV in your GCS bucket.
2. Configure an Airflow connection (`google_cloud_default`) with appropriate GCS read and BigQuery edit permissions.
3. Ensure the staging, transform, and reporting BigQuery datasets exist in your project.
4. Deploy `dags/load_and_transform_view.py` to your Airflow DAGs folder.
5. Unpause and trigger the `load_and_transform_view` DAG from the Airflow UI or CLI.

## Notes

This project was built as a hands-on case study in cloud-native ELT architecture, covering GCS as a data lake, BigQuery as a serverless warehouse, and Airflow for orchestration and dependency management across a multi-branch DAG (sensor → load → 7 parallel table/view creation branches → completion marker).
