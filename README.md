Possible Lake House Solution

           
             [Data Sources]  Possibly SQL, MySQL
                  |
               (Ingest)
                  ↓
          ┌──────────────────┐
          │   Object Storage │ ← S3, ADLS, GCS
          └──────────────────┘
                  ↓
          [Parquet/ORC files]
                  ↓
       [Lakehouse Table Format] ← Delta Lake / Iceberg / Hudi
                  ↓
        [Catalog + SQL Engine] ← Glue, Unity, Hive
                  ↓
       [Analytics, ML, BI, ETL]


       Additional Notes: must be logged on to UHMC Network to run this application successfully
