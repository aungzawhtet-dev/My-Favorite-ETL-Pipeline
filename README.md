# My-Favorite-Scripts
This is my favorite data pipeline, an Apache Airflow–orchestrated ETL pipeline that extracts data from MongoDB, transforms it using Pandas, and loads it into PostgreSQL. I designed it with clean task separation (extract, transform, load), environment-based configuration for security, and clear data normalization to handle semi-structured JSON fields.

I particularly like this pipeline because it demonstrates practical data engineering fundamentals: working with NoSQL to relational data, schema alignment, anonymized and reusable design, and production-ready practices such as environment variables, idempotent table creation, and clear DAG structure. It reflects how I approach building maintainable, readable pipelines that can be extended with incremental loading, monitoring, and scaling when needed.

