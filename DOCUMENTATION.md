## ENVIRONMENT VARIABLES CODE

```yaml
# ---------- POSTGRES ----------
POSTGRES_USER= "xxxxx"
POSTGRES_PASSWORD= "xxxxx"
POSTGRES_DB= "xxxxx"


# ---------- AIRFLOW ADMIN ----------
AIRFLOW_ADMIN_USER= "xxxxx"
AIRFLOW_ADMIN_FIRSTNAME= "xxxxx"
AIRFLOW_ADMIN_LASTNAME= "xxxxx"
AIRFLOW_ADMIN_EMAIL= "xxxxx@example.com"
AIRFLOW_ADMIN_PASSWORD= "xxxxx"
```

## DATA SOURCE 

### API
```yaml
https://opensky-network.org/api/states/all
```

### API DOCUMENTATION
```yaml
https://openskynetwork.github.io/opensky-api/rest.html
```


## AIRFLOW PIPELINE DAG STRUCTURE FOR BRONZE 

```python
import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_layer import run_bronze_ingestion

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay" : timedelta(minutes=5),
}

with DAG(
    dag_id="flights_ops_medallion_pipe",
    default_args=default_args,
    start_date=datetime(2025, 12, 10),
    schedule_interval="*/30 * * * *",
    catchup=False,
) as dag:

    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=run_bronze_ingestion,
    )

```

## SQL SCRIPT FOR DATABASE, SCHEMA AND TABLE CREATION

### DATABASE
```sql
CREATE DATABASE IF NOT EXISTS FLIGHTS;
```

### SCHEMA
```sql
CREATE SCHEMA IF NOT EXISTS FLIGHTS_SCHEMA;
```

### TABLE
```sql
USE SCHEMA FLIGHTS_SCHEMA;

CREATE TABLE FLIGHT_TABLE (
    window_start TIMESTAMP,
    origin_country TEXT,
    total_flights INT,
    avg_velocity FLOAT,
    on_ground INT,
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, origin_country)
);

```


## SNOWFLAKE STREAMLIT SCRIPT EXAMPLE

```sql
# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Page Title
st.title("✈️ Flight Analytics Dashboard")

# Get Snowflake session
session = get_active_session()

# Query your Snowflake table
query = """
SELECT
    origin_country,
    total_flights,
    avg_velocity,
    on_ground
FROM FLIGHT_TABLE
"""

# Execute query
df = session.sql(query).to_pandas()

# Display Data
st.subheader("Flight Dataset")
st.dataframe(df, use_container_width=True)

# Bar Chart - Total Flights by Country
st.subheader("Total Flights by Country")
st.bar_chart(
    data=df,
    x="ORIGIN_COUNTRY",
    y="TOTAL_FLIGHTS"
)

# Line Chart - Average Velocity
st.subheader("Average Velocity")
st.line_chart(
    data=df,
    x="ORIGIN_COUNTRY",
    y="AVG_VELOCITY"
)

# Bar Chart - Aircraft on Ground
st.subheader("Aircraft on Ground")
st.bar_chart(
    data=df,
    x="ORIGIN_COUNTRY",
    y="ON_GROUND"
)
```
