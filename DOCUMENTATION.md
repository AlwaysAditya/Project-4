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
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Flight Analytics Dashboard",
    page_icon="✈️",
    layout="wide",
)

# ============================================================
# STYLE
# ============================================================
st.markdown("""
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    .hero {
        background: linear-gradient(135deg, #1e3a8a 0%, #2563eb 100%);
        padding: 26px 32px;
        border-radius: 16px;
        margin-bottom: 22px;
        box-shadow: 0 8px 24px rgba(37, 99, 235, 0.25);
    }
    .hero h1 {
        color: white;
        font-size: 32px;
        font-weight: 800;
        margin: 0;
    }
    .hero p {
        color: rgba(255,255,255,0.85);
        margin: 6px 0 0 0;
        font-size: 15px;
    }

    .section-header {
        background: linear-gradient(90deg, #2563eb 0%, #1e40af 100%);
        padding: 10px 18px;
        border-radius: 10px;
        margin: 20px 0 12px 0;
    }
    .section-header h3 {
        color: white;
        margin: 0;
        font-size: 18px;
    }

    div[data-testid="stMetric"] {
        background-color: #111827;
        border: 1px solid #1f2937;
        padding: 16px 12px;
        border-radius: 12px;
    }
    div[data-testid="stMetricLabel"] {
        color: #9ca3af;
    }
    </style>
""", unsafe_allow_html=True)


def section_header(title: str):
    st.markdown(f'<div class="section-header"><h3>{title}</h3></div>', unsafe_allow_html=True)


# ============================================================
# HERO
# ============================================================
st.markdown("""
    <div class="hero">
        <h1>✈️ Flight Analytics Dashboard</h1>
        <p>Live flight activity by country — volume, velocity, and ground status at a glance.</p>
    </div>
""", unsafe_allow_html=True)

# ============================================================
# DATA
# ============================================================
session = get_active_session()

query = """
SELECT
    origin_country,
    total_flights,
    avg_velocity,
    on_ground
FROM FLIGHTS.FLIGHTS_SCHEMA.FLIGHT_TABLE
"""
df = session.sql(query).to_pandas()

# Snowpark can return numeric columns as Decimal/object dtype, which breaks
# Altair's quantitative (:Q) encodings and pandas aggregations. Coerce explicitly.
numeric_cols = ["TOTAL_FLIGHTS", "AVG_VELOCITY", "ON_GROUND"]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

df["ORIGIN_COUNTRY"] = df["ORIGIN_COUNTRY"].astype(str).str.strip()

# The source table has multiple rows per country, so aggregate once here
# and reuse this for every "by country" chart below.
country_agg = (
    df.groupby("ORIGIN_COUNTRY", as_index=False)
    .agg(
        TOTAL_FLIGHTS=("TOTAL_FLIGHTS", "sum"),
        AVG_VELOCITY=("AVG_VELOCITY", "mean"),
        ON_GROUND=("ON_GROUND", "sum"),
    )
)

# ============================================================
# KPI ROW
# ============================================================
section_header("📊 Key Metrics")

total_flights_sum = int(country_agg["TOTAL_FLIGHTS"].sum())
avg_velocity_overall = round(country_agg["AVG_VELOCITY"].mean(), 1)
on_ground_sum = int(country_agg["ON_GROUND"].sum())
countries_tracked = country_agg["ORIGIN_COUNTRY"].nunique()
top_country = country_agg.loc[country_agg["TOTAL_FLIGHTS"].idxmax(), "ORIGIN_COUNTRY"]

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Flights", f"{total_flights_sum:,}")
k2.metric("Avg. Velocity", f"{avg_velocity_overall} km/h")
k3.metric("Aircraft on Ground", f"{on_ground_sum:,}")
k4.metric("Countries Tracked", f"{countries_tracked}")
k5.metric("Busiest Country", top_country)

# ============================================================
# DATASET
# ============================================================
section_header("🗂️ Flight Dataset")
tab_agg, tab_raw = st.tabs(["By Country (aggregated)", "Raw Data"])
with tab_agg:
    st.dataframe(
        country_agg.sort_values("TOTAL_FLIGHTS", ascending=False),
        use_container_width=True, hide_index=True,
    )
with tab_raw:
    st.dataframe(df, use_container_width=True, hide_index=True)

# ============================================================
# CHARTS
# ============================================================
section_header("🌍 Total Flights by Country")
flights_top10 = country_agg.sort_values("TOTAL_FLIGHTS", ascending=False).head(10)
st.caption(f"Top 10 of {len(country_agg)} countries by total flights.")
fig_flights = (
    alt.Chart(flights_top10)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("ORIGIN_COUNTRY:N", sort="-y", title="Country"),
        y=alt.Y("TOTAL_FLIGHTS:Q", title="Total Flights"),
        color=alt.Color("TOTAL_FLIGHTS:Q", scale=alt.Scale(scheme="blues"), legend=None),
        tooltip=["ORIGIN_COUNTRY", "TOTAL_FLIGHTS"],
    )
    .properties(height=440)
)
st.altair_chart(fig_flights, use_container_width=True)

col1, col2 = st.columns(2)

with col1:
    section_header("⚡ Average Velocity by Country")
    velocity_top10 = country_agg.sort_values("AVG_VELOCITY", ascending=False).head(10)
    st.caption(f"Top 10 of {len(country_agg)} countries by avg. velocity.")
    line_base = alt.Chart(velocity_top10).encode(
        x=alt.X("ORIGIN_COUNTRY:N", sort="-y", title="Country"),
        y=alt.Y("AVG_VELOCITY:Q", title="Avg. Velocity (km/h)"),
        tooltip=["ORIGIN_COUNTRY", "AVG_VELOCITY"],
    )
    fig_velocity = (
        line_base.mark_line(color="#3872fb", point=alt.OverlayMarkDef(color="#3872fb"))
        .properties(height=400)
    )
    st.altair_chart(fig_velocity, use_container_width=True)

with col2:
    section_header("🛬 Aircraft on Ground")
    ground_top10 = country_agg.sort_values("ON_GROUND", ascending=False).head(10)
    st.caption(f"Top 10 of {len(country_agg)} countries by aircraft on ground.")
    fig_ground = (
        alt.Chart(ground_top10)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("ORIGIN_COUNTRY:N", sort="-y", title="Country"),
            y=alt.Y("ON_GROUND:Q", title="Aircraft on Ground"),
            color=alt.Color("ON_GROUND:Q", scale=alt.Scale(scheme="oranges"), legend=None),
            tooltip=["ORIGIN_COUNTRY", "ON_GROUND"],
        )
        .properties(height=400)
    )
    st.altair_chart(fig_ground, use_container_width=True)

# ============================================================
# SHARE OF TOTAL FLIGHTS (bonus chart) — top 10 + Others
# ============================================================
section_header("🥧 Share of Total Flights by Country")

pie_ranked = country_agg.sort_values("TOTAL_FLIGHTS", ascending=False).reset_index(drop=True)
top10 = pie_ranked.iloc[:10][["ORIGIN_COUNTRY", "TOTAL_FLIGHTS"]]
rest = pie_ranked.iloc[10:]

if len(rest) > 0:
    others_row = pd.DataFrame({
        "ORIGIN_COUNTRY": ["Others"],
        "TOTAL_FLIGHTS": [rest["TOTAL_FLIGHTS"].sum()],
    })
    pie_df = pd.concat([top10, others_row], ignore_index=True)
else:
    pie_df = top10

st.caption(f"Showing top {len(top10)} countries out of {len(pie_ranked)} total"
           + (f" — remaining {len(rest)} grouped as 'Others'." if len(rest) > 0 else "."))

fig_pie = (
    alt.Chart(pie_df)
    .mark_arc(innerRadius=90)
    .encode(
        theta=alt.Theta("TOTAL_FLIGHTS:Q", stack=True),
        color=alt.Color("ORIGIN_COUNTRY:N", scale=alt.Scale(scheme="set2"), legend=alt.Legend(title="Country")),
        tooltip=["ORIGIN_COUNTRY", "TOTAL_FLIGHTS"],
    )
    .properties(height=420)
)
st.altair_chart(fig_pie, use_container_width=True)
```
