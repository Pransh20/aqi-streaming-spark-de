import streamlit as st
import pandas as pd
import glob
import pyarrow.parquet as pq
import os

st.set_page_config(page_title="Air Quality Germany", layout="wide")
st.title("🇩🇪 Real-time Air Quality in Germany")

st.markdown("""
**Live analytics from Kafka + Spark streaming.  
Averages are per Bundesland, pollutant, and rolling window.**
""")

# Read and merge all parquet files from the output directory
files = glob.glob("output/*.parquet")
if not files:
    st.warning("No data yet! Make sure your Spark job is running and writing to `output/`.")
    st.stop()

df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

# If window is a struct, unpack to start/end columns
if "window" in df.columns:
    if isinstance(df["window"].iloc[0], dict):
        df["window_start"] = pd.to_datetime(df["window"].apply(lambda x: x["start"]))
        df["window_end"] = pd.to_datetime(df["window"].apply(lambda x: x["end"]))
    else:
        # PySpark Row type
        df["window_start"] = pd.to_datetime(df["window"].apply(lambda x: x.start))
        df["window_end"] = pd.to_datetime(df["window"].apply(lambda x: x.end))

# Sidebar filters
states = sorted(df["bundesland"].dropna().unique())
pollutants = sorted(df["parameter"].dropna().unique())

state_sel = st.sidebar.multiselect("Select Bundesland", states, default=states)
pollutant_sel = st.sidebar.multiselect("Select Pollutant", pollutants, default=pollutants)

df_filt = df[
    df["bundesland"].isin(state_sel) & 
    df["parameter"].isin(pollutant_sel)
]

st.subheader("Average Air Quality Over Time")
for pollutant in pollutant_sel:
    plot_df = df_filt[df_filt["parameter"] == pollutant]
    if plot_df.empty:
        continue
    st.write(f"**Pollutant:** {pollutant}")
    chart_df = (
        plot_df
        .pivot_table(
            index="window_end", columns="bundesland", values="avg_value"
        )
        .sort_index()
    )
    st.line_chart(chart_df)

st.subheader("Latest Values Per Region")
latest = (
    df_filt
    .sort_values("window_end")
    .groupby(["bundesland", "parameter"])
    .tail(1)
    .sort_values(["parameter", "bundesland"])
)
st.dataframe(latest[["window_end", "bundesland", "parameter", "avg_value"]])

st.info("Dashboard auto-refreshes as Spark writes new data to 'output/'.")

