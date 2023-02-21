# Data manipulation
import pyarrow.parquet as pq
from pyarrow import fs

import pandas as pd
import geopandas as gpd
import contextily as ctx

import streamlit as st

BR_RODOVIAS_JSON_GIST = "https://gist.githubusercontent.com/jaumpedro214/8524d59a2e7791c504eda0783975bcb0/raw/810b46a61fe909ca0a8416d762928e230fe8aa8c/rodovias_federais_brasil.json"
BR_UFS_JSON = "https://raw.githubusercontent.com/fititnt/gis-dataset-brasil/master/uf/topojson/uf.json"


@st.cache_data
def get_geodf_br_rodovias():
    gdf_rodovias = gpd.read_file(BR_RODOVIAS_JSON_GIST, driver="GeoJSON")

    # Group geometry by vl_br
    gdf_rodovias = gdf_rodovias.groupby("vl_br").agg(
        {"geometry": lambda x: gpd.GeoSeries(x).unary_union}
    ).reset_index()

    # Convert to geodataframe
    gdf_rodovias = gpd.GeoDataFrame(gdf_rodovias, geometry="geometry")

    # Set CRS
    gdf_rodovias.crs = "EPSG:4326"

    return gdf_rodovias

@st.cache_data
def get_geodf_br_ufs():
    gdf_ufs = gpd.read_file(BR_UFS_JSON)

    # Set CRS
    gdf_ufs.crs = "EPSG:4326"

    return gdf_ufs

@st.cache_data
def get_df_accidents_gold_agg():
    PATH = "data-lake-accidents/data/accidents_gold_agg.parquet"
    gcs = fs.GcsFileSystem(anonymous=True)
    return pq.ParquetDataset(PATH, filesystem=gcs).read_pandas().to_pandas()

@st.cache_data
def aggregate_dataframe_and_join_with_geodataframe(
    df, _gdf, column_to_aggregate,
    df_join_column, gdf_join_column
):
    # Aggregate dataframe
    df_agg = df[[column_to_aggregate, df_join_column]].groupby(df_join_column).sum().reset_index()
    
    # Join aggregated dataframe with geodataframe
    _gdf = _gdf.merge(df_agg, left_on=gdf_join_column, right_on=df_join_column)
    return _gdf

@st.cache_data
def aggregate_dataframe_by_column(
    df, columns_to_aggregate, columns_to_groupby
):
    # Aggregate dataframe
    df_agg = df[[*columns_to_aggregate, *columns_to_groupby]].groupby(columns_to_groupby).sum().reset_index()
    return df_agg


def calculate_death_rate_by_accident_type(df):

    df_agg = df[["DS_TIPO", "QT_TOTAL_MORTOS", "QT_TOTAL_PESSOAS"]].groupby("DS_TIPO").sum().reset_index()
    df_agg["TAXA_LETALIDADE"] = df_agg["QT_TOTAL_MORTOS"] / df_agg["QT_TOTAL_PESSOAS"]

    return df_agg
