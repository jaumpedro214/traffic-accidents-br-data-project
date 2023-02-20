import streamlit as st

# Data manipulation
import pyarrow.parquet as pq
from pyarrow import fs
import numpy as np

import pandas as pd
import geopandas as gpd
import contextily as ctx

# Visualization
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import seaborn as sns



BR_RODOVIAS_JSON_GIST = "https://gist.githubusercontent.com/jaumpedro214/8524d59a2e7791c504eda0783975bcb0/raw/810b46a61fe909ca0a8416d762928e230fe8aa8c/rodovias_federais_brasil.json"
BR_UFS_JSON = "https://raw.githubusercontent.com/fititnt/gis-dataset-brasil/master/uf/topojson/uf.json"

# =====================
# Data loading functions
# =====================

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

# =====================
# Filters
# =====================

def add_filters():

    st.sidebar.text("Filtro1")
    st.sidebar.text("Filtro2")
    st.sidebar.text("Filtro3")

# =====================
# Plotting functions
# =====================

def plot_highways_map_with_count( panel, gdf_rodovias, column="QT_ACIDENTES" ):
    max_value = gdf_rodovias[column].max()
    min_value = gdf_rodovias[column].min()
    
    # Join accidents data with highways data   
    fig, ax = plt.subplots(figsize=(5, 5))

    # get Reds palette with N=6 colors
    cmap = colors.ListedColormap(sns.color_palette("Reds", 6))
    sm = plt.cm.ScalarMappable(
        cmap=cmap,
        norm=colors.Normalize(
            vmin=min_value,
            vmax=max_value,
        )
    )

    gdf_rodovias.plot(
        ax=ax, 
        column=column, 
        legend=False,
        linewidth=1,
        cmap=cmap,
        norm=sm.norm
    )
    ax.set_axis_off()

    # add colorbar below the plot
    ticks = np.linspace(min_value, max_value, 6+1)
    ticks_labels_formatted = [ 
        f"{tick//1e3:.0f} mil" if tick >= 1e3 else 
        f"{int(tick)}" for tick in ticks 
    ]
    color_bar_legend = fig.colorbar(
        sm, ax=ax, orientation="horizontal", pad=0.05, shrink=0.5,
        ticks=ticks
    )
    color_bar_legend.ax.set_xticklabels(ticks_labels_formatted)
    color_bar_legend.ax.tick_params(rotation=45, labelsize=8)

    # Add background map
    # ctx.add_basemap(
    #     ax, 
    #     crs=gdf_rodovias.crs.to_string(), 
    #     source=ctx.providers.Stamen.TonerLite
    # )

    panel.pyplot(fig)


def plot_states_map_with_count( panel, gdf_ufs, column="QT_ACIDENTES" ):
    max_value = gdf_rodovias[column].max()
    min_value = gdf_rodovias[column].min()

    fig, ax = plt.subplots(figsize=(5, 5))
    sm = plt.cm.ScalarMappable(
        cmap="Reds", 
        norm  = colors.Normalize(
            vmin=1,
            vmax=gdf_ufs[column].max(),
        )
    )

    gdf_ufs.plot(
        ax=ax, 
        column=column, 
        legend=False,
        linewidth=1,
        cmap=sm.cmap,
    )

    ax.set_axis_off()

    # add colorbar below the plot
    ticks = np.linspace(min_value, max_value, 7)
    ticks_labels_formatted = [ 
        f"{tick//1000:.0f} mil" if tick >= 1000 else f"{tick:.0f}"
        for tick in ticks 
    ]
        
    # add the colorbar to the figure
    cbar = fig.colorbar(
        sm, ax=ax, orientation="horizontal", pad=0.05, shrink=0.5,
        ticks=ticks
    )
    cbar.ax.set_xticklabels(ticks_labels_formatted)
    cbar.ax.tick_params(rotation=45, labelsize=8)
    
    # Add plot to the streamlit panel
    panel.pyplot(fig)

def plot_column_sum_on_card( panel, df, column, label="Label" ):
    value = df[column].sum()
    panel.metric(label, value=value)

if __name__ == "__main__":
    st.set_page_config(layout="wide")

    st.sidebar.title("Sidebar")

    gdf_rodovias = get_geodf_br_rodovias()
    gdf_ufs = get_geodf_br_ufs()
    df_accidents = get_df_accidents_gold_agg()

    add_filters()
    st.title("Hello World!")

    # plot gdf_rodovias map
    # st.dataframe(df_accidents.head(10))

    big_number_columns = st.columns(4)
    plot_column_sum_on_card(big_number_columns[0], df_accidents, "QT_ACIDENTES",     label="Acidentes")
    plot_column_sum_on_card(big_number_columns[1], df_accidents, "QT_TOTAL_PESSOAS", label="Acidentados")
    plot_column_sum_on_card(big_number_columns[2], df_accidents, "QT_TOTAL_FERIDOS", label="Feridos")
    plot_column_sum_on_card(big_number_columns[3], df_accidents, "QT_TOTAL_MORTOS",  label="Mortos")


    columns_lv_1 = st.columns([.75, .25])

    columns_lv_1[0].markdown("## Rodovias Federais")
    columns_lv_1[1].markdown("## Causas")

    tab_por_br, tab_por_uf = columns_lv_1[0].tabs(["Por BR", "Por UF"])
    tab_por_br_columns = tab_por_br.columns([.50, .25])
    tab_por_uf_columns = tab_por_uf.columns([.50, .25])

    # Plotting Rodovias Federais - Accidents by BR
    # Group df_accidents by BR
    df_accidents_por_br = df_accidents[["DS_BR", "QT_ACIDENTES"]].groupby("DS_BR").sum().reset_index()
    # Join with gdf_rodovias
    gdf_rodovias = gdf_rodovias.merge(df_accidents_por_br, left_on="vl_br", right_on="DS_BR")
    plot_highways_map_with_count(tab_por_br_columns[0], gdf_rodovias, column="QT_ACIDENTES")

    # Plotting UFs - Accidents by UF  
    df_accidents_por_uf = df_accidents[["SG_UF", "QT_ACIDENTES"]].groupby("SG_UF").sum().reset_index()
    # Join with gdf_ufs
    gdf_ufs = gdf_ufs.merge(df_accidents_por_uf, left_on="id", right_on="SG_UF")
    plot_states_map_with_count(tab_por_uf_columns[0], gdf_ufs, column="QT_ACIDENTES")

    st.text("Rodovias Federais")
