import streamlit as st

# Data manipulation
import pyarrow.parquet as pq
from pyarrow import fs

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
def get_br_rodovias():
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
def get_br_ufs():
    gdf_ufs = gpd.read_file(BR_UFS_JSON)

    # Set CRS
    gdf_ufs.crs = "EPSG:4326"

    return gdf_ufs

@st.cache_data
def get_accidents_gold_agg():
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

def plot_highways_map_with_count( panel, gdf_rodovias, value_column="QT_ACIDENTES" ):
    # Join accidents data with highways data   
    fig, ax = plt.subplots(figsize=(5, 5))
    gdf_rodovias.plot(
        ax=ax, 
        column=value_column, 
        legend=False,
        linewidth=1,
        cmap="Reds",
    )
    ax.set_axis_off()

    # add colorbar below the plot
    # with the values 100, 1000, 10000, 100000, 50000, 300000
    sm = plt.cm.ScalarMappable(
        cmap="Reds", 
        norm = colors.LogNorm(
            vmin=100, 
            vmax=gdf_rodovias[value_column].max(),
        )
    )
    ticks = [100, 1000, 10000, 100000, 50000, 300000]
    # automatically set the labels to the formatted ticks
    ticks_labels = [ f"{tick//1000} mil" if tick >= 1000 else f"{tick}" for tick in ticks ]
    ticks_labels[-1] = f"<{ticks_labels[-1]}"
    ticks_labels[0] = f">{ticks_labels[0]}"
        
    # add the colorbar to the figure
    cbar = fig.colorbar(
        sm, ax=ax, orientation="horizontal", pad=0.05, shrink=0.5,
        ticks=ticks
    )
    cbar.ax.set_xticklabels(ticks_labels)
    cbar.ax.tick_params(rotation=45, labelsize=8)

    # Add background map
    ctx.add_basemap(
        ax, 
        crs=gdf_rodovias.crs.to_string(), 
        source=ctx.providers.Stamen.TonerLite
    )

    panel.pyplot(fig)


def plot_accidents_by_uf( panel, gdf_ufs, df_accidents ):
    fig, ax = plt.subplots(figsize=(5, 5))
    gdf_ufs.plot(ax=ax, color="blue")
    panel.pyplot(fig)

def plot_agg_big_card( panel, df, column, label="Label" ):
    value = df[column].sum()
    panel.metric(label, value=value)

if __name__ == "__main__":
    st.set_page_config(layout="wide")

    st.sidebar.title("Sidebar")

    gdf_rodovias = get_br_rodovias()
    gdf_ufs = get_br_ufs()
    df_accidents = get_accidents_gold_agg()

    add_filters()
    st.title("Hello World!")

    # plot gdf_rodovias map
    # st.dataframe(df_accidents.head(10))

    big_number_columns = st.columns(4)
    plot_agg_big_card(big_number_columns[0], df_accidents, "QT_ACIDENTES",     label="Acidentes")
    plot_agg_big_card(big_number_columns[1], df_accidents, "QT_TOTAL_PESSOAS", label="Acidentados")
    plot_agg_big_card(big_number_columns[2], df_accidents, "QT_TOTAL_FERIDOS", label="Feridos")
    plot_agg_big_card(big_number_columns[3], df_accidents, "QT_TOTAL_MORTOS",  label="Mortos")


    columns_lv_1 = st.columns([.75, .25])

    columns_lv_1[0].markdown("## Rodovias Federais")
    columns_lv_1[1].markdown("## Causas")

    tab_por_br, tab_por_uf = columns_lv_1[0].tabs(["Por BR", "Por UF"])
    tab_por_br_columns = tab_por_br.columns([.50, .25])
    tab_por_uf_columns = tab_por_uf.columns([.50, .25])

    # Plotting Rodovias Federais - Accidents by BR
    # Group df_accidents by BR
    df_accidents_por_br = df_accidents.groupby("DS_BR").sum().reset_index()
    # Join with gdf_rodovias
    gdf_rodovias = gdf_rodovias.merge(df_accidents_por_br, left_on="vl_br", right_on="DS_BR")
    plot_highways_map_with_count(tab_por_br_columns[0], gdf_rodovias, value_column="QT_ACIDENTES")



    plot_accidents_by_uf(tab_por_uf_columns[0], gdf_ufs, df_accidents)

    st.text("Rodovias Federais")
