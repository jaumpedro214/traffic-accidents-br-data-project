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
import squarify
import textwrap


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

# =====================
# Filters
# =====================

def add_filters_on_sidebar(df_acidentes):

    min_year = int(df_acidentes["ANO"].min())
    max_year = int(df_acidentes["ANO"].max())
    min_year_selected, max_year_selected = st.sidebar.slider(
        "Ano", 
        min_value=min_year, max_value=max_year, 
        value=(min_year, max_year)
    )

    tipos = list( df_acidentes["DS_TIPO"].unique() )
    tipos_selected = st.sidebar.multiselect("Tipo de Acidente", tipos, default=[])
    if tipos_selected == []:
        tipos_selected = tipos

    ufs = list( df_acidentes["SG_UF"].unique() )
    ufs_selected = st.sidebar.multiselect("UF", ufs, default=[])
    if ufs_selected == []:
        ufs_selected = ufs

    brs = list( df_acidentes["DS_BR"].unique() )
    brs_selected = st.sidebar.multiselect("BR", brs, default=[])
    if brs_selected == []:
        brs_selected = brs

    return min_year_selected, max_year_selected, tipos_selected, ufs_selected, brs_selected


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
    
    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)
    # remove xticks
    ax.set_xticks([])
    ax.set_yticks([])

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

def plot_stylized_df_table_with_counts( panel, df, id_column, value_column, value_column_name=None, n_colors=256 ):

    df = (
        df[[id_column, value_column]]
        .sort_values(value_column, ascending=False)
        .head(15)
        .set_index(id_column)
        .rename(columns={value_column: value_column_name or value_column})
    )
    value_column = value_column_name or value_column
    # Color columns
    # using "Reds" palette with N=6 colors
    cmap = colors.ListedColormap(sns.color_palette("Reds", n_colors))
    # Style table
    df_styled = df.style.background_gradient(
        cmap=cmap
    )
    # Format values XXX XXX,XX  
    df_styled = df_styled.format({
        value_column: lambda x: f"{x:,.0f}"
    })

    # Show table
    panel.table(df_styled)

def plot_states_map_with_count( panel, gdf_ufs, column="QT_ACIDENTES" ):
    max_value = gdf_ufs[column].max()
    min_value = gdf_ufs[column].min()

    fig, ax = plt.subplots(figsize=(5,5))
    sm = plt.cm.ScalarMappable(
        cmap="Reds", 
        norm  = colors.Normalize(
            vmin=min_value,
            vmax=max_value,
        )
    )

    gdf_ufs.plot(
        ax=ax, 
        column=column, 
        legend=False,
        linewidth=1,
        cmap=sm.cmap,
    )

    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)
    # remove xticks
    ax.set_xticks([])
    ax.set_yticks([])

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

def plot_treemap_by_column( panel, df, column, label="Label" ):

    df = df.sort_values(by=column, ascending=False)
    df["PERCENTUAL"] = df[column] / df[column].sum()
    # Order by PERCENTUAL
    df = df.sort_values(by="PERCENTUAL", ascending=False)
    # Limit top 10 andd aggregate the rest using "Outros"
    df = pd.concat(
        [
            df.iloc[:10],
            pd.DataFrame(
                {
                    label: ["Outros"],
                    column: [df.iloc[10:][column].sum()],
                    "PERCENTUAL": [df.iloc[10:]["PERCENTUAL"].sum()],
                }
            )
        ]
    )

    # wrap text on table over 20 characters
    df[label] = df[label].apply(
        lambda x: textwrap.fill(x, width=15) if len(x) < 35 else textwrap.fill(x[:35] + "...", width=15) 
    )

    fig, ax = plt.subplots(figsize=(6, 4))
    squarify.plot(
        sizes=df[column],
        label=df[label],
        color=sns.color_palette("Reds_r", n_colors=len(df)),
        alpha=.8,
        pad=0.5,
        # bold text, wrap text, outline
        text_kwargs={
            "fontsize": 8,
            "fontweight": "bold",
            "wrap": True,
        },
        ax=ax,
    )
    ax.set_axis_off()

    panel.pyplot(fig)

def plot_vertical_bar_chart_death_rate( panel, df, values_column="TAXA_LETALIDADE", label_column="DS_TIPO" ):

    # plot vertical bar chart with seaborn
    df = df.sort_values(by=values_column, ascending=False)
    fig, ax = plt.subplots(figsize=(4, 8))

    sns.barplot(
        data=df,
        y=label_column,
        x=values_column,
        ax=ax,
        color="#f0050d"#"#d1060d" 
    )

    # Add percentage labels
    for p in ax.patches:
        width = p.get_width()
        ax.text(
            width*1.02,
            p.get_y() + p.get_height() / 2.0,
            f"{100*width:.1f}%",
            ha="left",
            va="center",
        )

    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)
    # remove xticks
    ax.set_xticks([])

    panel.pyplot(fig)

def plot_horizontal_bar_chart_with_counts( panel, df, y_column="QT_ACIDENTES", x_column="ANO"):

    # plot horizontal bar chart with seaborn
    fig, ax = plt.subplots(figsize=(8, 2))

    sns.barplot(
        data=df,
        x=x_column,
        y=y_column,
        ax=ax,
        color="#f0050d"#"#d1060d"
    )

    # Add total count labels on top of the bars
    for p in ax.patches:
        height = p.get_height()
        ax.text(
            p.get_x() + p.get_width() / 2.0,
            height+0.1,
            f"{height/1e3:.1f}" ,
            ha="center",
            va="bottom",
            fontsize=8,
        )

    ax.set_xlabel("")
    ax.set_ylabel("")
    # Remove box around the plot
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    # ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)
    # remove y ticks
    ax.set_yticks([])
    # rotate xticks 90 degrees
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90, fontsize=8)

    panel.pyplot(fig)

if __name__ == "__main__":
    st.set_page_config(layout="wide")
    st.sidebar.title("Filtros")
    st.title("Acidentes de Trânsito nas Rodovias do Brasil")

    gdf_rodovias = get_geodf_br_rodovias()
    gdf_ufs = get_geodf_br_ufs()
    df_accidents = get_df_accidents_gold_agg()

    df_accidents["DATA"] = pd.to_datetime(df_accidents["DATA"])
    df_accidents["ANO"] = df_accidents["DATA"].dt.year.astype(int)

    # Get filters
    min_year, max_year, tipos, ufs, brs = add_filters_on_sidebar(df_accidents)
    # Filter data
    df_accidents = df_accidents.loc[
        (df_accidents["ANO"] >= min_year)
        & (df_accidents["ANO"] <= max_year)
        & (df_accidents["DS_TIPO"].isin(tipos))
        & (df_accidents["SG_UF"].isin(ufs))
        & (df_accidents["DS_BR"].isin(brs))
    ]


    big_number_columns = st.columns(4)
    columns_lv_1 = st.columns([.60, .40])

    columns_lv_1[0].markdown("### Contagem por localidade")
    tab_por_br, tab_por_uf = columns_lv_1[0].tabs(["Por BR", "Por UF"])
    tab_por_br_columns = tab_por_br.columns([.50, .25])
    tab_por_uf_columns = tab_por_uf.columns([.50, .25])

    plot_column_sum_on_card(big_number_columns[0], df_accidents, "QT_ACIDENTES",     label="Acidentes")
    plot_column_sum_on_card(big_number_columns[1], df_accidents, "QT_TOTAL_PESSOAS", label="Acidentados")
    plot_column_sum_on_card(big_number_columns[2], df_accidents, "QT_TOTAL_FERIDOS", label="Feridos")
    plot_column_sum_on_card(big_number_columns[3], df_accidents, "QT_TOTAL_MORTOS",  label="Mortos")
    
    columns_lv_1[1].markdown("### Principais Causas")

    columns_lv_1[0].markdown("### Acidentes por ano")
    # Plotting Rodovias Federais - Accidents by BR
    gdf_accidents_by_br = aggregate_dataframe_and_join_with_geodataframe(
        df_accidents, gdf_rodovias, "QT_ACIDENTES", "DS_BR", "vl_br"
    )
    # Filter BR with more than 1000 accidents
    gdf_accidents_by_br = gdf_accidents_by_br.loc[gdf_accidents_by_br["QT_ACIDENTES"] > 1000]
    plot_highways_map_with_count(tab_por_br_columns[0], gdf_accidents_by_br, column="QT_ACIDENTES")
    plot_stylized_df_table_with_counts(tab_por_br_columns[1], gdf_accidents_by_br, "DS_BR","QT_ACIDENTES", "Qtd. Acidentes", n_colors=6)

    # Plotting States - Accidents by UF
    gdf_accidents_by_uf = aggregate_dataframe_and_join_with_geodataframe(
        df_accidents, gdf_ufs, "QT_ACIDENTES", "SG_UF", "id"
    )
    plot_states_map_with_count(tab_por_uf_columns[0], gdf_accidents_by_uf, column="QT_ACIDENTES")
    plot_stylized_df_table_with_counts(tab_por_uf_columns[1], gdf_accidents_by_uf, "SG_UF","QT_ACIDENTES", "Qtd. Acidentes")

    # Group by accidents by year
    df_accidents_by_year = aggregate_dataframe_by_column(df_accidents, ["QT_ACIDENTES"], ["ANO"])

    # Plot accidents by year
    plot_horizontal_bar_chart_with_counts(columns_lv_1[0], df_accidents_by_year, y_column="QT_ACIDENTES", x_column="ANO")

    # Group by accidents by DS_CAUSA
    df_accidents_by_cause = aggregate_dataframe_by_column(df_accidents, ["QT_ACIDENTES"], ["DS_CAUSA"])
    # Plot accidents by cause
    plot_treemap_by_column(columns_lv_1[1], df_accidents_by_cause, "QT_ACIDENTES", label="DS_CAUSA")

    df_death_rate_by_type = calculate_death_rate_by_accident_type(df_accidents)
    columns_lv_1[1].markdown("### Mortalidade de cada tipo de acidente")
    plot_vertical_bar_chart_death_rate(columns_lv_1[1], df_death_rate_by_type)
