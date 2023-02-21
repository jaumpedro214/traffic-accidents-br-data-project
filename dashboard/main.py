import streamlit as st

from plotting import *
from data_manipulation import *


def add_filters_on_sidebar(df_acidentes):

    min_year = int(df_acidentes["ANO"].min())
    max_year = int(df_acidentes["ANO"].max())
    min_year_selected, max_year_selected = st.sidebar.slider(
        "Ano",
        min_value=min_year, max_value=max_year,
        value=(min_year, max_year)
    )

    tipos = list(df_acidentes["DS_TIPO"].unique())
    tipos_selected = st.sidebar.multiselect(
        "Tipo de Acidente", tipos, default=[])
    if tipos_selected == []:
        tipos_selected = tipos

    ufs = list(df_acidentes["SG_UF"].unique())
    ufs_selected = st.sidebar.multiselect("UF", ufs, default=[])
    if ufs_selected == []:
        ufs_selected = ufs

    brs = list(df_acidentes["DS_BR"].unique())
    brs_selected = st.sidebar.multiselect("BR", brs, default=[])
    if brs_selected == []:
        brs_selected = brs

    return min_year_selected, max_year_selected, tipos_selected, ufs_selected, brs_selected


def add_about_on_sidebar():
    about = """
    ## Sobre o projeto
    
    O intuito desse projeto foi desenvolver uma aplicação end-to-end da área de dados com um assunto de relevância pública. 
    
    Ele engloba a coleta dos dados ([publicados pela PRF](https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-acidentes)), 
    o tratamento e engenharia (Medallion Architecture com PySpark+DeltaLake), 
    a entrega na nuvem (Bucket do GCP) e a criação do dashboard final (Python com Streamlit e várias bibliotecas).
    
    _Tudo dentro do docker_

    Author: [João Pedro da S.L.](https://github.com/jaumpedro214)
    """

    st.sidebar.markdown(about)


if __name__ == "__main__":
    st.set_page_config(layout="wide")
    st.sidebar.title("Filtros")
    st.title("Acidentes de Trânsito nas Rodovias Federais do Brasil")
    st.markdown("### Uma visão geral dos acidentes em números e mapas")

    gdf_rodovias = get_geodf_br_rodovias()
    gdf_ufs = get_geodf_br_ufs()
    df_accidents = get_df_accidents_gold_agg()

    df_accidents["DATA"] = pd.to_datetime(df_accidents["DATA"])
    df_accidents["ANO"] = df_accidents["DATA"].dt.year.astype(int)

    # Filter dataframe
    min_year, max_year, tipos, ufs, brs = add_filters_on_sidebar(df_accidents)
    df_accidents = df_accidents.loc[
        (df_accidents["ANO"] >= min_year)
        & (df_accidents["ANO"] <= max_year)
        & (df_accidents["DS_TIPO"].isin(tipos))
        & (df_accidents["SG_UF"].isin(ufs))
        & (df_accidents["DS_BR"].isin(brs))
    ]

    add_about_on_sidebar()

    # Adding big numbers
    # =======================================
    big_number_columns = st.columns(4)

    plot_column_sum_on_card(
        big_number_columns[0], df_accidents, "QT_ACIDENTES",     label="Acidentes"
    )
    plot_column_sum_on_card(
        big_number_columns[1], df_accidents, "QT_TOTAL_PESSOAS", label="Acidentados"
    )
    plot_column_sum_on_card(
        big_number_columns[2], df_accidents, "QT_TOTAL_FERIDOS", label="Feridos"
    )
    plot_column_sum_on_card(
        big_number_columns[3], df_accidents, "QT_TOTAL_MORTOS",  label="Mortos"
    )

    # Adding tabs with maps
    # =======================================
    columns_lv_1 = st.columns([.60, .40])
    columns_lv_1[0].markdown("### Contagem por localidade")
    tab_por_br, tab_por_uf = columns_lv_1[0].tabs(["Por BR", "Por UF"])
    tab_por_br_columns = tab_por_br.columns([.50, .25])
    tab_por_uf_columns = tab_por_uf.columns([.50, .25])

    # Plotting Rodovias Federais - Accidents by BR
    gdf_accidents_by_br = aggregate_dataframe_and_join_with_geodataframe(
        df_accidents, gdf_rodovias, "QT_ACIDENTES", "DS_BR", "vl_br"
    )
    # Filter BR with more than 1000 accidents
    gdf_accidents_by_br = gdf_accidents_by_br.loc[gdf_accidents_by_br["QT_ACIDENTES"] > 1000]
    plot_highways_map_with_count(
        tab_por_br_columns[0], gdf_accidents_by_br, column="QT_ACIDENTES")
    plot_stylized_df_table_with_counts(
        tab_por_br_columns[1], gdf_accidents_by_br, "DS_BR", "QT_ACIDENTES", "Qtd. Acidentes", n_colors=6
    )

    # Plotting States - Accidents by UF
    gdf_accidents_by_uf = aggregate_dataframe_and_join_with_geodataframe(
        df_accidents, gdf_ufs, "QT_ACIDENTES", "SG_UF", "id"
    )
    plot_states_map_with_count(
        tab_por_uf_columns[0], gdf_accidents_by_uf, column="QT_ACIDENTES"
    )
    plot_stylized_df_table_with_counts(
        tab_por_uf_columns[1], gdf_accidents_by_uf, "SG_UF", "QT_ACIDENTES", "Qtd. Acidentes"
    )

    # Adding treemap - Principais Causas
    # =======================================
    columns_lv_1[1].markdown("### Causas dos Acidentes")
    # Group by accidents by DS_CAUSA
    df_accidents_by_cause = aggregate_dataframe_by_column(
        df_accidents, ["QT_ACIDENTES"], ["DS_CAUSA"]
    )
    # Plot accidents by cause
    plot_treemap_by_column(
        columns_lv_1[1], df_accidents_by_cause, "QT_ACIDENTES", label="DS_CAUSA"
    )

    # Adding bar plot - Taxa de mortalidade
    # =======================================
    df_death_rate_by_type = calculate_death_rate_by_accident_type(df_accidents)
    columns_lv_1[1].markdown("### Taxa de Mortalidade por tipo de acidente")
    plot_vertical_bar_chart_death_rate(columns_lv_1[1], df_death_rate_by_type)

    # Time series - Contagem por ano
    # =======================================
    columns_lv_1[0].markdown("### Acidentes por ano (em milhares)")
    df_accidents_by_year = aggregate_dataframe_by_column(
        df_accidents, ["QT_ACIDENTES"], ["ANO"]
    )
    plot_horizontal_bar_chart_with_counts(
        columns_lv_1[0], df_accidents_by_year, y_column="QT_ACIDENTES", x_column="ANO"
    )
