# Visualization
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import seaborn as sns
import squarify
import textwrap

import numpy as np
import pandas as pd

SEQUENTIAL_RED_COLOR_PALLETE = "Reds"
GRAY_CHARCOAL = "#373F51"

def plot_highways_map_with_count( panel, gdf_rodovias, column="QT_ACIDENTES" ):
    max_value = gdf_rodovias[column].max()
    min_value = gdf_rodovias[column].min()
    
    # Join accidents data with highways data   
    fig, ax = plt.subplots(figsize=(5, 5))

    # get Reds palette with N=6 colors
    cmap = colors.ListedColormap(sns.color_palette(SEQUENTIAL_RED_COLOR_PALLETE, 6))
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
    # using SEQUENTIAL_COLOR_PALLETE palette with N=6 colors
    cmap = colors.ListedColormap(sns.color_palette(SEQUENTIAL_RED_COLOR_PALLETE, n_colors))
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
        cmap=SEQUENTIAL_RED_COLOR_PALLETE, 
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
    "Reds_r"
    fig, ax = plt.subplots(figsize=(6, 4))
    squarify.plot(
        sizes=df[column],
        label=df[label],
        color=sns.color_palette("bone", n_colors=len(df)),
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
    df = df.sort_values(by=values_column, ascending=False).head(10)

    # wrap label column text
    df[label_column] = df[label_column].apply(
        lambda x: textwrap.fill(x, width=20)
    )

    fig, ax = plt.subplots(figsize=(4, 7))
    
    sns.barplot(
        data=df,
        y=label_column,
        x=values_column,
        ax=ax,
        color=GRAY_CHARCOAL,
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
            fontsize=12
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

    # Set the color of the highest bar to red
    colors = ["#414148", "#c20000"]
    df["color"] = colors[0]
    df = df.sort_values(by=y_column, ascending=False)
    df.loc[df[y_column] == df[y_column].max(), "color"] = colors[1]

    sns.barplot(
        data=df,
        x=x_column,
        y=y_column,
        ax=ax,
        palette=df["color"],
    )

    # Add total count labels on top of the bars
    # Color the highest bar in red
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
