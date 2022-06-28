from matplotlib.pyplot import plot
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import os
import pandas as pd
from datetime import datetime
import numpy as np

import geopandas as gpd

from logging import getLogger
from utils.logging import LOG_NAME, NOTIFICATION

log = getLogger(f"{LOG_NAME}.{__name__}")


def plot_reservoir(inflow_fn, outflow_fn, dels_fn, sarea_fn, reservoir_name, save_fn):
    """Plots the fluxes of the reservoirs using `plotly`

    Args:
        inflow_fn (str): path of the inflow file
        outflow_fn (str): path of the outflow file
        dels_fn (str): path of the dels file
        sarea_fn (str): path of the sarea file
        reservoir_name (str): name of the reservoir. Will be used as title
        save_fn (str): path where to save the file
    """
    log.debug(f"Plotting: {reservoir_name} -> {save_fn}")
    inflow = pd.read_csv(inflow_fn, parse_dates=['date'])
    inflow['streamflow'] = inflow['streamflow'] * (24*60*60)        # convert from m3/s to m3/d
    inflow["streamflow"] = inflow['streamflow'] * 1e-6              # convert from m3/d to million m3/d
    dels = pd.read_csv(dels_fn, parse_dates=['date'])[['date', 'dS', 'days_passed']]
    dels['dS'] = dels['dS'] * 1e3                                   # converting from billion m3 to million m3
    outflow = pd.read_csv(outflow_fn, parse_dates=['date'])[['date', 'outflow_rate']]
    outflow.loc[outflow['outflow_rate']<0, 'outflow_rate'] = 0
    outflow['outflow_rate'] = outflow['outflow_rate'] * (24*60*60)  # convert from m3/s to m3/d
    outflow['outflow_rate'] = outflow['outflow_rate'] * 1e-6        # converting from m3/d to million m3/d

    sarea = pd.read_csv(sarea_fn, parse_dates=['date'])[['date', 'area']]

    all_min = np.min([outflow.loc[outflow.date>'2019-01-01', 'outflow_rate'].min(), inflow.loc[inflow.date>'2019-01-01', 'streamflow'].min(), dels.loc[dels.date>'2019-01-01', 'dS'].min()])
    all_max = np.max([outflow.loc[outflow.date>'2019-01-01', 'outflow_rate'].max(), inflow.loc[inflow.date>'2019-01-01', 'streamflow'].max(), dels.loc[dels.date>'2019-01-01', 'dS'].max()])

    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.3, 0.7],
        shared_xaxes=True,
        vertical_spacing=0.05,
        specs=[
            [{"secondary_y": False}],
            [{"secondary_y": True}],
        ]
    )

    # Surface Area
    fig.add_trace(
        go.Scatter(
            x=list(sarea['date']),
            y=list(sarea['area']),
            name="Surface Area",
            showlegend=True,
            mode='markers+lines',
            hovertemplate=r'%{y:.3f}',
            line=dict(color='black'),
            yaxis='y'
        ),
        row=1, col=1,
    )

    # Inflow
    fig.add_trace(
        go.Scatter(
            x=list(inflow.date), 
            y=list(inflow.streamflow), 
            name="Inflow", 
            showlegend=True,
            hovertemplate=r'%{y:.3f}',
            line=dict(color='#4427B0'),
            yaxis='y2'
        ),
        row=2, col=1,
    )

    # delta S
    fig.add_trace(
        go.Scatter(
            x=list(dels['date']),
            y=list(dels['dS']),
            name="∆S",
            showlegend=True,
            mode='markers+lines',
            hovertemplate=r'%{y:.3f}',
            line=dict(color='#72C401'),
            yaxis='y3'
        ),
        row=2, col=1,
        secondary_y=True,
    )

    # Outflow
    fig.add_trace(
        go.Scatter(
            x=list(outflow['date']),
            y=list(outflow['outflow_rate']),
            name="Outflow",
            showlegend=True,
            mode='markers+lines',
            hovertemplate=r'%{y:.3f}',
            line=dict(color='#D81A6D'),
            yaxis='y2'
        ),
        row=2, col=1,

    )

    # Update Layout
    fig['layout'].update(
        # Styling
        autosize=False, 
        height=400, 
        width=800,
        margin={
            'b': 10,
            'l': 10,
            'r': 10,
            't': 50
        },
        title=dict(
            text=reservoir_name,
            y=0.98,
            x=0.5,
            xanchor='center',
            yanchor='top'
        ),
        hovermode='x unified',
        xaxis1=dict(
            rangeselector=dict(
                buttons=list([
                    dict(count=6, label='6 Months', step="month", stepmode="backward"),
                    dict(count=1, label='1 Year', step="year", stepmode="backward"),
                    dict(count=1, label='This Year', step="year", stepmode="todate"),
                    dict(label='All', step="all")
                ])
            ),
        ),
        xaxis2=dict(
            type="date",
            rangeslider=dict(
                visible=True
            ),
            range=['2019-01-01', datetime.today().strftime('%Y-%m-%d')]
        ),
        yaxis=dict(
            anchor="x",
            title="Area (km²)"
        ),
        yaxis2=dict(
            anchor="x",
            title="Flux (×10⁶ m³/day)", #+ r"$\times 10^6 m^3/day$"
            rangemode='normal',
            range=[all_min, all_max],
            scaleratio=1,
        ),
        yaxis3=dict(
            anchor="x",
            title="∆S Volume (×10⁶ m³)", 
            rangemode='normal',
            range=[all_min, all_max],
            scaleanchor='y2',
            scaleratio=1,
        ),
        legend=dict(
            orientation='h',
            yanchor='bottom',
            xanchor='right',
            y=1,
            x=1
        ),
    )
    fig.update_xaxes(showspikes=True, spikemode='across', spikesnap='cursor')

    fig.write_html(save_fn, include_plotlyjs='cdn', include_mathjax='cdn')


def generate_plots(reservoir_db_fn, project_dir):
    """Calls `plot_reservoir` to generate the html plots for all reservoirs mapped in RAT.

    Args:
        reservoir_db_fn (str): Path to a geojson file containing reservoir locations as points.
            db must have column named `RAT_ID` referring to the internal ID that'll be used in RAT.
            db must have column named `NAME` correspondign to the name of the reservoir.
    """
    reservoirs = gpd.read_file(reservoir_db_fn).set_index('RAT_ID', drop=False).sort_index()

    # Temporarily use these dictionaries to map IDs to their corresponding flux file names 
    sarea_fns = {
        1: "Sre_Pok_4",
        2: "Phumi_Svay_Chrum",
        3: "Battambang_1",
        4: "5117",
        5: "Nam_Ngum_1",
        6: "5138",
        7: "5143",
        8: "5147",
        9: "5148",
        10: "Ubol_Ratana",
        11: "Lam_Pao",
        12: "5151",
        13: "5152",
        14: "5155",
        15: "5156",
        16: "5160",
        17: "5162",
        18: "5795",
        19: "Sirindhorn",
        20: "5797",
        21: "Nam_Theun_2",
        22: "7000",
        23: "7001",
        24: "7002",
        25: "Xe_Kaman_1",
        26: "7004",
        27: "7037",
        28: "7159",
        29: "7164",
        30: "7181",
        31: "7201",
        32: "Sesan_4",
        33: "7232",
        34: "7284",
        35: "Lower_Sesan_2",
        36: "Yali",
        37: "Nam_Ton"
    }

    inflow_fns = {
        1: "Sre_P",
        2: "Phumi",
        3: "Batta",
        4: "5117 ",
        5: "Nam_N",
        6: "5138 ",
        7: "5143 ",
        8: "5147 ",
        9: "5148 ",
        10: "Ubol_",
        11: "Lam_P",
        12: "5151 ",
        13: "5152 ",
        14: "5155 ",
        15: "5156 ",
        16: "5160 ",
        17: "5162 ",
        18: "5795 ",
        19: "Sirid",
        20: "5797 ",
        21: "Nam_T",
        22: "7000 ",
        23: "7001 ",
        24: "7002 ",
        25: "Xe_Ka",
        26: "7004 ",
        27: "7037 ",
        28: "7159 ",
        29: "7164 ",
        30: "7181 ",
        31: "7201 ",
        32: "Sesan",
        33: "7232 ",
        34: "7284 ",
        35: "Lower",
        36: "Yali ",
        37: "NamTo",
    }

    dels_outflow_fns = {
        1: "Sre_Pok_4",
        2: "Phumi_Svay_Chrum",
        3: "Battambang_1",
        4: "5117",
        5: "Nam_Ngum_1",
        6: "5138",
        7: "5143",
        8: "5147",
        9: "5148",
        10: "Ubol_Ratana",
        11: "Lam_Pao",
        12: "5151",
        13: "5152",
        14: "5155",
        15: "5156",
        16: "5160",
        17: "5162",
        18: "5795",
        19: "Sirindhorn",
        20: "5797",
        21: "Nam_Theun_2",
        22: "7000",
        23: "7001",
        24: "7002",
        25: "Xe_Kaman_1",
        26: "7004",
        27: "7037",
        28: "7159",
        29: "7164",
        30: "7181",
        31: "7201",
        32: "Sesan_4",
        33: "7232",
        34: "7284",
        35: "Lower_Sesan_2",
        36: "Yali",
        37: "Nam_Ton"
    }

    # using `project_dir` determine the directories of fluxes
    inflow_dir = os.path.join(project_dir, "backend/data/inflow")
    outflow_dir = os.path.join(project_dir, "backend/data/outflow")
    dels_dir = os.path.join(project_dir, "backend/data/dels")
    sarea_dir = os.path.join(project_dir, "backend/data/sarea_tmsos")

    for RAT_ID in reservoirs['RAT_ID']:
        inflow_fn = os.path.join(inflow_dir, inflow_fns[RAT_ID] + ".csv")
        dels_fn = os.path.join(dels_dir, dels_outflow_fns[RAT_ID] + ".csv")
        outflow_fn = os.path.join(outflow_dir, dels_outflow_fns[RAT_ID] + ".csv")
        sarea_fn = os.path.join(sarea_dir, sarea_fns[RAT_ID] + ".csv")
        name = reservoirs.loc[RAT_ID, 'NAME']
        save_fn = os.path.join(project_dir, f"backend/data/website_plots/{RAT_ID}.html")

        if inflow_fns[RAT_ID] and dels_outflow_fns[RAT_ID] and sarea_fns[RAT_ID]:
            plot_reservoir(inflow_fn, outflow_fn, dels_fn, sarea_fn, name, save_fn)