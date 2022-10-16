from matplotlib.pyplot import plot
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import os
import pandas as pd
from datetime import datetime
import numpy as np
import bs4

import geopandas as gpd

from logging import getLogger
from rat.utils.logging import LOG_NAME, NOTIFICATION
from rat.utils.convert_for_website import convert_v2_frontend
from rat.utils.utils import create_directory

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


def generate_plots(res_shpfile, res_shpfile_column_dict, basin_data_dir):
    """Calls `plot_reservoir` to generate the html plots for all reservoirs mapped in RAT.

    Args:
        res_shpfile (str): Path to a geojson file containing reservoir locations as points.
        res_shpfile_column_dict (dict): Dictionary of column names in reservoir shapefile. 
                                        Example (The keys should be asit is ):
                                        {
                                            unique_identifier: 'id'
                                            id_column : 'xyz',
                                            dam_name_column : 'pqr',
                                            area_column     : 'aey'
                                        }
        basin_data_dir (str): Path to the basin data directory 
            
    """
    reservoirs_polygon = gpd.read_file(res_shpfile)

    # using `project_dir` determine the directories of fluxes
    inflow_dir = os.path.join(basin_data_dir, "rout_inflow")
    outflow_dir = os.path.join(basin_data_dir, "rat_outflow")
    dels_dir = os.path.join(basin_data_dir, "dels")
    sarea_dir = os.path.join(basin_data_dir, "gee_sarea_tmsos")
    website_plot_dir = create_directory(os.path.join(basin_data_dir,"website_plots"),True)

    for reservoir_no,reservoir in reservoirs_polygon.iterrows():
        # Reading reservoir information
        reservoir_file_name = str(reservoir[res_shpfile_column_dict['unique_identifier']])
        name = str(reservoir[res_shpfile_column_dict['dam_name_column']])
        inflow_fn = os.path.join(inflow_dir, reservoir_file_name[:5] + ".csv")
        dels_fn = os.path.join(dels_dir, reservoir_file_name + ".csv")
        outflow_fn = os.path.join(outflow_dir, reservoir_file_name + ".csv")
        sarea_fn = os.path.join(sarea_dir, reservoir_file_name + ".csv")
        save_fn = os.path.join(website_plot_dir, f"{reservoir_file_name}.html")

        if os.path.exists(inflow_fn) and os.path.exists(dels_fn) and os.path.exists(outflow_fn) and os.path.exists(sarea_fn):
            # Plot
            log.debug(f"Plotting: {name}")
            plot_reservoir(inflow_fn, outflow_fn, dels_fn, sarea_fn, name, save_fn)

            # Convert for v2-frontend
            log.debug(f"Converting to v2-website format: {name}")
            convert_v2_frontend(basin_data_dir, reservoir_file_name, inflow_fn, sarea_fn, dels_fn, outflow_fn)

            # Inject html
            log.debug(f"Injecting download links: {name}")
            inject_download_links(save_fn, reservoir_file_name)


def inject_download_links(html_fn, res_name, prefix="../"):
    """Injects download links at the end of the html file.

    Args:
        html_fn (str): path of the html file whgere links will be injected
        res_name (str): name of reservoir (assumed to be the name of the file for downloading)
        prefix (str; default: "data"): prefix to use for obtaining the relative path that will be delivered as downloadable link 
    """
    with open(html_fn) as src:
        txt = src.read()
        soup = bs4.BeautifulSoup(txt)

    # Create Paragraph
    p_tag = soup.new_tag("p")

    # Create unordered list
    ul_tag = soup.new_tag("ul")

    # Heading text
    strong_title = soup.new_tag("strong")
    strong_title.string = "Download Data"
    ul_tag.append(strong_title)

    # link to Inflow data
    inflow_link = soup.new_tag("a", href=f"{prefix}/inflow/{res_name}.csv")
    inflow_link.string = "Inflow"
    li_inflow_link = soup.new_tag('li')
    li_inflow_link.append(inflow_link)

    # link to Surface Area data
    sarea_link = soup.new_tag("a", href=f"{prefix}/sarea_tmsos/{res_name}.csv")
    sarea_link.string = "Surface Area (TMS-OS)"
    li_sarea_link = soup.new_tag('li')
    li_sarea_link.append(sarea_link)

    # link to dels data
    dels_link = soup.new_tag("a", href=f"{prefix}/dels/{res_name}.csv")
    dels_link.string = "Storage Change"
    li_dels_link = soup.new_tag('li')
    li_dels_link.append(dels_link)

    # link to Outflow data
    outflow_link = soup.new_tag("a", href=f"{prefix}/outflow/{res_name}.csv")
    outflow_link.string = "Outflow"
    li_outflow_link = soup.new_tag('li')
    li_outflow_link.append(outflow_link)

    # add the links to the unordered list
    ul_tag.append(li_inflow_link)
    ul_tag.append(li_outflow_link)
    ul_tag.append(li_sarea_link)
    ul_tag.append(li_dels_link)

    # add the list to the paragraph
    p_tag.append(ul_tag)

    # add the paragraph to the end of the body
    soup.body.append(p_tag)

    # save the file
    with open(html_fn, "w") as dst:
        dst.write(str(soup))