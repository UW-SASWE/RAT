import plotly.graph_objects as go
import pandas as pd
from pathlib import Path
from shapely.geometry import Polygon
from shapely.ops import unary_union
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import plotly.graph_objects as go

class RAT_RESERVOIR:
    """
    A class for representing various reservoir-related variables using Plotly.

    Attributes:
    - rat_output_vars (dict): Description of properties useful for creating plots for each reservoir variable.
    - final_outputs (str): Path to the final outputs directory.
    - file_name (str): Name of the reservoir file to be plotted.
    - reservoir_name (str): Optional parameter for specifying the reservoir name.
    - observed_data_path (str): Optional parameter for specifying the path to the observed data directory. Structure should be similar to RAT's final_outputs.
    - prev_fig: Stores the previous figure instance for multi-trace plotting.

    Methods:
    - __init__: Initializes the RAT_RESERVOIR instance.
    - plot_var: Plots a reservoir variable from inflow, outflow, storage change, elevation, evaporation, surface area and area elevation curve.
    """

    rat_output_vars = {
        'Storage Change': {
                  'x_var_name'    : 'Date',
                  'y_var_name'    : '∆S',
                  'data_folder'   : 'dels',
                  'x_data_column' : 'date',
                  'y_data_column' : 'dS (m3)',
                  'colors'        : ['#FB931D'],
                },
        'Inflow': {
                  'x_var_name'    : 'Date',
                  'y_var_name'    : 'Inflow',  
                  'data_folder'   : 'inflow',
                  'x_data_column' : 'date',
                  'y_data_column' : 'inflow (m3/d)',
                  'colors'        : ['#06CCD3'],
              },
        'Outflow': {
                  'x_var_name'    : 'Date',
                  'y_var_name'    : 'Outflow',
                  'data_folder'   : 'outflow',
                  'x_data_column' : 'date',
                  'y_data_column' : 'outflow (m3/d)',
                  'colors'        : ['#146698'],
          },
        'Surface Area': {
                  'x_var_name'    : 'Date',
                  'y_var_name'    : 'Surface Area',
                  'data_folder'   : 'sarea_tmsos',
                  'x_data_column' : 'date',
                  'y_data_column' : 'area (km2)',
                  'colors'        : ['#F7675E'],
                    },
        'Elevation': {
                    'x_var_name'    : 'Date',
                    'y_var_name'    : 'Elevation',
                    'data_folder'   : 'elevation',
                    'x_data_column' : 'date',
                    'y_data_column' : 'wse (m)',
                    'colors'        : ['#8A2BE2'],
                        },
        'Evaporation': {
                  'x_var_name'    : 'Date',
                  'y_var_name'    : 'Evaporation',
                  'data_folder'   : 'evaporation',
                  'x_data_column' : 'date',
                  'y_data_column' : 'evaporation (mm)',
                  'colors'        : ['green'],
                    },
        'A-E Curve'    : {
                  'x_var_name'    : 'Area Inundated',
                  'y_var_name'    : 'Elevation',
                  'data_folder'   : 'aec',
                  'x_data_column' : 'area',
                  'y_data_column' : 'elevation',
                  'colors'      : ['brown'],
                   }
    }

    def __init__(self,final_outputs, file_name, reservoir_name=None, observed_data_path=None):
        """
        Initializes the RAT_RESERVOIR instance.

        Parameters:
        - final_outputs (str): Path to the final outputs directory.
        - file_name (str): Name of the reservoir file to be plotted.
        - reservoir_name (str): Optional parameter for specifying the reservoir name.
        - observed_data_path (str): Optional parameter for specifying the path to the observed data directory. Structure should be similar to RAT's final_outputs.
        """
        self.final_outputs = final_outputs
        self.reservoir_file_name = file_name
        self.reservoir_name = reservoir_name
        self.observed_data_path = observed_data_path
    
    def plot_var(self, var_to_observe, title_for_plot, xlabel='', ylabel='', 
             x_axis_units='', y_axis_units='', x_scaling_factor=1, 
             y_scaling_factor=1, new_plot=True, width=950, height=450, 
             savepath=None, color=None, swot_method=None,
             x_col=None, y_col=None, observed_data=False, line_label=None,
             **layout_kwargs):
        """
        Plots a specified Reservoir variable using plotly. The variables that can be plotted are 
        Inflow, Outflow, Storage Change, Surface Area, Evaporation, Elevation, and A-E Curve.

        Parameters:
        - var_to_observe (str): Variable to be plotted. Acceptable values are 'Inflow', 'Outflow',
        'Storage Change', 'Surface Area', 'Evaporation', 'Elevation', and 'A-E Curve'.
        - title_for_plot (str): Title for the plot.
        - xlabel, ylabel (str): Axis labels.
        - x_axis_units, y_axis_units (str): Units for axes.
        - x_scaling_factor, y_scaling_factor (float): Scaling factors for axis data.
        - new_plot (bool): Whether to create a new plot instance.
        - width, height (int): Plot dimensions.
        - savepath (str): Optional path to save the plot as an HTML file.
        - color (str): Optional color for the line plot.
        - swot_method (str, optional): Must be one of ['sarea_based', 'elevation_based', 'elevation_sarea_based'].
        - x_col, y_col (str, optional): Column names to use for x and y.
        If None, defaults from RAT final outputs are used.
        - observed_data (bool): If True, plots observed data from observed_data_path instead of model outputs.
        - line_label (str): Optional label for the line plot.
        - **layout_kwargs: Additional keyword arguments passed directly to `fig.update_layout`.
        (e.g., xaxis_range=[0,100], yaxis_range=[0,50], legend=dict(x=0.1, y=0.9))

        Returns:
        - fig: The plotly figure instance
        """

        # --- Build file path depending on swot_method ---
        if observed_data and self.observed_data_path:
            base_path = self.observed_data_path
        elif observed_data and not self.observed_data_path:
            raise ValueError("observed_data_path for the Reservoir object must be provided when observed_data is True.")
        else:
            base_path = self.final_outputs

        if swot_method is None:
            file_path = Path(base_path) / self.rat_output_vars[var_to_observe]['data_folder'] / self.reservoir_file_name
        else:
            if swot_method not in ['sarea_based', 'elevation_based', 'elevation_sarea_based']:
                raise ValueError(f"Invalid swot_method: {swot_method}. Must be one of 'sarea_based','elevation_based','elevation_sarea_based'.")

            # Adjust data folder for Surface Area
            data_folder = self.rat_output_vars[var_to_observe]['data_folder']
            if var_to_observe == 'Surface Area':
                data_folder = data_folder.replace('tmsos', 'swot')

            file_path = Path(base_path) / 'swot' / swot_method / data_folder / self.reservoir_file_name

        # --- Check if file exists ---
        if not file_path.exists():
            print(f"Data file for {var_to_observe} using {swot_method if swot_method else 'default method'} does not exist at {file_path}.")
            return None

        # --- Read dataframe ---
        df = pd.read_csv(file_path)

        # --- Determine which columns to use ---
        x_col = x_col or self.rat_output_vars[var_to_observe]['x_data_column']
        y_col = y_col or self.rat_output_vars[var_to_observe]['y_data_column']

        # Scale data
        df[x_col] *= x_scaling_factor
        df[y_col] *= y_scaling_factor

        # --- Create figure ---
        if new_plot:
            fig = go.Figure()
        elif self.prev_fig:
            fig = self.prev_fig
        else:
            fig = go.Figure()

        line_color = color if color else self.rat_output_vars[var_to_observe]['colors'][0]

        trace = go.Scatter(
            x=df[x_col],
            y=df[y_col],
            mode='lines',
            name=line_label or var_to_observe,
            line=dict(color=line_color)
        )

        fig.add_trace(trace)

        # Layout update with user-customizable params
        fig.update_layout(
            title=title_for_plot,
            xaxis_title=xlabel + (f' {x_axis_units}' if x_axis_units else ''),
            yaxis_title=ylabel + (f' {y_axis_units}' if y_axis_units else ''),
            showlegend=True,
            width=width,
            height=height,
            **layout_kwargs  # inject user-provided layout settings
        )

        # Store previous figure for multi-trace plotting
        self.prev_fig = fig

        if savepath:
            fig.write_html(savepath)

        return fig

    def compute_correlation(self, var_to_observe, observed_y_col, start_date, end_date,
                            swot=False, x_col=None, y_col=None, correlation_method='spearman'):
        """
        Computes correlation between observed data and model/SWOT outputs
        for a specified variable.

        Parameters:
        - var_to_observe (str): Variable to compute correlation for 
        (e.g., 'Surface Area', 'Elevation', 'Storage Change').
        - observed_y_col (str): Column to use for observed data.
        - start_date, end_date (str): Date range for comparison.
        - swot (bool): If True, compute correlations for SWOT methods 
        ['sarea_based', 'elevation_based', 'elevation_sarea_based'] as well.
        - x_col, y_col (str, optional): Column names for modeled data. 
        If None, defaults from RAT final outputs are used.
        - correlation_method (str): Correlation method. It can be 'spearman' or 'pearson' or 'kendall'.
        Default is 'spearman'.
        

        Returns:
        - correlations (dict): { "source": correlation_value }
        """

        correlations = {}

        # --- Observed data ---
        if not self.observed_data_path:
            raise ValueError("observed_data_path must be set to compute correlation with observed data.")

        obs_file = Path(self.observed_data_path)  / self.rat_output_vars[var_to_observe]['data_folder'] / self.reservoir_file_name
        if not obs_file.exists():
            print(f"Observed file not found: {obs_file}")
            return {}

        obs_df = pd.read_csv(obs_file, parse_dates=["datetime"])
        obs_df = obs_df.set_index("datetime")[[observed_y_col]]
        obs_df = obs_df.loc[start_date:end_date].rename(columns={observed_y_col: "obs"})

        # --- Default model output ---
        base_file = Path(self.final_outputs) / self.rat_output_vars[var_to_observe]['data_folder'] / self.reservoir_file_name
        if base_file.exists():
            mod_df = pd.read_csv(base_file, parse_dates=[self.rat_output_vars[var_to_observe]['x_data_column']])
            x_col = x_col or self.rat_output_vars[var_to_observe]['x_data_column']
            y_col = y_col or self.rat_output_vars[var_to_observe]['y_data_column']
            mod_df = mod_df.set_index(x_col)[[y_col]].rename(columns={y_col: "pred"})
            mod_df = mod_df.loc[start_date:end_date]
            merged = obs_df.join(mod_df, how="inner").dropna()
            if not merged.empty:
                correlations["RAT-Tmsos"] = merged["obs"].corr(merged["pred"], method=correlation_method)

        # --- SWOT outputs ---
        if swot:
            methods = ['sarea_based', 'elevation_based', 'elevation_sarea_based']
            for method in methods:
                data_folder = self.rat_output_vars[var_to_observe]['data_folder']
                if var_to_observe == 'Surface Area':
                    data_folder = data_folder.replace('tmsos', 'swot')

                swot_file = Path(self.final_outputs) / "swot" / method / data_folder / self.reservoir_file_name
                if not swot_file.exists():
                    continue

                swot_df = pd.read_csv(swot_file, parse_dates=[self.rat_output_vars[var_to_observe]['x_data_column']])
                x_col = self.rat_output_vars[var_to_observe]['x_data_column']
                y_col = self.rat_output_vars[var_to_observe]['y_data_column']
                swot_df = swot_df.set_index(x_col)[[y_col]].rename(columns={y_col: "pred"})
                swot_df = swot_df.loc[start_date:end_date]
                merged = obs_df.join(swot_df, how="inner").dropna()
                if not merged.empty:
                    correlations[f"swot_{method}"] = merged["obs"].corr(merged["pred"], method=correlation_method)
            # Convert and round in place
            correlations = {k: round(float(v), 3) for k, v in correlations.items()}
        return correlations

def plot_hydrocron_data(ts_df, ts_type='wse', show_polygon_coverage=True, colorbar =True):
    """
    Creates an interactive Plotly time series plot for a reservoir showing either 'area' or 'wse'.
    Optionally overlays polygon_coverage as a color-coded marker.

    Parameters:
        ts_df (pd.DataFrame): DataFrame with ['time', 'area', 'wse', 'polygon_coverage', 'lake_id_list'].
        ts_type (str): Metric to plot ('area' or 'wse').
        show_polygon_coverage (bool): Whether to represent polygon_coverage using marker color.
    """
    if ts_type not in ['area', 'wse']:
        raise ValueError("ts_type must be 'area' or 'wse'")

    if colorbar==True:    
        hover_text = (
            ts_df['time'].astype(str) +
            f"<br>{ts_type.upper()}: " + ts_df[ts_type].round(2).astype(str)
        )

        if show_polygon_coverage and 'polygon_coverage' in ts_df.columns:
            hover_text += "<br>Polygon Coverage (%): " + ts_df['polygon_coverage'].round(1).astype(str)

            color = ts_df['polygon_coverage']
            colorscale = 'Viridis'
            showscale = True
            marker = dict(
                color=color,
                colorscale=colorscale,
                size=10,
                colorbar=dict(title="Polygon Coverage (%)") if showscale else None,
                line=dict(width=0.5, color='DarkSlateGrey'),
            )
        else:
            marker = dict(size=8, color='blue')
    
        # Create the figure
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=ts_df['time'],
            y=ts_df[ts_type],
            mode='markers+lines',
            name=ts_type.upper(),
            marker=marker,
            hovertext=hover_text,
            hoverinfo='text',
            line=dict(color='blue')
        ))

        fig.update_layout(
            title=f'SWOT Time Series - {ts_type.upper()}{" with Polygon Coverage" if show_polygon_coverage else ""}',
            xaxis_title='Time',
            yaxis_title=ts_type.upper(),
            template='plotly_white',
            hovermode='x unified',
            margin=dict(l=60, r=60, t=60, b=40)
        )

    elif show_polygon_coverage and colorbar == False:
         # Basic figure with main ts_type variable
        fig = go.Figure()

            # Add main time series trace
        fig.add_trace(go.Scatter(
            x=ts_df['time'],
            y=ts_df[ts_type],
            mode='lines+markers',
            name=ts_type.upper(),
            line=dict(color='blue'),
            yaxis='y1',
            hovertemplate='%{x}<br>' + ts_type.upper() + ': %{y:.2f}<extra></extra>'
        ))

        # Add polygon_coverage on secondary y-axis
        fig.add_trace(go.Scatter(
            x=ts_df['time'],
            y=ts_df['polygon_coverage'],
            mode='lines+markers',
            name='Polygon Coverage (%)',
            line=dict(color='rgba(59, 59, 59, 0.3)', dash='dot'),
            yaxis='y2',
            hovertemplate='%{x}<br>Coverage: %{y:.1f}%<extra></extra>'
        ))

        # Layout configuration
        fig.update_layout(
            title=f'SWOT Time Series - {ts_type.upper()} with Polygon Coverage',
            xaxis=dict(title='Time'),
            yaxis=dict(
                title=ts_type.upper(),
                titlefont=dict(color='blue'),
                tickfont=dict(color='blue')
            ),
            yaxis2=dict(
                title='Polygon Coverage (%)',
                titlefont=dict(color='#3b3b3b'),
                tickfont=dict(color='#3b3b3b'),
                overlaying='y',
                side='right',
                range=[0, 100]
            ),
            legend=dict(x=0.01, y=0.99),
            hovermode='x unified',
            template='plotly_white',
            margin=dict(l=60, r=60, t=60, b=40)
        )
        
    fig.show()

def plot_reservoir_and_prior_lakes(
    rat_reservoirs,
    lake_id_gdf,
    rat_lake_id_to_analyze,
    rat_lake_id_field,
    reservoir_name,
    save_path
):
    """
    Plots a specific RAT reservoir along with its matched prior lake polygons with distinct colors and legend.

    Parameters:
        rat_reservoirs (GeoDataFrame): GeoDataFrame containing all RAT reservoirs.
        lake_id_gdf (GeoDataFrame): GeoDataFrame of prior lakes with a 'lake_id' column.
        rat_lake_id_to_analyze (str or int): The specific RAT lake/reservoir ID to analyze and plot.
        rat_lake_id_field (str): Column name in `rat_reservoirs` that contains RAT lake/reservoir IDs.
        reservoir_name (str): Name of the reservoir.
    """

    # Filter prior lakes
    filtered_gdf = lake_id_gdf[lake_id_gdf[rat_lake_id_field] == rat_lake_id_to_analyze].copy()
    if filtered_gdf.empty:
        print(f"No prior lakes found for RAT lake ID {rat_lake_id_to_analyze}")
        return

    # Filter the reservoir
    reservoir_row = rat_reservoirs[rat_reservoirs[rat_lake_id_field] == rat_lake_id_to_analyze]
    if reservoir_row.empty:
        print(f"RAT lake ID {rat_lake_id_to_analyze} not found in RAT reservoirs.")
        return

    # Assign distinct face colors
    colors = get_distinct_colors(len(filtered_gdf))
    filtered_gdf['facecolor'] = colors

    # Start plot
    fig, ax = plt.subplots(figsize=(9,9))
    legend_handles = []

    # Plot each lake and collect legend handle
    for idx, row in filtered_gdf.iterrows():
        color = row['facecolor']
        lake_id = row['lake_id']
        gpd.GeoSeries([row.geometry]).plot(
            ax=ax, facecolor=color, edgecolor='grey', linewidth=0.5
        )
        legend_handles.append(
            Patch(facecolor=color, edgecolor='grey', label=f'Lake ID: {lake_id}')
        )

    # Plot reservoir boundary
    reservoir_row.plot(
        ax=ax, facecolor='none', edgecolor='black', linewidth=2
    )
    legend_handles.append(
        Patch(facecolor='none', edgecolor='black', label=f'Reservoir: {reservoir_name}', linewidth=2)
    )

    # Final touches
    ax.legend(handles=legend_handles, loc='best', fontsize=9)
    ax.set_title(f'Overlay: Prior Lakes vs RAT Reservoir\nReservoir: {reservoir_name}', fontsize=14)
    ax.axis('equal')
    plt.tight_layout()
    plt.savefig(save_path)
    plt.close()

def get_distinct_colors(n):
    """
    Get a list of 'n' visually distinct colors.
    If n > 20, fall back to HSL-based generation.
    """
    base_colors = list(plt.get_cmap('tab20').colors)  # 20 distinct colors
    if n <= len(base_colors):
        return base_colors[:n]
    else:
        # Generate more using HSL
        return [mcolors.hsv_to_rgb((i / n, 0.8, 0.9)) for i in range(n)]