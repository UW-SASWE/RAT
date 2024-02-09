import plotly.graph_objects as go
import pandas as pd
from pathlib import Path

class RAT_RESERVOIR:
    """
    A class for representing various reservoir-related variables using Plotly.

    Attributes:
    - rat_output_vars (dict): Description of properties useful for creating plots for each reservoir variable.
    - final_outputs (str): Path to the final outputs directory.
    - file_name (str): Name of the reservoir file to be plotted.
    - reservoir_name (str): Optional parameter for specifying the reservoir name.

    Methods:
    - __init__: Initializes the RAT_RESERVOIR instance.
    - plot_var: Plots a reservoir variable from inflow, outflow, storage change, evaporation, surface area and area elevation curve.
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

    def __init__(self,final_outputs, file_name, reservoir_name=None):
        """
        Initializes the RAT_RESERVOIR instance.

        Parameters:
        - final_outputs (str): Path to the final outputs directory.
        - file_name (str): Name of the reservoir file to be plotted.
        - reservoir_name (str): Optional parameter for specifying the reservoir name.
        """
        self.final_outputs = final_outputs
        self.reservoir_file_name = file_name
        self.reservoir_name = reservoir_name
    
    def plot_var(self, var_to_observe, title_for_plot, xlabel='', ylabel='', 
                 x_axis_units='', y_axis_units='', x_scaling_factor=1, 
                 y_scaling_factor=1, new_plot=True, width=950, height=450, savepath=None):
        """
        Plots a specified Reservoir variable using plotly. The variables that can be plotted are 
        Inflow, Outflow, Storage Change, Surface Area, Evaporation and A-E Curve.

        Parameters:
        - var_to_observe (str): Variable to be plotted. Acceptable values are 'Inflow', 'Outflow',
          'Storage Change', 'Surface Area', 'Evaporation' and 'A-E Curve'.
        - title_for_plot (str): Title for the plot.
        - xlabel (str): Label for the x-axis.
        - ylabel (str): Label for the y-axis.
        - x_axis_units (str): Units for the x-axis.
        - y_axis_units (str): Units for the y-axis.
        - x_scaling_factor (float): Scaling factor for the x-axis data.
        - y_scaling_factor (float): Scaling factor for the y-axis data.
        - new_plot (bool): Whether to create a new plot instance.
        - width (int): Width of the plot.
        - height (int): Height of the plot.
        - savepath (str): Optional path to save the plot as an HTML file.

        Returns:
        -fig: The plotly figure instance
        """
        # Read dataframe 
        df = pd.read_csv(Path(self.final_outputs)/self.rat_output_vars[var_to_observe]['data_folder']/self.reservoir_file_name)

        # Scale data to change units
        df[self.rat_output_vars[var_to_observe]['x_data_column']]=df[self.rat_output_vars[var_to_observe]['x_data_column']]*x_scaling_factor
        df[self.rat_output_vars[var_to_observe]['y_data_column']]=df[self.rat_output_vars[var_to_observe]['y_data_column']]*y_scaling_factor

        # Create a figure instance if new_plot is true
        if new_plot:
            fig = go.Figure()
        elif self.prev_fig:
            fig = self.prev_fig
        else:
            fig = go.Figure()

        # Create a trace to plot
        trace = go.Scatter(x=df[self.rat_output_vars[var_to_observe]['x_data_column']], # Change x axis column 
                        y=df[self.rat_output_vars[var_to_observe]['y_data_column']], # Change y axis column,
                        mode='lines', # Line plot
                        name=var_to_observe, # Name of trace
                        line=dict(color=self.rat_output_vars[var_to_observe]['colors'][0]) # Color of line plot
                        )

        # Add the trace to the plot
        fig.add_trace(trace)

        # Update layout
        fig.update_layout(title=title_for_plot, # plot title
                        xaxis_title=xlabel+f' {x_axis_units}', # x axis label
                        yaxis_title=ylabel+f' {y_axis_units}', # y axis label
                        showlegend=True, # show legend
                        width=width, # width of plot
                        height=height) # height of plot
        
        # Saving the previous figure to add traces if needed.
        self.prev_fig = fig

        if savepath:
            # Save the plot 
            fig.write_html(savepath)
        
        return fig