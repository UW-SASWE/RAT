import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
import networkx as nx
import geonetworkx as gnx

G = 9.81 # m/s2

class Reservoir:
    def __init__(self, node, start_time='2000-01-01', **kwargs):
        self.node = node

        self.reservoir_breadth = 500  # m
        self.reservoir_depth = 500    # m
        self.outlet_height = 0        # m
        self.outlet_diameter = 0.1    # m
        
        self.reaction_factor = 0.01  # 1/d
        self.storage = 1e4            # m3
        self.inflow = 0               # m3/d
        self.storage_change = np.nan
        self.time = start_time if isinstance(start_time, pd.Timestamp) else pd.to_datetime(start_time)
        self.outlet_area = np.pi * (self.outlet_diameter / 2) ** 2

        # calculate derived properties
        self.water_height = self.storage / (self.reservoir_breadth * self.reservoir_depth)
        self.height_above_outlet = max([0, self.water_height - self.outlet_height]) # return height above outlet, or 0 if below outlet
        self.outlet_velocity = np.sqrt(2 * G * self.height_above_outlet)
        self.outlet_flow = self.outlet_area * self.outlet_velocity

        self.FIRST_RUN = 1

    def update(self, inflow, algorithm='linear_reservoir', dt=1):
        """update for one time step

        Args:
            inflow (number): inflow rate (m3/d)
            dt (number): time step (1 day)
        """
        # prev_time = self.time
        # self.time += pd.Timedelta(seconds=dt)
        if self.FIRST_RUN:
            self.FIRST_RUN = 0  # if this is the first run, don't advance time
        else:
            self.time += pd.Timedelta(days=dt)
        
        self.inflow = inflow
        
        if algorithm == 'outlet':
            self._alg_outlet(dt)
        
        if algorithm == 'linear_reservoir':
            self._alg_linear_reservoir(dt)

    def _alg_linear_reservoir(self, dt):
        self.outflow = self.reaction_factor * self.storage
        self.storage_change = self.inflow - self.outflow
        self.storage += self.storage_change * dt

        return {
            'inflow': self.inflow,
            'outflow': self.outflow,
            'storage': self.storage,
            'storage_change': self.storage_change,
        }

    def _alg_outlet(self, dt):
        last_storage = self.storage
        self.storage += self.inflow * dt

        # using water height method
        self.water_height = self.storage / (self.reservoir_breadth * self.reservoir_depth)
        self.height_above_outlet = max([0, self.water_height - self.outlet_height])

        self.outlet_velocity = np.sqrt(2 * G * self.height_above_outlet) # m/s
        self.outflow = self.outlet_area * self.outlet_velocity # m3/d

        self.storage -= self.outflow * dt # update storage again after outflow is calcualted
        self.storage_change = self.storage - last_storage # calculate storage change
    
    def dumpds(self):
        return {
            'inflow': self.inflow,
            'outflow': self.outflow,
            'storage': self.storage,
            'storage_change': self.storage_change,
            'water_height': self.water_height,
            'height_above_outlet': self.height_above_outlet,
        }