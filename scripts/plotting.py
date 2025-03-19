import pandas as pd
import matplotlib.pyplot as plt
from typing import Tuple, List
from .utils import get_nodes_between_dates, normalize_cycle_time
from .anomaly import detect_anomalies

class CyclePlotter:
    def __init__(self, cycles_df: pd.DataFrame, nodes: pd.DataFrame):
        self.cycles_df = cycles_df
        self.nodes = nodes

    def plot_data_for_cycles(self, cycle_ids: List[str], property_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Generate plot data for multiple cycles.
        
        Args:
            cycle_ids (List[str]): List of cycle IDs to plot
            property_name (str): Property to plot
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: Time-based and normalized data
        """
        plot_data = pd.DataFrame()
        normalized_data = pd.DataFrame()
        
        for cycle_id in cycle_ids:
            cycle_data = self.cycles_df[self.cycles_df['cycleId'] == cycle_id].copy()
            cycle_start = pd.to_datetime(cycle_data["start_date"].values[0]).strftime("%Y-%m-%d %H:%M:%S")
            cycle_end = pd.to_datetime(cycle_data["end_date"].values[0]).strftime("%Y-%m-%d %H:%M:%S")

            nodes_between_dates = get_nodes_between_dates(self.nodes.copy(), cycle_start, cycle_end)
            nodes_between_dates['cycle'] = f'Cycle {cycle_id}'
            
            normalized_cycle = normalize_cycle_time(nodes_between_dates, cycle_id)
            normalized_data = pd.concat([normalized_data, normalized_cycle])
            plot_data = pd.concat([plot_data, nodes_between_dates])
        
        time_pivot = plot_data.pivot(index='time', columns='cycle', values=property_name)
        normalized_pivot = normalized_data.pivot(index='normalized_time', 
                                              columns='cycle', 
                                              values=property_name)
        
        return time_pivot, normalized_pivot

    @staticmethod
    def create_anomaly_plot(data: pd.DataFrame, threshold: float, property_name: str) -> Tuple[plt.Figure, pd.DataFrame]:
        """Create plot with anomaly detection.
        
        Args:
            data (pd.DataFrame): Input data
            threshold (float): Anomaly detection threshold
            property_name (str): Property name for y-axis
        Returns:
            Tuple[plt.Figure, pd.DataFrame]: Figure and anomalies dataframe
        """
        fig, ax = plt.subplots(figsize=(10, 6))
        anomalies_df = pd.DataFrame()
        
        for column in data.columns:
            ax.plot(data.index, data[column], label=column)
            
            anomalies = detect_anomalies(data[column], threshold)
            anomaly_points = data[column][anomalies]
            ax.scatter(anomaly_points.index, anomaly_points.values, 
                      color='red', s=50, label=f'{column} Anomalies')

            anomaly_data = pd.DataFrame({
                'timestamp': anomaly_points.index,
                'value': anomaly_points.values,
                'cycle': column
            })
            anomalies_df = pd.concat([anomalies_df, anomaly_data]) if not anomalies_df.empty else anomaly_data
        
        ax.set_xlabel("Time")
        ax.set_ylabel(property_name)
        ax.legend()
        
        return fig, anomalies_df