import pandas as pd
from typing import List, Tuple

def time_to_seconds(time: str) -> int:
    """Convert time string to seconds.
    
    Args:
        time (str): Time string in format 'HH:MM:SS'
    Returns:
        int: Total seconds
    """
    h, m, s = map(int, time.split(':'))
    return h * 3600 + m * 60 + s

def seconds_to_time(seconds: int) -> str:
    """Convert seconds to time format.
    
    Args:
        seconds (int): Total seconds
    Returns:
        str: Formatted time string 'HH:MM'
    """
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{int(hours):02d}:{int(minutes):02d}"

def get_nodes_between_dates(data: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    """Filter data between two dates.
    
    Args:
        data (pd.DataFrame): Input dataframe
        start_date (str): Start date
        end_date (str): End date
    Returns:
        pd.DataFrame: Filtered dataframe
    """
    start_date = pd.to_datetime(start_date).strftime('%Y-%m-%d %H:%M:%S')
    end_date = pd.to_datetime(end_date).strftime('%Y-%m-%d %H:%M:%S')
    data['time'] = pd.to_datetime(data['time'], format='%y-%m-%dT%H:%M:%S.%f')
    return data[(data['time'] >= start_date) & (data['time'] <= end_date)]

def normalize_cycle_time(data: pd.DataFrame, cycle_id: str) -> pd.DataFrame:
    """Normalize time to start from 0 for each cycle.
    
    Args:
        data (pd.DataFrame): Input dataframe
        cycle_id (str): Cycle identifier
    Returns:
        pd.DataFrame: DataFrame with normalized time
    """
    cycle_data = data[data['cycle'] == f'Cycle {cycle_id}'].copy()
    cycle_data['normalized_time'] = (pd.to_datetime(cycle_data['time']) - 
                                   pd.to_datetime(cycle_data['time']).min()).dt.total_seconds()
    return cycle_data
