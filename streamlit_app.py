import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

nodes = pd.read_csv('datasets/data.csv', sep='\t')

# add date to nodes
nodes['date'] = pd.to_datetime(nodes['time'], format='%y-%m-%dT%H:%M:%S.%f').dt.strftime('%Y-%m-%d')

st.subheader('Pre-processed data')
st.write('The data has been pre-processed from `nodes` and `date` column has been added.')
st.dataframe(nodes, use_container_width=True)

# convert time to seconds
def time_to_seconds(time):
    h, m, s = map(int, time.split(':'))
    return h * 3600 + m * 60 + s

# Convert seconds to time format for better readability
def seconds_to_time(seconds):
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{int(hours):02d}:{int(minutes):02d}"

def st_plot_data_for_dates(data, dates, property_name, start_time=None, end_time=None):
    if len(dates) > 7:
        raise ValueError("Can only compare up to 7 days")
    
    plot_data = pd.DataFrame()
    
    for date in dates:
        day_data = data[data['date'] == date].copy()
        
        # Filter by time range if specified
        if start_time and end_time:
            day_seconds = [time_to_seconds(t.split('T')[1].split('.')[0]) for t in day_data['time']]
            start_seconds = start_time.hour * 3600 + start_time.minute * 60 + start_time.second
            end_seconds = end_time.hour * 3600 + end_time.minute * 60 + end_time.second
            
            time_mask = [(s >= start_seconds) & (s <= end_seconds) for s in day_seconds]
            day_data = day_data[time_mask]
            day_seconds = [s for s, m in zip(day_seconds, time_mask) if m]
        else:
            day_seconds = [time_to_seconds(t.split('T')[1].split('.')[0]) for t in day_data['time']]
        
        day_data['seconds'] = day_seconds
        day_data = day_data[['seconds', property_name]]
        day_data = day_data.rename(columns={property_name: f'{date}'})
        
        if plot_data.empty:
            plot_data = day_data
        else:
            plot_data = plot_data.merge(day_data, on='seconds', how='outer')
    
    plot_data = plot_data.sort_values(by='seconds')
    plot_data = plot_data.set_index('seconds')

    return plot_data

st.subheader('Plot the data for comparison')
st.write("This plot displays upto 7 days of data. The data is displayed on daily basis.")

with st.form("main_form"):
    dates_to_compare = nodes['date'].unique()[:7] # use first 7 days for comparison

    property_name = st.selectbox('Select property to compare', nodes.columns.difference(['time', 'date']))

    col1, col2 = st.columns(2)
    with col1:
        start_time = st.time_input('Start Time', value='00:00:00')
    with col2:
        end_time = st.time_input('End Time', value='23:59:00')

    is_submitted = st.form_submit_button("Submit")

if is_submitted:
    with st.spinner('Generating plot...'):
        plot_data = st_plot_data_for_dates(nodes, dates_to_compare, property_name, 
                                    start_time, end_time)
        
        colors = ['#0000FF', '#FF0000', '#008000', '#00FFFF', '#FF00FF', '#FFFF00', '#000000']  # Hex colors for up to 7 days

        st.line_chart(plot_data, x_label="Time in (24 hour format)", 
                      y_label=f"{property_name}", color=colors[:len(dates_to_compare)], use_container_width=True)