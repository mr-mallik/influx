import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

st.set_page_config(page_title="Anomaly Detection", page_icon="📈")

# Get cycles
cycles = pd.read_csv('datasets/cycleData.csv', sep='\t')
cycle_options = cycles['cycleId'].unique().tolist()

nodes = pd.read_csv('datasets/data.csv', sep='\t')

# add date to nodes
nodes['date'] = pd.to_datetime(nodes['time'], format='%y-%m-%dT%H:%M:%S.%f').dt.strftime('%Y-%m-%d')

# st.subheader('Pre-processed data')
# st.write('The data has been pre-processed from `nodes` and `date` column has been added.')
# st.dataframe(nodes, use_container_width=True)

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

def get_nodes_between_dates(data, start_date, end_date):
    
    start_date = pd.to_datetime(start_date).strftime('%Y-%m-%d %H:%M:%S')
    end_date = pd.to_datetime(end_date).strftime('%Y-%m-%d %H:%M:%S')
    data['time'] = pd.to_datetime(data['time'], format='%y-%m-%dT%H:%M:%S.%f')
    # print(start_date, end_date)
    # print(data['time'])
    return data[(data['time'] >= start_date) & (data['time'] <= end_date)]

def normalize_cycle_time(data, cycle_id):
    """Normalize time to start from 0 for each cycle"""
    cycle_data = data[data['cycle'] == f'Cycle {cycle_id}'].copy()
    cycle_data['normalized_time'] = (pd.to_datetime(cycle_data['time']) - 
                                   pd.to_datetime(cycle_data['time']).min()).dt.total_seconds()
    return cycle_data

def st_plot_data_for_cycles(cycles_df, cycle_ids, nodes, property_name):
    plot_data = pd.DataFrame()
    normalized_data = pd.DataFrame()
    
    for cycle_id in cycle_ids:
        cycle_data = cycles_df[cycles_df['cycleId'] == cycle_id].copy()
        cycle_start = pd.to_datetime(cycle_data["start_date"].values[0]).strftime("%Y-%m-%d %H:%M:%S")
        cycle_end = pd.to_datetime(cycle_data["end_date"].values[0]).strftime("%Y-%m-%d %H:%M:%S")

        st.write("The plot is being created for cycle: ", cycle_id)
        st.write("Duration: ", cycle_start , " to ", cycle_end)

        # Get nodes between start and end date
        nodes_between_dates = get_nodes_between_dates(nodes, cycle_start, cycle_end)
        nodes_between_dates['cycle'] = f'Cycle {cycle_id}'
        
        # Create normalized time data
        normalized_cycle = normalize_cycle_time(nodes_between_dates, cycle_id)
        normalized_data = pd.concat([normalized_data, normalized_cycle])
        
        # Original time data
        plot_data = pd.concat([plot_data, nodes_between_dates])
    
    # Create two different pivot tables
    time_pivot = plot_data.pivot(index='time', columns='cycle', values=property_name)
    normalized_pivot = normalized_data.pivot(index='normalized_time', 
                                          columns='cycle', 
                                          values=property_name)
    
    return time_pivot, normalized_pivot

def detect_anomalies(data, threshold=3):
    """Detect anomalies using Z-score method"""
    mean = data.mean()
    std = data.std()
    z_scores = abs((data - mean) / std)
    return z_scores > threshold

st.subheader('Anomaly detection between cycles')
# st.write("This plot displays upto 7 days of data. The data is displayed on daily basis.")

with st.form("main_form"):
    dates_to_compare = nodes['date'].unique()[:7] # use first 7 days for comparison

    col1, col2 = st.columns(2)
    with col1:
        selected_cycle = st.multiselect('Select cycles to compare', cycle_options, default=cycle_options[:2])
    with col2:
        property_name = st.selectbox('Select property to compare', nodes.columns.difference(['time', 'date']))

    # Add threshold control
    threshold = st.slider('Anomaly detection sensitivity (Z-score threshold)', 
                            min_value=1.0, max_value=5.0, value=2.0, step=0.1)

    # col1, col2 = st.columns(2)
    # with col1:
    #     start_time = st.time_input('Start Time', value='00:00:00')
    # with col2:
    #     end_time = st.time_input('End Time', value='23:59:00')

    is_submitted = st.form_submit_button("Submit")

if is_submitted:
    with st.spinner('Generating plots...'):       
        time_data, normalized_data = st_plot_data_for_cycles(cycles, selected_cycle, nodes, property_name)
        anomalies_df = pd.DataFrame()
        
        st.subheader("Original Timeline")
        st.line_chart(time_data, use_container_width=True,
                        x_label="Time", y_label=f"{property_name}")
        
        # Modified matplotlib plot with anomalies
        fig, ax = plt.subplots(figsize=(10, 6))
        for column in time_data.columns:
            # Plot regular data
            ax.plot(time_data.index, time_data[column], label=column)
            
            # Detect and plot anomalies
            anomalies = detect_anomalies(time_data[column], threshold)
            anomaly_points = time_data[column][anomalies]
            ax.scatter(anomaly_points.index, anomaly_points.values, 
                      color='red', s=50, label=f'{column} Anomalies')

            # Store anomaly details in the dataframe
            anomaly_data = pd.DataFrame({
                'timestamp': anomaly_points.index,
                'value': anomaly_points.values,
                'cycle': column
            })
            if anomalies_df.empty:
                anomalies_df = anomaly_data
            else:
                anomalies_df = pd.concat([anomalies_df, anomaly_data])            
        
        ax.set_xlabel("Time")
        ax.set_ylabel(f"{property_name}")
        ax.legend()
        st.pyplot(fig, use_container_width=True)
            
        st.subheader("Time-shifted Comparison")
        st.line_chart(normalized_data, use_container_width=True,
                        x_label="Time (seconds from start)", y_label=f"{property_name}")
        
        # Modified matplotlib plot with anomalies for normalized data
        fig, ax = plt.subplots(figsize=(10, 6))
        for column in normalized_data.columns:
            # Plot regular data
            ax.plot(normalized_data.index, normalized_data[column], label=column)
            
            # Detect and plot anomalies
            anomalies = detect_anomalies(normalized_data[column], threshold)
            anomaly_points = normalized_data[column][anomalies]
            ax.scatter(anomaly_points.index, anomaly_points.values, 
                      color='red', s=50, label=f'{column} Anomalies')
        
        ax.set_xlabel("Time (seconds from start)")
        ax.set_ylabel(f"{property_name}")
        ax.legend()
        st.pyplot(fig, use_container_width=True)

        # Display anomaly statistics
        st.subheader("Anomaly Statistics")
        st.dataframe(anomalies_df, use_container_width=True, hide_index=True)


# footer
import datetime
version = "0.1-Prototype"
st.markdown(f"""
<p style="color:#cecece; text-align:center; font-size:12px;" >&copy {datetime.datetime.today().year} | Anomaly Detection | ECMPG | Version v{version}</p>
""", unsafe_allow_html=True)