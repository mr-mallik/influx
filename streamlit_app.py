import streamlit as st
import pandas as pd
import datetime
from scripts.plotting import CyclePlotter

# Page configuration
st.set_page_config(page_title="Anomaly Detection", page_icon="📈")

# Load data
@st.cache_data
def load_data():
    cycles = pd.read_csv('datasets/cycleData.csv', sep='\t')
    nodes = pd.read_csv('datasets/data.csv', sep='\t')
    nodes['date'] = pd.to_datetime(nodes['time'], format='%y-%m-%dT%H:%M:%S.%f').dt.strftime('%Y-%m-%d')
    return cycles, nodes

cycles, nodes = load_data()
cycle_options = cycles['cycleId'].unique().tolist()

# Main application
st.subheader('Anomaly detection between cycles')

with st.form("main_form"):
    col1, col2 = st.columns(2)
    with col1:
        selected_cycle = st.multiselect('Select cycles to compare', cycle_options, default=cycle_options[:2])
    with col2:
        property_name = st.selectbox('Select property to compare', nodes.columns.difference(['time', 'date']))

    threshold = st.slider('Anomaly detection sensitivity (Z-score threshold)', 
                         min_value=1.0, max_value=5.0, value=2.0, step=0.1)

    is_submitted = st.form_submit_button("Submit")

if is_submitted:
    with st.spinner('Generating plots...'):
        plotter = CyclePlotter(cycles, nodes)
        time_data, normalized_data = plotter.plot_data_for_cycles(selected_cycle, property_name)
        
        # Original timeline plots
        st.subheader("Original Timeline")
        st.line_chart(time_data, use_container_width=True)
        fig, anomalies_df = plotter.create_anomaly_plot(time_data, threshold, property_name)
        st.pyplot(fig, use_container_width=True)
        
        # Time-shifted comparison plots
        st.subheader("Time-shifted Comparison")
        st.line_chart(normalized_data, use_container_width=True)
        fig, normalized_anomalies = plotter.create_anomaly_plot(normalized_data, threshold, property_name)
        st.pyplot(fig, use_container_width=True)

        # Anomaly statistics
        st.subheader("Anomaly Statistics")
        st.dataframe(anomalies_df, use_container_width=True, hide_index=True)

# Footer
version = "0.1-Prototype"
st.markdown(f"""
<p style="color:#cecece; text-align:center; font-size:12px;">
    &copy {datetime.datetime.today().year} | Anomaly Detection | ECMPG | Version v{version}
</p>
""", unsafe_allow_html=True)