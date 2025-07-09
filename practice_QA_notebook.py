import streamlit as st
import pandas as pd
import numpy as np
import io # Used to read uploaded CSV and Excel files in memory (no need to save to disk)
import json # Used to create downloadable JSON report of analysis results
import hashlib # Used to check for duplicate files by comparing their unique hashes
from datetime import datetime # Used only to add timestamps to JSON report and filename
import plotly.graph_objects as go # Used to create interactive anomaly and spike detection charts
import warnings # Used to turn off warning messages so they dont show in the app
warnings.filterwarnings('ignore') # From this point forward, ignore all warning messages that would normally be printed.
import base64

st.set_page_config(
    page_title="FMD DQA Tool",
    page_icon = "🔍",
    layout = "wide",
    initial_sidebar_state="auto"
)

# Open image and convert to base64 instead of st.image() for better positioning/sizing
with open("fmd.png", "rb") as f:
    data = f.read() # read into binary mode
    encoded = base64.b64encode(data).decode() # Encode data into base64 format and decode into string format for HTML

# Use st.markdown() to inject HTML to show the image
st.markdown(
    # f for dynamic variables in string
    # Embed data directly into streamlit using data:[<mediatype>][;base64],<data>
    f"""
    <div style = "text-align: center;">
        <img src = "data:image/png;base64,{encoded}" width="700"/>
    </div>
    """,
    unsafe_allow_html = True
)

class DQATool:
    def __init__(self):
        self.results = {}
        self.comparison_results = {}
    
    def detect_header_row(self, file_content, filename):
        try:
            