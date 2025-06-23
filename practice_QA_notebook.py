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
    page_title="FMD "
)