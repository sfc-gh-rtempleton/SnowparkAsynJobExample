import streamlit as st
import pandas as pd
import logging
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
from snowflake.snowpark.window import Window
import plotly.express as px 
import time
import datetime

logger = logging.getLogger("snowflake_dashboard_logger")

session = get_active_session()


def pageheader():
    #############################################
    #     FORMATTING
    #############################################
    #set to wide format
    st.set_page_config(layout="wide")

    # Write directly to the app
    
    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.title("Snowflake Account Usage App :snowflake:")
        

    with col2:
        #Sub heading info
        st.markdown("This app is developed to go off of the account usage schema in your Snowflake account. The data shown here may be up to 45 minutes old. For detailed information please see the documentation page below.")
        st.markdown("https://docs.snowflake.com/en/sql-reference/account-usage#account-usage-views")

    date_filter()
        
    st.divider()


def date_filter():

    if 'startingdate' not in st.session_state:
            st.session_state.startingdate = datetime.datetime.now() - datetime.timedelta(days=30)
            st.session_state.datefilter = 30

    if 'endingdate' not in st.session_state:
        st.session_state.endingdate = datetime.datetime.now()

    with st.expander(f"Date Filter", expanded=False):
        #############################################
        #     DATE FILTER
        #############################################
        max_date = datetime.datetime.now()
        min_date = datetime.datetime.now() - datetime.timedelta(days=365)

        st.markdown("Enter your desired date range (30 days on initial load):")

        #Column for Date Picker Buttons
        col1, col2, col3, col4, col5, col6 = st.columns([1,1,1,1,1, 1])

        with col1:
            if st.button('30 Days'):
                    st.session_state.startingdate = datetime.datetime.now() - datetime.timedelta(days=30)
                    st.session_state.endingdate = datetime.datetime.now()
        with col2:
            if st.button('60 Days'):
                    st.session_state.startingdate = datetime.datetime.now() - datetime.timedelta(days=60)
                    st.session_state.endingdate = datetime.datetime.now()
        with col3:
            if st.button('90 Days'):
                    st.session_state.startingdate = datetime.datetime.now() - datetime.timedelta(days=90)
                    st.session_state.endingdate = datetime.datetime.now()
        with col4:
            if st.button('180 Days'):
                    st.session_state.startingdate = datetime.datetime.now() - datetime.timedelta(days=180)
                    st.session_state.endingdate = datetime.datetime.now()
        with col5:
            if st.button('365 Days'):
                    st.session_state.startingdate = datetime.datetime.now() - datetime.timedelta(days=365)
                    st.session_state.endingdate = datetime.datetime.now()

        with col6:
            if st.button('Year to date'):
                    st.session_state.startingdate = datetime.date(datetime.datetime.now().year, 1, 1)
                    st.session_state.endingdate = datetime.datetime.now()

        #Date Input
        date_input_filter = st.date_input(
            "",
            (st.session_state.startingdate,st.session_state.endingdate),
            min_date,
            max_date,
        )

        #Start and End Date (s = start, e = end)
        st.session_state.startingdate, st.session_state.endingdate = date_input_filter
        st.session_state.datefilter = abs((st.session_state.endingdate - st.session_state.startingdate).days)
        
    # st.write(f"Filtering by last {st.session_state.datefilter} days")
    st.write(f"Filtering between dates {st.session_state.startingdate} and {st.session_state.endingdate}")
