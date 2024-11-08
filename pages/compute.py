# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
from snowflake.snowpark.window import Window
import plotly.express as px 
import datetime
import lutils
import logging
from AsyncCache import AsyncCache, ReturnType

logger = logging.getLogger("snowflake_dashboard_logger")
lutils.pageheader()
session = get_active_session()


# #############################################
# #     DATE FILTER
# #############################################
# lutils.date_filter()

if 'compute_cache' not in st.session_state:
    st.session_state.compute_cache = AsyncCache()


#############################################
#     Container 1: Credits & Jobs
#############################################

def rendercontainer1():

    cache = st.session_state.compute_cache

    ### Add your queries to the cache
    total_credits_used_sql = f"select warehouse_name,sum(credits_used) as total_credits_used from snowflake.account_usage.warehouse_metering_history where start_time between '{st.session_state.startingdate}' and '{st.session_state.endingdate}' group by 1 order by 2 desc limit 10 "
    cache.addquery('total_credits_used_sql', total_credits_used_sql)

    jobs_by_warehouse_sql = f"select warehouse_name,count(*) as number_of_jobs from snowflake.account_usage.query_history where start_time between '{st.session_state.startingdate}' and '{st.session_state.endingdate}' group by 1 order by 2 desc limit 10"
    cache.addquery('jobs_by_warehouse_sql', jobs_by_warehouse_sql)

    execution_by_qtype = f"select query_type, warehouse_size, avg(execution_time) / 1000 as average_execution_time from snowflake.account_usage.query_history where start_time between '{st.session_state.startingdate}' and '{st.session_state.endingdate}' group by 1, 2 order by 3 desc;"
    cache.addquery('execution_by_qtype', execution_by_qtype)

    ### Process the queries in the cache
    cache.runasyncbatch(session)

    ### Retrieve the results from the cache
    res1 = cache.result('total_credits_used_sql')
    res2 = cache.result('execution_by_qtype')
    res3 = cache.result('jobs_by_warehouse_sql')

    c1, c2 = st.columns(2)
    fig_credits_used=px.bar(res1 ,x='TOTAL_CREDITS_USED',y='WAREHOUSE_NAME',orientation='h',title="Credits Used by Warehouse")
    fig_credits_used.update_traces(marker_color='green')
    c1.plotly_chart(fig_credits_used, use_container_width=True)

    fig_execution_by_qtype=px.bar(res2, x='AVERAGE_EXECUTION_TIME',y='QUERY_TYPE',orientation='h',title="Average Execution by Query Type")
    c1.plotly_chart(fig_execution_by_qtype, use_container_width=True)

    fig_jobs_by_warehouse=px.bar(res3 ,x='NUMBER_OF_JOBS',y='WAREHOUSE_NAME',orientation='h',title="# of Jobs by Warehouse")
    fig_jobs_by_warehouse.update_traces(marker_color='purple')
    c2.plotly_chart(fig_jobs_by_warehouse, use_container_width=True)
            

if __name__ == "__main__":
    rendercontainer1()