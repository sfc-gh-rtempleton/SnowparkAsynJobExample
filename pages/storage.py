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

logger = logging.getLogger("storage_logger")
lutils.pageheader()
session = get_active_session()

# #############################################
# #     DATE FILTER
# #############################################
# lutils.date_filter()

if 'storage_cache' not in st.session_state:
    st.session_state.storage_cache = AsyncCache()



def rendercontainer1():

    cache = st.session_state.storage_cache

    current_storage_sql = f"select round(avg(storage_bytes + stage_bytes + failsafe_bytes) / power(1024, 4),2) as billable_tb from snowflake.account_usage.storage_usage where USAGE_DATE = current_date() -1;"
    cache.addquery('current_storage_sql', current_storage_sql)

    storage_overtime_sql = f"select date_trunc(month, usage_date) as usage_month, avg(storage_bytes + stage_bytes + failsafe_bytes) / power(1024, 4) as billable_tb, avg(storage_bytes) / power(1024, 4) as Storage_TB, avg(stage_bytes) / power(1024, 4) as Stage_TB, avg(failsafe_bytes) / power(1024, 4) as Failsafe_TB from snowflake.account_usage.storage_usage group by 1 order by 1"
    cache.addquery('storage_overtime_sql', storage_overtime_sql)

    rows_loaded_sql = f"select to_timestamp(date_trunc(day,last_load_time)) as usage_date, sum(row_count) as total_rows from snowflake.account_usage.load_history group by 1 order by usage_date desc"
    cache.addquery('rows_loaded_sql', rows_loaded_sql)

    storage_breakdown = f""" with x as (    
        select * from snowflake.account_usage.storage_usage
        unpivot (TB for TYPE in (storage_bytes, stage_bytes, failsafe_bytes, hybrid_table_storage_bytes))
        where usage_date between '{st.session_state.startingdate}' and '{st.session_state.endingdate}'
        )
        select
            usage_date date,
            replace(type, '_BYTES', '') type,
            (TB/pow(1024, 4))::number(10, 4) TB
        from x
        order by usage_date """
    cache.addquery('storage_breakdown', storage_breakdown)

    cache.runasyncbatch(session)
    

    with st.container():

        #Column formatting and metrics of header 3 metrics
        col1, col2, col3 = st.columns(3)
        col1.metric("**Current Storage (TB)**",cache.result('current_storage_sql').iloc[0].values)


        fig = px.line(cache.result('storage_breakdown'), x='DATE', y='TB', color='TYPE', title="Storage over time")
        st.plotly_chart(fig, use_container_width=True)


        #############################################
        #     Data Storage used Overtime                
        #############################################
        storage_overtime_df = cache.result('storage_overtime_sql')
        fig_storage_overtime=px.bar(storage_overtime_df,x='USAGE_MONTH',y='BILLABLE_TB', orientation='v',title="Data Storage used Overtime", barmode="group")
        st.plotly_chart(fig_storage_overtime, use_container_width=True)
        st.info('The above chart is static and not modified by the date range filter', icon="ℹ️")


        #############################################
        #     Rows Loaded Overtime (COPY INTO)                   
        #############################################
        rows_loaded_df = cache.result('rows_loaded_sql')
        fig_rows_loaded=px.line(rows_loaded_df,x='USAGE_DATE',y='TOTAL_ROWS', orientation='v',title="Rows Loaded Overtime (Copy Into)")
        st.plotly_chart(fig_rows_loaded, use_container_width=True)
        st.info('The above chart is static and not modified by the date range filter', icon="ℹ️")



if __name__ == "__main__":
    rendercontainer1()