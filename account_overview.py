# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import plotly.express as px 
import datetime
import lutils
import logging
from AsyncCache import AsyncCache, ReturnType

logger = logging.getLogger("account_overview_logger")
lutils.pageheader()
session = get_active_session()

# #############################################
# #     DATE FILTER
# #############################################
# lutils.date_filter()

if 'overview_cache' not in st.session_state:
    st.session_state.overview_cache = AsyncCache()


def rendercontainer1():

    cache = st.session_state.overview_cache

    credits_used_sql = f"select sum(credits_billed)::number(10,3) as total_credits from snowflake.account_usage.METERING_DAILY_HISTORY where usage_date between '{st.session_state.startingdate}' and '{st.session_state.endingdate}'"
    cache.addquery('credits_used_sql', credits_used_sql)

    num_jobs_sql = f"select count(*) as number_of_jobs from snowflake.account_usage.query_history where start_time between '{st.session_state.startingdate}' and '{st.session_state.endingdate}'"
    cache.addquery('num_jobs_sql', num_jobs_sql)

    current_storage_sql = f"select (avg(storage_bytes + stage_bytes + failsafe_bytes) / power(1024, 4))::number(10,3) as billable_tb from snowflake.account_usage.storage_usage where USAGE_DATE between '{st.session_state.startingdate}' and '{st.session_state.endingdate}'"
    cache.addquery('current_storage_sql', current_storage_sql)

    credits_by_service = f"select service_type, sum(credits_billed)::number(10,3) billed_credits from SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY where usage_date between '{st.session_state.startingdate}' and '{st.session_state.endingdate}' group by 1"
    cache.addquery('credits_by_service', credits_by_service)


    daily_detailed_credits = f"""with x as (select usage_date, service_type,(zeroifnull(credits_billed))::number(10,3) credits_billed from SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY where usage_date between '{st.session_state.startingdate}' and '{st.session_state.endingdate}'),
    y as (select *
        from x
        pivot(sum(credits_billed)for service_type in (
        'AI_SERVICES',
        'AUTO_CLUSTERING',
        'COPY_FILES',
        'HYBRID_TABLE_REQUESTS',
        'MATERIALIZED_VIEW',
        'REPLICATION',
        'SEARCH_OPTIMIZATION',
        'SERVERLESS_TASK',
        'SNOWPARK_CONTAINER_SERVICES',
        'SNOWPIPE_STREAMING',
        'WAREHOUSE_METERING',
        'WAREHOUSE_METERING_READER')
        default on null (0)
        ) as z (usage_date, AI_SERVICES, AUTO_CLUSTERING, COPY_FILES, HYBRID_TABLE_REQUESTS, MATERIALIZED_VIEW, REPLICATION, SEARCH_OPTIMIZATION, SERVERLESS_TASK, SNOWPARK_CONTAINER_SERVICES, SNOWPIPE_STREAMING, WAREHOUSE_METERING, WAREHOUSE_METERING_READER)
        order by usage_date
    )
    select 
        USAGE_DATE,
        AI_SERVICES + AUTO_CLUSTERING + COPY_FILES + HYBRID_TABLE_REQUESTS + MATERIALIZED_VIEW + REPLICATION + SEARCH_OPTIMIZATION + SERVERLESS_TASK + SNOWPARK_CONTAINER_SERVICES + SNOWPIPE_STREAMING + WAREHOUSE_METERING + WAREHOUSE_METERING_READER as TOTAL_CREDITS_BILLED,
        * exclude (usage_date)
    from y
    order by 1 desc"""
    cache.addquery('daily_detailed_credits', daily_detailed_credits)

    cache.runasyncbatch(session)



    #############################################
    #     Cards at Top
    #############################################
    #Column formatting and metrics of header 3 metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("**Credits Used**", cache.result('credits_used_sql').iloc[0].values) 

    col2.metric("**Total # of Jobs Executed**","{:,}".format(int(cache.result('num_jobs_sql').iloc[0].values)))

    col3.metric("**Current Storage (TB)**", cache.result('current_storage_sql').iloc[0].values)


    st.write(' ')
    col4, col5 = st.columns(2)
    #############################################
    #     Table
    #############################################
    col4.write('**Daily spend breakdown**')
    col4.dataframe(cache.result('daily_detailed_credits').style.background_gradient(cmap='bone_r'))


    col5.write('**Credit Spend by Service Type**')
    fig = px.pie(cache.result('credits_by_service'), values='BILLED_CREDITS', names='SERVICE_TYPE', title=None)
    col5.plotly_chart(fig, use_container_width=True)

    
def rendercontainer2():

    cache = st.session_state.overview_cache

    running_total_byweek = f"""with x as(
        select date_part(week, usage_date) week, sum(credits_billed)::number(10,3) credits_billed, 
        from SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY 
        where usage_date > dateadd(year, -1, current_date())
        group by 1)
        select *, sum(credits_billed) over (order by week) running_credit_spend
        from x
        order by 1 """
    cache.addquery('running_total_byweek', running_total_byweek)
    

    montly_cost_sql = f"""select
        date_part(year, usage_date)::string || '-' || iff (len(date_part(month, usage_date)) = 1, concat('0', date_part(month, usage_date)::string), date_part(month, usage_date)::string) date,
        service_type, 
        sum(credits_billed)::number(10,2) credits_billed
    from SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY 
    where usage_date > dateadd(month, -12, current_date())
    group by 1,2 order by 1,2"""
    cache.addquery('montly_cost_sql', montly_cost_sql)

    cache.runasyncbatch(session)


    #############################################
    #     Charts
    #############################################
    #Column formatting and metrics of header 3 metrics
    st.write(" ")
    st.write("**Running Credit spend by week this year**")
    fig = px.line(cache.result('running_total_byweek'), x="WEEK", y="RUNNING_CREDIT_SPEND", markers=True)
    st.plotly_chart(fig, use_container_width=True)
    st.info("This chart does not rely on the date filter above. This shows growth over the trailing year", icon="ℹ️")


    st.write(" ")
    st.write("**Monthly Costs**")
    st.bar_chart(cache.result('montly_cost_sql'), x='DATE', y='CREDITS_BILLED', color='SERVICE_TYPE')
    st.info("This chart does not rely on the date filter above. This shows monthly costs over the trailing year", icon="ℹ️")



if __name__ == "__main__":
    rendercontainer1()
    rendercontainer2()