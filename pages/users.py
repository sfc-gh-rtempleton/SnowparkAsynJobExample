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

if 'users_cache' not in st.session_state:
    st.session_state.users_cache = AsyncCache()



def rendercontainer1():

    cache = st.session_state.users_cache

    active_users_sql = f"select count(*) count from SNOWFLAKE.ACCOUNT_USAGE.USERS where deleted_on is null and disabled = 'false'"
    cache.addquery('active_users_sql', active_users_sql)

    svc_users_sql = f"select count(*) from SNOWFLAKE.ACCOUNT_USAGE.USERS where deleted_on is null and disabled = 'false' and login_name like '%$%'"
    cache.addquery('svc_users_sql', svc_users_sql)

    disabled_users_sql = f"select count(*) from SNOWFLAKE.ACCOUNT_USAGE.USERS where deleted_on is null and (disabled = 'true' or LAST_SUCCESS_LOGIN < DATEADD(year, -1, current_date()))"
    cache.addquery('disabled_users_sql', disabled_users_sql)

    logins_sql = f"select user_name, sum(iff(is_success = 'NO', 1, 0)) as Failed, count(*)-Failed as Success from snowflake.account_usage.login_history where event_timestamp between '{st.session_state.startingdate}' and '{st.session_state.endingdate}' group by 1 order by 2 desc limit 10"
    cache.addquery('logins_sql', logins_sql)

    logins_client_sql = f"select reported_client_type as Client, user_name, sum(iff(is_success = 'NO', 1, 0)) as Failed, count(*)-Failed as Success from snowflake.account_usage.login_history where event_timestamp between '{st.session_state.startingdate}' and '{st.session_state.endingdate}' group by 1, 2 order by 4 desc limit 10"
    cache.addquery('logins_client_sql', logins_client_sql)

    suspicious_ip_users_sql = f"""with x as (
    select distinct user_name, client_ip from SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY where event_timestamp between '{st.session_state.startingdate}' and '{st.session_state.endingdate}'
    )
    select
        x.user_name,
        count(client_ip) different_ip_logins
    from
        x
        inner join SNOWFLAKE.ACCOUNT_USAGE.USERS u
        on x.user_name = u.name
    where true
        and u.deleted_on is null 
        and u.disabled = 'false'
    group by 1
    order by 2 desc
    limit 10"""
    cache.addquery('suspicious_ip_users_sql', suspicious_ip_users_sql)
    
    suspicious_failed_logins_sql = f"""select l.user_name, count(*) failed_logins from SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY l inner join SNOWFLAKE.ACCOUNT_USAGE.USERS u on l.user_name = u.name where true and u.deleted_on is null and u.disabled = 'false' and l.is_success = 'NO' and event_timestamp between '{st.session_state.startingdate}' and '{st.session_state.endingdate}' group by 1 order by 2 desc limit 10"""
    cache.addquery('suspicious_failed_logins_sql', suspicious_failed_logins_sql)
    
    cache.runasyncbatch(session)

    ##############

    active_users_cnt = cache.result('active_users_sql').iloc[0].values
    service_users_cnt = cache.result('svc_users_sql').iloc[0].values
    disabled_users_cnt = cache.result('disabled_users_sql').iloc[0].values

    #Column formatting and metrics of header 3 metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("**Total Active Users**","{:,}".format(int(active_users_cnt)), help="Total number of non deleted or disabled users in the Account") 
    col2.metric("**Service Users**","{:,}".format(int(service_users_cnt)), help="Total number of users who have $ in their user_name")
    col3.metric("**Count Disabled/Defunct users**","{:,}".format(int(disabled_users_cnt)), help="Number of users who are disabled or haven't logged in within the last year")


    container_users = st.container()

    with container_users:
        plot1, plot2 = st.columns(2)
        with plot1:
            st.write("     ")
            st.write("**Logins by User**")
            fig = px.bar(cache.result('logins_sql'), x='USER_NAME', y=['SUCCESS', 'FAILED'])
            st.plotly_chart(fig, use_container_width=True)

            st.write("     ")
            st.write("**Logins by different IPs**")
            suspicious_ip_users_df = cache.result('suspicious_ip_users_sql')
            fig = px.bar(cache.result('suspicious_ip_users_sql'), x='USER_NAME',y='DIFFERENT_IP_LOGINS')
            fig.update_traces(marker_color='orange')
            st.plotly_chart(fig, use_container_width=True)
            st.info("Users logging in from numerous IP addresses could indicate hackers attempting to access the account")
        with plot2:
            st.write("     ")
            st.write("**Logins by Client Type**")
            fig = px.bar(cache.result('logins_client_sql'), x='CLIENT',y='SUCCESS')
            fig.update_traces(marker_color='red')
            st.plotly_chart(fig, use_container_width=True)


            st.write("     ")
            st.write("**Number of failed logins**")
            fig = px.bar(cache.result('suspicious_failed_logins_sql'), x='USER_NAME',y='FAILED_LOGINS')
            fig.update_traces(marker_color='LightSkyBlue')
            st.plotly_chart(fig, use_container_width=True)
            st.info("Users with numerous failed login attempts is suspicious")


if __name__ == "__main__":
    rendercontainer1()