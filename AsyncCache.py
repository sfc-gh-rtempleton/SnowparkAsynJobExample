import streamlit as st
import logging
import time
from snowflake.snowpark import Session
from enum import Enum


logger = logging.getLogger("AsyncCache_logger")

class ReturnType(Enum):
    ROW = 'row'
    ROW_INTERATOR = 'row_iterator'
    PANDAS = 'pandas'
    PANDAS_BATCHES = 'pandas_batches'


class AsyncCache:

    def __init__(self):
        self._res = {}


    def addquery(self, qname: str, query: str, overwrite: bool=False):
        """
        Adds the query to the local dictionary if the qname is not found OR the hash of the cached query matches the hash of the incoming query

        :qname: The key value to search for in the local dictionary
        :query: The query to be added
        :overwrite: Force the query to be added even if a cached version exists
        """

        if overwrite:
            self.__forcequery(qname, query)
        elif qname in self._res: 
            if type(self._res[qname]) is str:
                self.__forcequery(qname, query)
            elif hash(self._res[qname].query) == hash(query) :
                logger.info(f"query {hash(query)} was found and matched")
        else: 
            self.__forcequery(qname, query)


    def __forcequery(self, qname: str, query: str):
        self._res[qname] = query
        logger.info(f"adding/updating query {hash(query)} to cache")


    def runasyncbatch(self, session: Session, forcererun: bool=False):
        """
        Given a dictionary of queries (str) or Async objects, iterate and run asynchronously 
        any queries found there. Will only rerun queries where the VALUE of the dict is a string. Existing Async objects are not rerun

        :session: The snowflake session
        :forcererun: Forces previously run AsyncJobs to rerun 
        :return: The dict after all async queries have completed.
        """
        
        count = 0
        loop = 0

        with st.spinner("Gathering information, please wait..."):
            for key, value in self._res.items():
                if type(value) is str:
                    self._res[key] = session.sql(value).collect_nowait()
                if forcererun:
                    self._res[key] = session.sql(value.query).collect_nowait()


            while True:
                for query in self._res.values():
                    if query.is_done():
                        count += 1 # increment the counter when the query is finished
                    elif loop > 5:
                        logger.info(f"query {query.query_id} is taking a long time to complete") 
                if count < len(self._res): # if the counter of finished queries is less then the total number of queries
                    time.sleep(2)
                    loop += 1
                    count = 0
                else:
                    break
                

    def result(self, qname: str, response_type: ReturnType = ReturnType.PANDAS):
        """
        Get the result from the AsyncJob in the form of the ReturnType. Defaults to Pandas dataframe if unspecified

        :qname: The name of the AsyncJob
        :response_type: Specify the format of result
        :return: The result in the form of the corresponding ReturnType
        """
        return self._res[qname].result(response_type.value)