import os
import streamlit as st
from sqlalchemy import create_engine
import pandas as pd

def get_connection_string():
    """Database kapcsolati string"""
    if 'DATABASE_URL' in st.secrets:
        return st.secrets['DATABASE_URL']
    return os.getenv('DATABASE_URL')

@st.cache_resource
def get_engine():
    """SQLAlchemy engine"""
    return create_engine(get_connection_string())

def execute_query(query):
    """SQL query futtatása"""
    engine = get_engine()
    return pd.read_sql(query, engine)