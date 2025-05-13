import streamlit as st
import pandas as pd
import plotly.express as px
from src.database import execute_query

st.set_page_config(
    page_title="FitZone Analytics",
    page_icon="🏋️",
    layout="wide"
)

st.title("🏋️ FitZone Analytics Dashboard")

# Sidebar
page = st.sidebar.selectbox("Válassz oldalt", ["Áttekintés", "Tagok", "ETL"])

if page == "Áttekintés":
    st.header("Áttekintés")
    
    # KPI-k
    col1, col2, col3 = st.columns(3)
    
    try:
        # Aktív tagok
        active_members = execute_query("SELECT COUNT(*) FROM members WHERE status = 'ACTIVE'")
        col1.metric("Aktív tagok", active_members.iloc[0,0])
        
        # Összes tag
        total_members = execute_query("SELECT COUNT(*) FROM members")
        col2.metric("Összes tag", total_members.iloc[0,0])
        
        # Tagság típusok
        types = execute_query("SELECT COUNT(*) FROM membership_types")
        col3.metric("Tagság típusok", types.iloc[0,0])
        
    except Exception as e:
        st.error(f"Hiba: {e}")

elif page == "Tagok":
    st.header("Tagok")
    
    try:
        members = execute_query("SELECT * FROM members")
        st.dataframe(members)
    except Exception as e:
        st.error(f"Hiba: {e}")

elif page == "ETL":
    st.header("ETL Műveletek")
    
    if st.button("Tag dimenzió frissítése"):
        try:
            query = """
            INSERT INTO dim_member (member_id, first_name, last_name, email, member_status)
            SELECT member_id, first_name, last_name, email, status
            FROM members
            ON CONFLICT (member_id) DO NOTHING
            """
            execute_query(query)
            st.success("Kész!")
        except Exception as e:
            st.error(f"Hiba: {e}")