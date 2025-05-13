import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="FitZone Analytics", page_icon="🏋️")

st.title("🏋️ FitZone Analytics Dashboard")

# Debug információ
st.sidebar.header("Debug Info")
if st.sidebar.checkbox("Show connection info"):
    # NE mutasd a teljes connection stringet!
    conn_str = st.secrets.get("DATABASE_URL", "Not found")
    if conn_str != "Not found":
        st.sidebar.text(f"DB URL létezik: {len(conn_str)} karakter")
        st.sidebar.text(f"Starts with: {conn_str[:20]}...")
    else:
        st.sidebar.error("DATABASE_URL not found in secrets!")

# Kapcsolat teszt
try:
    from sqlalchemy import create_engine
    engine = create_engine(st.secrets["DATABASE_URL"])
    
    # Egyszerű teszt query
    result = pd.read_sql("SELECT version()", engine)
    st.success("✅ Adatbázis kapcsolat OK!")
    st.write(result)
    
except Exception as e:
    st.error("❌ Adatbázis kapcsolat hiba!")
    st.error(str(e))
    st.info("Ellenőrizd:")
    st.info("1. Supabase projekt aktív?")
    st.info("2. Connection string helyes?")
    st.info("3. Jelszó nem tartalmaz spec karaktert?")
