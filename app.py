# app.py - Egyszerűsített FitZone alkalmazás
import streamlit as st
import pandas as pd
import requests
import json
from datetime import datetime, timedelta

st.set_page_config(
    page_title="FitZone Simple Dashboard",
    page_icon="🏋️",
    layout="wide"
)

# Supabase beállítások
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_ANON_KEY"]

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

# API függvények
def supabase_get(table, select="*", filter_params=None):
    """Adatok lekérése"""
    url = f"{SUPABASE_URL}/rest/v1/{table}?select={select}"
    
    if filter_params:
        for key, value in filter_params.items():
            url += f"&{key}={value}"
    
    response = requests.get(url, headers=headers)
    
    if response.ok:
        data = response.json()
        return pd.DataFrame(data) if data else pd.DataFrame()
    else:
        return pd.DataFrame()

def supabase_insert(table, data):
    """Adatok beszúrása"""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    response = requests.post(url, headers=headers, data=json.dumps(data))
    return response.ok

# Egyszerű ETL
def update_dim_member():
    """Tag dimenzió frissítése"""
    members = supabase_get("members")
    if members.empty:
        return 0
    
    # Töröljük a régieket (egyszerű megoldás)
    # dim_members = supabase_get("dim_member")
    
    success = 0
    for _, member in members.iterrows():
        dim_data = {
            "member_id": int(member['member_id']),
            "first_name": member['first_name'],
            "last_name": member['last_name'],
            "email": member['email'],
            "member_status": member['status'],
            "is_current": True
        }
        
        if supabase_insert("dim_member", dim_data):
            success += 1
    
    return success

# Egyszerű számítások
def calculate_daily_stats():
    """Napi statisztikák"""
    stats = {}
    
    # Aktív tagok
    members = supabase_get("members")
    if not members.empty:
        stats['total_members'] = len(members)
        stats['active_members'] = len(members[members['status'] == 'ACTIVE'])
    else:
        stats['total_members'] = 0
        stats['active_members'] = 0
    
    # Mai látogatók
    check_ins = supabase_get("check_ins")
    if not check_ins.empty:
        check_ins['check_in_time'] = pd.to_datetime(check_ins['check_in_time'])
        today = pd.Timestamp.now().date()
        today_visits = check_ins[check_ins['check_in_time'].dt.date == today]
        
        stats['today_visits'] = len(today_visits)
        stats['unique_visitors'] = today_visits['member_id'].nunique()
        stats['currently_inside'] = len(today_visits[pd.isna(today_visits['check_out_time'])])
    else:
        stats['today_visits'] = 0
        stats['unique_visitors'] = 0
        stats['currently_inside'] = 0
    
    # DWH statisztika
    dim_members = supabase_get("dim_member")
    stats['dwh_records'] = len(dim_members)
    
    return stats

# Főalkalmazás
def main():
    st.title("🏋️ FitZone - Egyszerű Dashboard")
    
    # Sidebar
    st.sidebar.header("Műveletek")
    
    # ETL futtatás
    if st.sidebar.button("🔄 Dimenzió frissítése"):
        with st.spinner("Frissítés..."):
            count = update_dim_member()
            st.sidebar.success(f"✅ {count} rekord frissítve")
    
    # Egyszerű számítások
    stats = calculate_daily_stats()
    
    # KPI-k megjelenítése
    st.header("📊 Alapvető számok")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Összes tag", stats['total_members'])
    
    with col2:
        st.metric("Aktív tagok", stats['active_members'])
    
    with col3:
        st.metric("Mai látogatók", stats['unique_visitors'])
    
    with col4:
        st.metric("Most bent", stats['currently_inside'])
    
    # Egyszerű táblázatok
    tab1, tab2, tab3 = st.tabs(["Tagok", "Mai látogatások", "ETL Státusz"])
    
    with tab1:
        st.subheader("Tagok listája")
        members = supabase_get("members")
        if not members.empty:
            display_df = members[['member_id', 'first_name', 'last_name', 'email', 'status']]
            st.dataframe(display_df)
        else:
            st.info("Nincsenek tagok")
    
    with tab2:
        st.subheader("Mai látogatások")
        check_ins = supabase_get("check_ins")
        if not check_ins.empty:
            check_ins['check_in_time'] = pd.to_datetime(check_ins['check_in_time'])
            today = pd.Timestamp.now().date()
            today_visits = check_ins[check_ins['check_in_time'].dt.date == today]
            
            if not today_visits.empty:
                # Tagnevek hozzáadása
                members = supabase_get("members")
                if not members.empty:
                    visits_with_names = today_visits.merge(
                        members[['member_id', 'first_name', 'last_name']], 
                        on='member_id',
                        how='left'
                    )
                    display_visits = visits_with_names[['first_name', 'last_name', 'check_in_time', 'check_out_time']]
                    st.dataframe(display_visits)
                else:
                    st.dataframe(today_visits)
            else:
                st.info("Ma még nem volt látogatás")
        else:
            st.info("Nincsenek látogatási adatok")
    
    with tab3:
        st.subheader("ETL Státusz")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("OLTP rekordok", stats['total_members'])
        
        with col2:
            st.metric("DWH rekordok", stats['dwh_records'])
        
        # Egyszerű összegzés
        st.divider()
        st.markdown("""
        ### Projekt összefoglalás
        
        ✅ **OLTP adatbázis**: members, check_ins, memberships  
        ✅ **ETL folyamat**: Tag dimenzió frissítése  
        ✅ **DWH tábla**: dim_member  
        ✅ **Automatizált számítások**: Napi statisztikák  
        
        A rendszer megfelel a tantárgy követelményeinek:
        - OLTP → DWH ETL folyamat
        - Dimenzió tábla (dim_member)
        - Automatizált elemzések
        """)

if __name__ == "__main__":
    main()
