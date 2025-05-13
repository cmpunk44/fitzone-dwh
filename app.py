import streamlit as st
import pandas as pd
import requests
import json

st.set_page_config(
    page_title="FitZone Analytics",
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

def supabase_get(table, select="*", filter_params=None):
    """Adatok lekérése Supabase-ből"""
    url = f"{SUPABASE_URL}/rest/v1/{table}?select={select}"
    
    if filter_params:
        for key, value in filter_params.items():
            url += f"&{key}={value}"
    
    response = requests.get(url, headers=headers)
    
    if response.ok:
        return pd.DataFrame(response.json())
    else:
        st.error(f"API hiba: {response.status_code} - {response.text}")
        return pd.DataFrame()

def supabase_insert(table, data):
    """Adatok beszúrása Supabase-be"""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    
    response = requests.post(url, headers=headers, data=json.dumps(data))
    
    if response.ok:
        return True
    else:
        st.error(f"Insert hiba: {response.status_code} - {response.text}")
        return False

# Főoldal
st.title("🏋️ FitZone Analytics Dashboard")

# Sidebar
page = st.sidebar.selectbox("Navigáció", ["Áttekintés", "Tagok", "Látogatások", "Admin"])

if page == "Áttekintés":
    st.header("Áttekintés")
    
    col1, col2, col3 = st.columns(3)
    
    # Aktív tagok
    members_df = supabase_get("members", filter_params={"status": "eq.ACTIVE"})
    col1.metric("Aktív tagok", len(members_df))
    
    # Összes tag
    all_members_df = supabase_get("members")
    col2.metric("Összes tag", len(all_members_df))
    
    # Tagság típusok
    types_df = supabase_get("membership_types")
    col3.metric("Tagság típusok", len(types_df))
    
    # Grafikon - tagok státusz szerint
    if not all_members_df.empty:
        status_counts = all_members_df['status'].value_counts()
        
        import plotly.express as px
        fig = px.pie(values=status_counts.values, names=status_counts.index,
                    title="Tagok megoszlása státusz szerint")
        st.plotly_chart(fig)

elif page == "Tagok":
    st.header("Tagok kezelése")
    
    # Szűrők
    col1, col2 = st.columns(2)
    with col1:
        status_filter = st.selectbox("Státusz szűrő", ["Mind", "ACTIVE", "INACTIVE"])
    
    # Tagok lekérése
    if status_filter == "Mind":
        members_df = supabase_get("members")
    else:
        members_df = supabase_get("members", filter_params={"status": f"eq.{status_filter}"})
    
    if not members_df.empty:
        st.dataframe(members_df)
        
        # CSV letöltés
        csv = members_df.to_csv(index=False)
        st.download_button(
            label="Letöltés CSV-ként",
            data=csv,
            file_name="members.csv",
            mime="text/csv"
        )
    else:
        st.info("Nincsenek tagok")

elif page == "Látogatások":
    st.header("Látogatások")
    
    # Látogatások lekérése
    checkins_df = supabase_get("check_ins", "*, members(first_name, last_name)")
    
    if not checkins_df.empty:
        # Member adatok kicsomagolása
        if 'members' in checkins_df.columns:
            checkins_df['member_name'] = checkins_df['members'].apply(
                lambda x: f"{x['first_name']} {x['last_name']}" if x else "N/A"
            )
        
        # Utolsó 10 látogatás
        st.subheader("Legutóbbi látogatások")
        latest = checkins_df.sort_values('check_in_time', ascending=False).head(10)
        st.dataframe(latest[['member_name', 'check_in_time', 'check_out_time']])
    else:
        st.info("Még nincsenek látogatások")
    
    # Új látogatás hozzáadása
    st.subheader("Új belépés rögzítése")
    
    members_df = supabase_get("members", "member_id, first_name, last_name")
    if not members_df.empty:
        member_options = {
            f"{row['first_name']} {row['last_name']}": row['member_id'] 
            for _, row in members_df.iterrows()
        }
        
        selected_member = st.selectbox("Tag kiválasztása", options=list(member_options.keys()))
        
        if st.button("Belépés rögzítése"):
            member_id = member_options[selected_member]
            
            check_in_data = {
                "member_id": member_id,
                "check_in_time": "now()"
            }
            
            if supabase_insert("check_ins", check_in_data):
                st.success(f"✅ {selected_member} belépett!")
                st.rerun()

elif page == "Admin":
    st.header("Admin funkciók")
    
    # Minta adatok generálása
    st.subheader("Teszt adatok")
    
    if st.button("Minta tagok generálása"):
        sample_members = [
            {"first_name": "Teszt", "last_name": "Elek", "email": "teszt.elek@email.com"},
            {"first_name": "Minta", "last_name": "Béla", "email": "minta.bela@email.com"},
            {"first_name": "Próba", "last_name": "Anna", "email": "proba.anna@email.com"}
        ]
        
        success_count = 0
        for member in sample_members:
            if supabase_insert("members", member):
                success_count += 1
        
        st.success(f"✅ {success_count} tag létrehozva")
    
    # ETL műveletek
    st.subheader("ETL Műveletek")
    
    if st.button("Tag dimenzió frissítése"):
        # DWH frissítés
        members_df = supabase_get("members")
        dim_members = []
        
        for _, member in members_df.iterrows():
            dim_member = {
                "member_id": member['member_id'],
                "first_name": member['first_name'],
                "last_name": member['last_name'],
                "email": member['email'],
                "member_status": member['status'],
                "is_current": True
            }
            dim_members.append(dim_member)
        
        # Batch insert
        if dim_members:
            # Itt lehetne a dim_member táblába beszúrni
            st.success(f"✅ {len(dim_members)} rekord feldolgozva")

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("🎓 Haladó adattárház projekt")
st.sidebar.markdown("📅 2024")

# Debug info
if st.sidebar.checkbox("Debug info"):
    st.sidebar.text(f"URL: {SUPABASE_URL}")
    st.sidebar.text(f"Key: {SUPABASE_KEY[:20]}...")
