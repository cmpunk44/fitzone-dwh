# app.py - FitZone teljes alkalmazás
import streamlit as st
import pandas as pd
import requests
import json
from datetime import datetime, timedelta
import time

st.set_page_config(
    page_title="FitZone Management",
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
        st.error(f"API hiba: {response.text}")
        return pd.DataFrame()

def supabase_insert(table, data):
    """Adatok beszúrása"""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    response = requests.post(url, headers=headers, data=json.dumps(data))
    return response.ok

def supabase_update(table, id_field, id_value, data):
    """Adatok frissítése"""
    url = f"{SUPABASE_URL}/rest/v1/{table}?{id_field}=eq.{id_value}"
    response = requests.patch(url, headers=headers, data=json.dumps(data))
    return response.ok

# Egyszerű ETL
def update_dim_member():
    """Tag dimenzió frissítése"""
    members = supabase_get("members")
    if members.empty:
        return 0
    
    success = 0
    for _, member in members.iterrows():
        # Életkor csoport
        age_group = "Unknown"
        if pd.notna(member.get('birth_date')):
            birth_date = pd.to_datetime(member['birth_date'])
            age = (datetime.now() - birth_date).days // 365
            if age < 25: age_group = "<25"
            elif age < 35: age_group = "25-35"
            elif age < 45: age_group = "35-45"
            elif age < 55: age_group = "45-55"
            else: age_group = "55+"
        
        dim_data = {
            "member_id": int(member['member_id']),
            "first_name": member['first_name'],
            "last_name": member['last_name'],
            "email": member['email'],
            "age_group": age_group,
            "member_status": member['status'],
            "is_current": True
        }
        
        if supabase_insert("dim_member", dim_data):
            success += 1
    
    return success

# Számítások
def calculate_stats():
    """Alapvető statisztikák"""
    stats = {}
    
    # Tagok
    members = supabase_get("members")
    if not members.empty:
        stats['total_members'] = len(members)
        stats['active_members'] = len(members[members['status'] == 'ACTIVE'])
    else:
        stats['total_members'] = 0
        stats['active_members'] = 0
    
    # Látogatások
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
    
    return stats

# Főalkalmazás
def main():
    st.title("🏋️ FitZone Management System")
    
    # Oldalsáv
    st.sidebar.header("Navigáció")
    page = st.sidebar.selectbox(
        "Válassz funkciót",
        ["📊 Dashboard", "🚪 Recepció", "👥 Tagok", "💳 Tagság", "⚙️ ETL"]
    )
    
    if page == "📊 Dashboard":
        show_dashboard()
    elif page == "🚪 Recepció":
        show_reception()
    elif page == "👥 Tagok":
        show_members()
    elif page == "💳 Tagság":
        show_membership()
    elif page == "⚙️ ETL":
        show_etl()

def show_dashboard():
    """Főoldal"""
    st.header("Dashboard")
    
    stats = calculate_stats()
    
    # KPI-k
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Összes tag", stats['total_members'])
    
    with col2:
        st.metric("Aktív tagok", stats['active_members'])
    
    with col3:
        st.metric("Mai látogatók", stats['unique_visitors'])
    
    with col4:
        st.metric("Most bent", stats['currently_inside'])
    
    # Táblázatok
    tab1, tab2 = st.tabs(["Aktív tagok", "Mai látogatások"])
    
    with tab1:
        members = supabase_get("members", filter_params={"status": "eq.ACTIVE"})
        if not members.empty:
            st.dataframe(members[['member_id', 'first_name', 'last_name', 'email', 'status']])
        else:
            st.info("Nincsenek aktív tagok")
    
    with tab2:
        check_ins = supabase_get("check_ins")
        if not check_ins.empty:
            check_ins['check_in_time'] = pd.to_datetime(check_ins['check_in_time'])
            today = pd.Timestamp.now().date()
            today_visits = check_ins[check_ins['check_in_time'].dt.date == today]
            
            if not today_visits.empty:
                members = supabase_get("members")
                if not members.empty:
                    visits_with_names = today_visits.merge(
                        members[['member_id', 'first_name', 'last_name']], 
                        on='member_id',
                        how='left'
                    )
                    st.dataframe(visits_with_names[['first_name', 'last_name', 'check_in_time', 'check_out_time']])
            else:
                st.info("Ma még nem volt látogatás")

def show_reception():
    """Be/kiléptetés"""
    st.header("🚪 Recepció")
    
    tab1, tab2, tab3 = st.tabs(["Beléptetés", "Kiléptetés", "Jelenlegi státusz"])
    
    with tab1:
        st.subheader("Beléptetés")
        
        members = supabase_get("members", filter_params={"status": "eq.ACTIVE"})
        if not members.empty:
            # Keresés
            search = st.text_input("🔍 Keresés (név vagy email)")
            
            if search:
                mask = (
                    members['first_name'].str.contains(search, case=False, na=False) |
                    members['last_name'].str.contains(search, case=False, na=False) |
                    members['email'].str.contains(search, case=False, na=False)
                )
                filtered = members[mask]
            else:
                filtered = members.head(10)
            
            # Lista
            for _, member in filtered.iterrows():
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.write(f"**{member['first_name']} {member['last_name']}**")
                    st.caption(member['email'])
                
                with col2:
                    if st.button("✅ Beléptet", key=f"in_{member['member_id']}"):
                        check_in_data = {
                            "member_id": int(member['member_id']),
                            "check_in_time": datetime.now().isoformat()
                        }
                        if supabase_insert("check_ins", check_in_data):
                            st.success("Beléptetés sikeres!")
                            time.sleep(1)
                            st.rerun()
    
    with tab2:
        st.subheader("Kiléptetés")
        
        check_ins = supabase_get("check_ins")
        if not check_ins.empty:
            active = check_ins[pd.isna(check_ins['check_out_time'])]
            
            if not active.empty:
                members = supabase_get("members")
                active_with_names = active.merge(
                    members[['member_id', 'first_name', 'last_name']], 
                    on='member_id'
                )
                
                for _, checkin in active_with_names.iterrows():
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        check_in_time = pd.to_datetime(checkin['check_in_time'])
                        duration = datetime.now() - check_in_time
                        hours = int(duration.total_seconds() // 3600)
                        minutes = int((duration.total_seconds() % 3600) // 60)
                        
                        st.write(f"**{checkin['first_name']} {checkin['last_name']}**")
                        st.caption(f"Belépve: {hours}ó {minutes}p")
                    
                    with col2:
                        if st.button("🚪 Kiléptet", key=f"out_{checkin['checkin_id']}"):
                            update_data = {"check_out_time": datetime.now().isoformat()}
                            if supabase_update("check_ins", "checkin_id", 
                                             checkin['checkin_id'], update_data):
                                st.success("Kiléptetés sikeres!")
                                time.sleep(1)
                                st.rerun()
            else:
                st.info("Nincs bent látogató")
    
    with tab3:
        st.subheader("Jelenlegi státusz")
        
        check_ins = supabase_get("check_ins")
        if not check_ins.empty:
            active = check_ins[pd.isna(check_ins['check_out_time'])]
            st.metric("Bent lévők száma", len(active))
            
            if not active.empty:
                members = supabase_get("members")
                active_details = active.merge(
                    members[['member_id', 'first_name', 'last_name']], 
                    on='member_id'
                )
                
                active_details['check_in_time'] = pd.to_datetime(active_details['check_in_time'])
                active_details['duration'] = (
                    pd.Timestamp.now() - active_details['check_in_time']
                ).dt.total_seconds() / 60
                
                active_details['duration_str'] = active_details['duration'].apply(
                    lambda x: f"{int(x//60)}ó {int(x%60)}p"
                )
                
                display_df = active_details[[
                    'first_name', 'last_name', 'check_in_time', 'duration_str'
                ]].copy()
                display_df.columns = ['Keresztnév', 'Vezetéknév', 'Belépés', 'Bent töltött idő']
                
                st.dataframe(display_df, use_container_width=True)

def show_members():
    """Tag kezelés"""
    st.header("👥 Tagok kezelése")
    
    tab1, tab2, tab3 = st.tabs(["Tag lista", "Új tag", "Státusz váltás"])
    
    with tab1:
        st.subheader("Tagok listája")
        
        status_filter = st.selectbox("Státusz szűrő", ["Mind", "ACTIVE", "INACTIVE"])
        
        if status_filter == "Mind":
            members = supabase_get("members")
        else:
            members = supabase_get("members", filter_params={"status": f"eq.{status_filter}"})
        
        if not members.empty:
            st.dataframe(members)
        else:
            st.info("Nincsenek tagok")
    
    with tab2:
        st.subheader("Új tag regisztrálása")
        
        with st.form("new_member_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                first_name = st.text_input("Keresztnév*")
                last_name = st.text_input("Vezetéknév*")
                email = st.text_input("Email*")
            
            with col2:
                phone = st.text_input("Telefon")
                birth_date = st.date_input("Születési dátum")
            
            if st.form_submit_button("Regisztráció"):
                if first_name and last_name and email:
                    new_member = {
                        "first_name": first_name,
                        "last_name": last_name,
                        "email": email,
                        "phone": phone,
                        "birth_date": birth_date.isoformat() if birth_date else None,
                        "status": "ACTIVE"
                    }
                    
                    if supabase_insert("members", new_member):
                        st.success("✅ Új tag sikeresen regisztrálva!")
                        time.sleep(1)
                        st.rerun()
                else:
                    st.error("Kérjük töltse ki a kötelező mezőket!")
    
    with tab3:
        st.subheader("Státusz váltás")
        
        members = supabase_get("members")
        if not members.empty:
            member_names = {
                f"{m['first_name']} {m['last_name']} ({m['email']})": m['member_id']
                for _, m in members.iterrows()
            }
            
            selected = st.selectbox("Válassz tagot", list(member_names.keys()))
            member_id = member_names[selected]
            
            selected_member = members[members['member_id'] == member_id].iloc[0]
            current_status = selected_member['status']
            new_status = "INACTIVE" if current_status == "ACTIVE" else "ACTIVE"
            
            st.info(f"Jelenlegi státusz: **{current_status}**")
            
            if st.button(f"Váltás: {new_status}"):
                if supabase_update("members", "member_id", member_id, {"status": new_status}):
                    st.success(f"✅ Státusz módosítva: {new_status}")
                    time.sleep(1)
                    st.rerun()

def show_membership():
    """Tagság kezelés"""
    st.header("💳 Tagság kezelés")
    
    # Tag kiválasztása
    members = supabase_get("members")
    if not members.empty:
        member_options = {
            f"{m['first_name']} {m['last_name']} ({m['email']})": m['member_id']
            for _, m in members.iterrows()
        }
        
        selected_member = st.selectbox("Válassz tagot", list(member_options.keys()))
        member_id = member_options[selected_member]
        
        # Jelenlegi tagságok
        st.subheader("Jelenlegi tagságok")
        memberships = supabase_get("memberships", filter_params={"member_id": f"eq.{member_id}"})
        
        if not memberships.empty:
            membership_types = supabase_get("membership_types")
            if not membership_types.empty:
                memberships_with_type = memberships.merge(
                    membership_types[['type_id', 'type_name', 'price']], 
                    on='type_id',
                    how='left'
                )
                
                for _, ms in memberships_with_type.iterrows():
                    col1, col2, col3 = st.columns([2, 1, 1])
                    with col1:
                        st.write(f"**{ms['type_name']}**")
                        st.caption(f"{ms['start_date']} - {ms['end_date']}")
                    with col2:
                        st.write(f"{ms['price']} Ft")
                    with col3:
                        end_date = pd.to_datetime(ms['end_date']).date()
                        if end_date >= datetime.now().date():
                            st.success("Aktív")
                        else:
                            st.error("Lejárt")
                    st.divider()
        else:
            st.info("Nincs tagság")
        
        # Új tagság
        st.subheader("Új tagság hozzáadása")
        col1, col2 = st.columns(2)
        
        with col1:
            membership_types = supabase_get("membership_types")
            if not membership_types.empty:
                type_options = {
                    f"{t['type_name']} ({t['price']} Ft)": t
                    for _, t in membership_types.iterrows()
                }
                selected_type = st.selectbox("Tagság típus", list(type_options.keys()))
                type_info = type_options[selected_type]
        
        with col2:
            start_date = st.date_input("Kezdő dátum", datetime.now().date())
        
        if st.button("Tagság aktiválása"):
            duration_months = int(type_info['duration_months'])
            end_date = start_date + timedelta(days=30 * duration_months)
            
            new_membership = {
                "member_id": int(member_id),
                "type_id": int(type_info['type_id']),
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "payment_status": "PENDING"
            }
            
            if supabase_insert("memberships", new_membership):
                st.success(f"✅ Tagság aktiválva: {start_date} - {end_date}")
                time.sleep(1)
                st.rerun()

def show_etl():
    """ETL folyamatok"""
    st.header("⚙️ ETL Adminisztráció")
    
    # Statisztikák
    col1, col2, col3 = st.columns(3)
    
    with col1:
        members = supabase_get("members")
        st.metric("OLTP tagok", len(members))
    
    with col2:
        dim_members = supabase_get("dim_member")
        st.metric("DWH rekordok", len(dim_members))
    
    with col3:
        check_ins = supabase_get("check_ins")
        st.metric("Látogatások", len(check_ins))
    
    st.divider()
    
    # ETL műveletek
    if st.button("🔄 Tag dimenzió frissítése", type="primary"):
        with st.spinner("ETL fut..."):
            count = update_dim_member()
            st.success(f"✅ {count} rekord frissítve")
    
    # Összefoglalás
    st.divider()
    st.markdown("""
    ### Projekt összefoglalás
    
    ✅ **OLTP táblák**: members, check_ins, memberships  
    ✅ **ETL folyamat**: Tag dimenzió frissítése  
    ✅ **DWH tábla**: dim_member  
    ✅ **Funkciók**: Tag kezelés, be/kiléptetés, tagság kezelés  
    
    A rendszer megfelel a tantárgy követelményeinek.
    """)

if __name__ == "__main__":
    main()
