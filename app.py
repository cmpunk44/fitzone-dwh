# app.py - FitZone Egyszerű Verzió (csak dim_member ETL)
import streamlit as st
import pandas as pd
import requests
import json
from datetime import datetime, timedelta
import time

st.set_page_config(
    page_title="FitZone Adattárház",
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

# === API FÜGGVÉNYEK ===
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
        st.error(f"API hiba ({table}): {response.text}")
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

def supabase_delete(table, id_field, id_value):
    """Rekord törlése"""
    url = f"{SUPABASE_URL}/rest/v1/{table}?{id_field}=eq.{id_value}"
    response = requests.delete(url, headers=headers)
    return response.ok

# === EGYSZERŰ ETL - CSAK DIM_MEMBER ===
def simple_etl_dim_member():
    """Egyszerű dim_member frissítés"""
    
    # 1. OLTP members lekérése
    members = supabase_get("members")
    if members.empty:
        st.error("❌ Nincs tag az OLTP members táblában!")
        return 0
    
    st.info(f"📊 OLTP-ben {len(members)} tag található")
    
    # 2. Jelenlegi DWH dimenzió
    existing_dim = supabase_get("dim_member")
    existing_member_ids = set(existing_dim['member_id']) if not existing_dim.empty else set()
    
    processed = 0
    
    # 3. Minden tag feldolgozása
    for _, member in members.iterrows():
        member_id = int(member['member_id'])
        
        # Életkor csoport számítása
        age_group = "Unknown"
        member_since_days = 0
        
        if pd.notna(member.get('birth_date')):
            try:
                birth_date = pd.to_datetime(member['birth_date'])
                age = (datetime.now() - birth_date).days // 365
                if age < 25: age_group = "18-25"
                elif age < 35: age_group = "25-35" 
                elif age < 45: age_group = "35-45"
                elif age < 55: age_group = "45-55"
                else: age_group = "55+"
            except:
                age_group = "Unknown"
        
        if pd.notna(member.get('join_date')):
            try:
                join_date = pd.to_datetime(member['join_date'])
                member_since_days = (datetime.now() - join_date).days
            except:
                member_since_days = 0
        
        # Új dim_member rekord
        dim_record = {
            "member_id": member_id,
            "first_name": member['first_name'],
            "last_name": member['last_name'],
            "email": member['email'],
            "age_group": age_group,
            "member_since_days": member_since_days,
            "member_status": member['status'],
            "valid_from": datetime.now().date().isoformat(),
            "valid_to": "2099-12-31",
            "is_current": True
        }
        
        # Egyszerű logika: ha nem létezik, akkor beszúrás
        if member_id not in existing_member_ids:
            if supabase_insert("dim_member", dim_record):
                processed += 1
                st.success(f"✅ Hozzáadva: {member['first_name']} {member['last_name']}")
            else:
                st.error(f"❌ Hiba: {member['first_name']} {member['last_name']}")
        else:
            st.info(f"⚠️ Már létezik: {member['first_name']} {member['last_name']}")
    
    return processed

# === FŐALKALMAZÁS ===
def main():
    st.title("🏋️ FitZone Adattárház - Egyszerű Verzió")
    
    # Oldalsáv navigáció
    st.sidebar.header("📋 Navigáció")
    page = st.sidebar.selectbox(
        "Válassz funkciót:",
        [
            "📊 Dashboard & KPI",
            "👥 Tag Kezelés",
            "🚪 Be/Kiléptetés", 
            "⚙️ ETL - Dim_Member"
        ]
    )
    
    if page == "📊 Dashboard & KPI":
        show_dashboard()
    elif page == "👥 Tag Kezelés":
        show_members()
    elif page == "🚪 Be/Kiléptetés":
        show_checkin()
    elif page == "⚙️ ETL - Dim_Member":
        show_etl()

def show_dashboard():
    """KPI Dashboard"""
    st.header("📊 FitZone Dashboard")
    
    # OLTP Adatok
    members = supabase_get("members")
    check_ins = supabase_get("check_ins")
    
    # DWH Adatok
    dim_member = supabase_get("dim_member")
    
    # KPI Metrikák
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_members = len(members) if not members.empty else 0
        active_members = len(members[members['status'] == 'ACTIVE']) if not members.empty else 0
        st.metric("📊 OLTP Tagok", total_members)
        st.metric("🟢 Aktív tagok", active_members)
    
    with col2:
        dim_records = len(dim_member) if not dim_member.empty else 0
        current_records = len(dim_member[dim_member['is_current'] == True]) if not dim_member.empty else 0
        st.metric("🏢 DWH Rekordok", dim_records)
        st.metric("🔄 Aktuális rekordok", current_records)
    
    with col3:
        if not check_ins.empty:
            today = datetime.now().date()
            check_ins['check_in_date'] = pd.to_datetime(check_ins['check_in_time']).dt.date
            today_visits = len(check_ins[check_ins['check_in_date'] == today])
            currently_inside = len(check_ins[
                (check_ins['check_in_date'] == today) & 
                pd.isna(check_ins['check_out_time'])
            ])
        else:
            today_visits = 0
            currently_inside = 0
        
        st.metric("🚪 Mai látogatások", today_visits)
        st.metric("🏠 Most bent", currently_inside)
    
    with col4:
        if not dim_member.empty:
            age_groups = dim_member[dim_member['is_current'] == True]['age_group'].value_counts()
            most_common_age = age_groups.index[0] if len(age_groups) > 0 else "N/A"
            avg_member_days = dim_member[dim_member['is_current'] == True]['member_since_days'].mean()
        else:
            most_common_age = "N/A"
            avg_member_days = 0
        
        st.metric("👥 Legnépszerűbb korosztály", most_common_age)
        st.metric("📅 Átlag tagság (nap)", f"{avg_member_days:.0f}" if avg_member_days > 0 else "N/A")
    
    # Részletes KPI táblák
    st.divider()
    
    tab1, tab2, tab3 = st.tabs(["👥 Tag Összesítő", "📊 Korosztály Elemzés", "🚪 Látogatások"])
    
    with tab1:
        st.subheader("Tag Összesítő")
        
        if not members.empty and not dim_member.empty:
            # OLTP vs DWH összehasonlítás
            comparison_data = []
            
            for status in ['ACTIVE', 'INACTIVE']:
                oltp_count = len(members[members['status'] == status])
                dwh_count = len(dim_member[
                    (dim_member['member_status'] == status) & 
                    (dim_member['is_current'] == True)
                ])
                
                comparison_data.append({
                    'Státusz': status,
                    'OLTP Tagok': oltp_count,
                    'DWH Rekordok': dwh_count,
                    'Szinkronban': '✅' if oltp_count == dwh_count else '❌'
                })
            
            comparison_df = pd.DataFrame(comparison_data)
            st.dataframe(comparison_df, use_container_width=True)
            
            if not all(comparison_df['Szinkronban'] == '✅'):
                st.warning("⚠️ OLTP és DWH nem szinkronban! Futtasd az ETL-t!")
        else:
            st.info("Nincs elegendő adat az összehasonlításhoz.")
    
    with tab2:
        st.subheader("Korosztály Elemzés")
        
        if not dim_member.empty:
            current_members = dim_member[dim_member['is_current'] == True]
            
            if not current_members.empty:
                # Korosztály megoszlás
                age_distribution = current_members['age_group'].value_counts().reset_index()
                age_distribution.columns = ['Korosztály', 'Tagok száma']
                
                # Státusz szerinti bontás
                status_age = current_members.groupby(['age_group', 'member_status']).size().unstack(fill_value=0)
                
                st.markdown("**📊 Korosztály megoszlás:**")
                st.dataframe(age_distribution, use_container_width=True)
                
                if not status_age.empty:
                    st.markdown("**📈 Státusz korosztály szerint:**")
                    st.dataframe(status_age, use_container_width=True)
            else:
                st.info("Nincs aktuális tag rekord.")
        else:
            st.info("Nincs DWH adat. Futtasd az ETL-t!")
    
    with tab3:
        st.subheader("Mai Látogatások")
        
        if not check_ins.empty:
            check_ins['check_in_time'] = pd.to_datetime(check_ins['check_in_time'])
            today = datetime.now().date()
            today_visits = check_ins[check_ins['check_in_time'].dt.date == today]
            
            if not today_visits.empty and not members.empty:
                visits_with_names = today_visits.merge(
                    members[['member_id', 'first_name', 'last_name']], 
                    on='member_id',
                    how='left'
                )
                
                display_visits = visits_with_names[[
                    'first_name', 'last_name', 'check_in_time', 'check_out_time'
                ]].copy()
                display_visits.columns = ['Keresztnév', 'Vezetéknév', 'Belépés', 'Kilépés']
                
                st.dataframe(display_visits, use_container_width=True)
            else:
                st.info("Ma még nem volt látogatás.")
        else:
            st.info("Nincs látogatási adat.")

def show_members():
    """Tag kezelés"""
    st.header("👥 Tag Kezelés")
    
    tab1, tab2, tab3 = st.tabs(["👀 Tag Lista", "➕ Új Tag", "✏️ Tag Módosítás"])
    
    with tab1:
        st.subheader("Tagok listája")
        
        members = supabase_get("members")
        if not members.empty:
            st.dataframe(members, use_container_width=True)
        else:
            st.info("Nincsenek tagok.")
    
    with tab2:
        st.subheader("Új tag regisztrálása")
        
        with st.form("new_member_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                first_name = st.text_input("Keresztnév *")
                last_name = st.text_input("Vezetéknév *") 
                email = st.text_input("Email cím *")
            
            with col2:
                phone = st.text_input("Telefonszám")
                birth_date = st.date_input("Születési dátum", value=None)
                status = st.selectbox("Státusz", ["ACTIVE", "INACTIVE"])
            
            submitted = st.form_submit_button("💾 Tag regisztrálása")
            
            if submitted:
                if first_name and last_name and email:
                    new_member = {
                        "first_name": first_name,
                        "last_name": last_name,
                        "email": email,
                        "phone": phone,
                        "birth_date": birth_date.isoformat() if birth_date else None,
                        "join_date": datetime.now().date().isoformat(),
                        "status": status
                    }
                    
                    if supabase_insert("members", new_member):
                        st.success("✅ Tag sikeresen regisztrálva!")
                        st.info("💡 Most futtasd az ETL-t a DWH frissítéséhez!")
                        time.sleep(2)
                        st.rerun()
                    else:
                        st.error("❌ Hiba a regisztráció során!")
                else:
                    st.error("❌ Kötelező mezők kitöltése szükséges!")
    
    with tab3:
        st.subheader("Tag státusz módosítása")
        
        members = supabase_get("members")
        if not members.empty:
            member_options = {
                f"{row['first_name']} {row['last_name']} ({row['email']})": row['member_id']
                for _, row in members.iterrows()
            }
            
            selected_member_name = st.selectbox("Válassz tagot", list(member_options.keys()))
            selected_member_id = member_options[selected_member_name]
            
            selected_member = members[members['member_id'] == selected_member_id].iloc[0]
            current_status = selected_member['status']
            new_status = "INACTIVE" if current_status == "ACTIVE" else "ACTIVE"
            
            st.info(f"Jelenlegi státusz: **{current_status}**")
            
            if st.button(f"Váltás: {new_status}"):
                if supabase_update("members", "member_id", selected_member_id, {"status": new_status}):
                    st.success(f"✅ Státusz módosítva: {new_status}")
                    st.info("💡 Most futtasd az ETL-t a DWH frissítéséhez!")
                    time.sleep(2)
                    st.rerun()

def show_checkin():
    """Be/kiléptetés"""
    st.header("🚪 Be/Kiléptetés")
    
    tab1, tab2 = st.tabs(["🔓 Beléptetés", "🔒 Kiléptetés"])
    
    with tab1:
        st.subheader("Tag beléptetése")
        
        members = supabase_get("members", filter_params={"status": "eq.ACTIVE"})
        if not members.empty:
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
            
            for _, member in filtered.iterrows():
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.write(f"**{member['first_name']} {member['last_name']}**")
                    st.caption(f"📧 {member['email']}")
                
                with col2:
                    if st.button("🔓 Beléptet", key=f"in_{member['member_id']}"):
                        check_in_data = {
                            "member_id": int(member['member_id']),
                            "check_in_time": datetime.now().isoformat()
                        }
                        if supabase_insert("check_ins", check_in_data):
                            st.success(f"✅ {member['first_name']} beléptetve!")
                            time.sleep(1)
                            st.rerun()
                
                st.divider()
    
    with tab2:
        st.subheader("Tag kiléptetése")
        
        check_ins = supabase_get("check_ins")
        if not check_ins.empty:
            active = check_ins[pd.isna(check_ins['check_out_time'])]
            
            if not active.empty:
                members = supabase_get("members")
                active_with_names = active.merge(
                    members[['member_id', 'first_name', 'last_name']], 
                    on='member_id',
                    how='left'
                )
                
                for _, checkin in active_with_names.iterrows():
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        check_in_time = pd.to_datetime(checkin['check_in_time'])
                        duration = datetime.now() - check_in_time
                        hours = int(duration.total_seconds() // 3600)
                        minutes = int((duration.total_seconds() % 3600) // 60)
                        
                        st.write(f"**{checkin['first_name']} {checkin['last_name']}**")
                        st.caption(f"⏰ Bent: {hours}ó {minutes}p")
                    
                    with col2:
                        if st.button("🔒 Kiléptet", key=f"out_{checkin['checkin_id']}"):
                            update_data = {"check_out_time": datetime.now().isoformat()}
                            if supabase_update("check_ins", "checkin_id", checkin['checkin_id'], update_data):
                                st.success(f"✅ {checkin['first_name']} kiléptetve!")
                                time.sleep(1)
                                st.rerun()
                    
                    st.divider()
            else:
                st.info("🏠 Nincs bent senki.")

def show_etl():
    """ETL dim_member kezelése"""
    st.header("⚙️ ETL - Dim_Member Frissítése")
    
    st.markdown("""
    ### 📋 Mit csinál ez az ETL?
    
    1. **Extract**: Kinyeri az összes tagot a `members` táblából (OLTP)
    2. **Transform**: Kiszámolja az életkor csoportot és tagság napjait  
    3. **Load**: Betölti a `dim_member` táblába (DWH)
    
    **Egyszerű logika**: Ha egy tag még nincs a dim_member táblában, hozzáadja.
    """)
    
    # Státusz
    st.divider()
    st.subheader("📊 Jelenlegi Státusz")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        members = supabase_get("members")
        st.metric("🔧 OLTP Members", len(members) if not members.empty else 0)
        
        if not members.empty:
            active_count = len(members[members['status'] == 'ACTIVE'])
            st.write(f"🟢 Aktív: {active_count}")
            st.write(f"🔴 Inaktív: {len(members) - active_count}")
    
    with col2:
        dim_member = supabase_get("dim_member")
        st.metric("🏢 DWH Dim_Member", len(dim_member) if not dim_member.empty else 0)
        
        if not dim_member.empty:
            current_count = len(dim_member[dim_member['is_current'] == True])
            st.write(f"🔄 Aktuális rekordok: {current_count}")
    
    with col3:
        if not members.empty and not dim_member.empty:
            oltp_ids = set(members['member_id'])
            dwh_ids = set(dim_member['member_id'])
            missing_count = len(oltp_ids - dwh_ids)
            
            st.metric("🔄 Szinkronizálandó", missing_count)
            
            if missing_count == 0:
                st.success("✅ Szinkronban")
            else:
                st.warning(f"⚠️ {missing_count} tag hiányzik")
    
    # ETL futtatás
    st.divider()
    st.subheader("🚀 ETL Futtatása")
    
    if st.button("🔄 Dim_Member ETL Futtatása", type="primary", use_container_width=True):
        with st.spinner("ETL futtatása..."):
            count = simple_etl_dim_member()
            
            if count > 0:
                st.success(f"✅ ETL befejezve! {count} új rekord hozzáadva.")
                st.balloons()
            else:
                st.info("ℹ️ ETL befejezve! Minden tag már szinkronban van.")
    
    # Debug információ
    st.divider()
    st.subheader("🔍 Debug Információ")
    
    with st.expander("📋 Részletes státusz"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**OLTP Members minta:**")
            if not members.empty:
                st.dataframe(members[['member_id', 'first_name', 'last_name', 'status']].head(5))
            else:
                st.write("Nincs adat")
        
        with col2:
            st.markdown("**DWH Dim_Member minta:**")
            if not dim_member.empty:
                st.dataframe(dim_member[['member_id', 'first_name', 'member_status', 'is_current']].head(5))
            else:
                st.write("Nincs adat")

if __name__ == "__main__":
    main()
