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

# === API FÜGGVÉNYEK ===
def supabase_get(table, select="*", filter_params=None, show_error=True):
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
        if show_error:
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

# === TELJES ETL FOLYAMAT ===
def run_full_etl():
    """Teljes ETL folyamat futtatása"""
    results = {
        "dim_member": 0,
        "dim_date": 0,
        "fact_visits": 0,
        "fact_revenue": 0
    }
    
    # 1. Tag dimenzió frissítése (SCD2)
    results["dim_member"] = etl_dim_member()
    
    # 2. Dátum dimenzió feltöltése
    results["dim_date"] = etl_dim_date()
    
    # 3. Látogatás fact tábla feltöltése
    results["fact_visits"] = etl_fact_visits()
    
    # 4. Bevétel fact tábla feltöltése
    results["fact_revenue"] = etl_fact_revenue()
    
    return results

def etl_dim_member():
    """Tag dimenzió ETL - EGYSZERŰSÍTETT DEBUG VERZIÓ"""
    print("🔍 ETL dim_member DEBUG indítása...")
    
    # 1. OLTP members lekérése
    members = supabase_get("members")
    print(f"📊 Members tábla: {len(members)} rekord")
    
    if members.empty:
        print("❌ Members tábla üres!")
        return 0
    
    print(f"📋 Members mezők: {members.columns.tolist()}")
    print(f"📝 Utolsó tag: {members.iloc[-1].to_dict() if len(members) > 0 else 'Nincs'}")
    
    # 2. Jelenlegi DWH dimenzió
    existing_dim = supabase_get("dim_member")
    print(f"📦 Dim_member tábla: {len(existing_dim)} rekord")
    
    processed = 0
    errors = 0
    
    # 3. Minden tag feldolgozása
    for index, member in members.iterrows():
        try:
            member_id = member['member_id']
            print(f"🔄 Feldolgozás: {member['first_name']} {member['last_name']} (ID: {member_id})")
            
            # Életkor csoport számítása
            age_group = "Unknown"
            if pd.notna(member.get('birth_date')):
                birth_date = pd.to_datetime(member['birth_date'])
                age = (datetime.now() - birth_date).days // 365
                if age < 25: age_group = "18-25"
                elif age < 35: age_group = "25-35"
                elif age < 45: age_group = "35-45"
                elif age < 55: age_group = "45-55"
                else: age_group = "55+"
                print(f"   👤 Életkor csoport: {age_group}")
            
            # Tag az óta napok
            member_since_days = 0
            if pd.notna(member.get('join_date')):
                join_date = pd.to_datetime(member['join_date'])
                member_since_days = (datetime.now() - join_date).days
                print(f"   📅 Tag {member_since_days} napja")
            
            # EGYSZERŰSÍTETT LOGIKA: Mindig új rekord (SCD2 nélkül)
            new_record = {
                "member_id": int(member_id),
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
            
            print(f"   💾 Beszúrandó rekord: {new_record}")
            
            # Ellenőrzés: már létezik ez a member_id?
            if not existing_dim.empty:
                existing_member = existing_dim[existing_dim['member_id'] == member_id]
                if not existing_member.empty:
                    print(f"   ⚠️ Member_id {member_id} már létezik dim_member-ben")
                    continue
            
            # Beszúrás
            if supabase_insert("dim_member", new_record):
                processed += 1
                print(f"   ✅ Sikeresen beszúrva!")
            else:
                errors += 1
                print(f"   ❌ Beszúrás sikertelen!")
                
        except Exception as e:
            errors += 1
            print(f"   💥 Hiba: {str(e)}")
    
    print(f"📊 ETL dim_member befejezve: {processed} siker, {errors} hiba")
    return processed
def etl_dim_date():
    """Dátum dimenzió feltöltése"""
    existing_dates = supabase_get("dim_date")
    existing_keys = set(str(d) for d in existing_dates['date_key']) if not existing_dates.empty else set()
    
    # Utolsó 1 év + következő 6 hónap
    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now() + timedelta(days=180)
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    new_records = []
    for date in dates:
        date_key = int(date.strftime('%Y%m%d'))
        
        if str(date_key) not in existing_keys:
            record = {
                "date_key": date_key,
                "date": date.date().isoformat(),
                "year": date.year,
                "month": date.month,
                "month_name": date.strftime('%B'),
                "day_of_week": date.weekday() + 1,
                "is_weekend": date.weekday() >= 5
            }
            new_records.append(record)
    
    # Batch insert
    if new_records:
        # Egyesével beszúrás, mert batch nem mindig működik
        success = 0
        for record in new_records:
            if supabase_insert("dim_date", record):
                success += 1
        return success
    
    return 0

def etl_fact_visits():
    """Látogatás fact tábla feltöltése"""
    check_ins = supabase_get("check_ins")
    if check_ins.empty:
        return 0
    
    existing_visits = supabase_get("fact_visits")
    existing_keys = set(existing_visits['visit_key']) if not existing_visits.empty else set()
    
    processed = 0
    for _, visit in check_ins.iterrows():
        if pd.notna(visit.get('check_in_time')):
            check_in_time = pd.to_datetime(visit['check_in_time'])
            date_key = int(check_in_time.strftime('%Y%m%d'))
            visit_key = f"{visit['checkin_id']}_{date_key}"
            
            if visit_key not in existing_keys:
                # Időtartam számítása
                duration_minutes = 0
                if pd.notna(visit.get('check_out_time')):
                    check_out_time = pd.to_datetime(visit['check_out_time'])
                    duration_minutes = int((check_out_time - check_in_time).total_seconds() / 60)
                
                record = {
                    "visit_key": visit_key,
                    "date_key": date_key,
                    "time_key": check_in_time.hour * 100,
                    "member_key": int(visit['member_id']),
                    "check_in_time": check_in_time.isoformat(),
                    "check_out_time": pd.to_datetime(visit['check_out_time']).isoformat() if pd.notna(visit.get('check_out_time')) else None,
                    "duration_minutes": duration_minutes
                }
                
                if supabase_insert("fact_visits", record):
                    processed += 1
    
    return processed

def etl_fact_revenue():
    """Bevétel fact tábla feltöltése"""
    payments = supabase_get("payments")
    if payments.empty:
        return 0
    
    existing_revenue = supabase_get("fact_revenue")
    existing_keys = set(existing_revenue['revenue_key']) if not existing_revenue.empty else set()
    
    processed = 0
    for _, payment in payments.iterrows():
        if pd.notna(payment.get('payment_date')):
            payment_date = pd.to_datetime(payment['payment_date'])
            date_key = int(payment_date.strftime('%Y%m%d'))
            revenue_key = f"{payment['payment_id']}_{date_key}"
            
            if revenue_key not in existing_keys:
                record = {
                    "revenue_key": revenue_key,
                    "date_key": date_key,
                    "member_key": int(payment['member_id']),
                    "payment_amount": float(payment['amount']),
                    "payment_type": payment['payment_type'],
                    "payment_date": payment_date.isoformat()
                }
                
                if supabase_insert("fact_revenue", record):
                    processed += 1
    
    return processed

# === SEGÉDFÜGGVÉNYEK ===
def check_dwh_tables():
    """Ellenőrzi hogy a DWH táblák léteznek-e"""
    dwh_tables = ['dim_member', 'dim_date', 'fact_visits', 'fact_revenue']
    missing_tables = []
    
    for table in dwh_tables:
        df = supabase_get(table, show_error=False)
        if df is None or (hasattr(df, 'empty') and len(df.columns) == 0):
            missing_tables.append(table)
    
    return missing_tables

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

# === FŐALKALMAZÁS ===
def main():
    st.title("🏋️ FitZone Adattárház és BI Rendszer")
    
    # Oldalsáv navigáció
    st.sidebar.header("📋 Navigáció")
    page = st.sidebar.selectbox(
        "Válassz funkciót:",
        [
            "📊 Dashboard & KPI",
            "👥 Tag Kezelés (OLTP)",
            "🚪 Be/Kiléptetés (OLTP)", 
            "💳 Tagság Kezelés",
            "⚙️ ETL Folyamatok",
            "📈 DWH Elemzések"
        ]
    )
    
    if page == "📊 Dashboard & KPI":
        show_dashboard()
    elif page == "👥 Tag Kezelés (OLTP)":
        show_members()
    elif page == "🚪 Be/Kiléptetés (OLTP)":
        show_reception()
    elif page == "💳 Tagság Kezelés":
        show_membership()
    elif page == "⚙️ ETL Folyamatok":
        show_etl()
    elif page == "📈 DWH Elemzések":
        show_dwh_analysis()

def show_dashboard():
    """Főoldal - KPI Dashboard"""
    st.header("📊 Főoldal - KPI Mutatók")
    
    # DWH táblák ellenőrzése
    missing_tables = check_dwh_tables()
    if missing_tables:
        st.error(f"❌ Hiányzó DWH táblák: {', '.join(missing_tables)}")
        st.info("🔧 Hozd létre a DWH táblákat a Supabase SQL Editor-ban az alábbi script futtatásával:")
        
        with st.expander("📋 SQL Script a DWH táblák létrehozásához"):
            st.code("""
-- Futtasd le ezt a Supabase SQL Editor-ban:

CREATE TABLE IF NOT EXISTS public.dim_member (
    member_key SERIAL PRIMARY KEY,
    member_id INTEGER NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    age_group VARCHAR(20),
    member_since_days INTEGER DEFAULT 0,
    member_status VARCHAR(20),
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.dim_date (
    date_key INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20),
    day_of_week INTEGER,
    is_weekend BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.fact_visits (
    visit_key VARCHAR PRIMARY KEY,
    date_key INTEGER,
    time_key INTEGER,
    member_key INTEGER,
    check_in_time TIMESTAMPTZ,
    check_out_time TIMESTAMPTZ,
    duration_minutes INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.fact_revenue (
    revenue_key VARCHAR PRIMARY KEY,
    date_key INTEGER,
    member_key INTEGER,
    payment_amount DECIMAL(10,2),
    payment_type VARCHAR(50),
    payment_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
            """, language="sql")
        
        st.warning("⚠️ A DWH funkciók csak a táblák létrehozása után lesznek elérhetők!")
    
    stats = calculate_stats()
    
    # OLTP KPI-k
    st.subheader("🔧 OLTP Rendszer Státusz")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Összes tag", stats['total_members'])
    
    with col2:
        st.metric("Aktív tagok", stats['active_members'])
    
    with col3:
        st.metric("Mai látogatók", stats['unique_visitors'])
    
    with col4:
        st.metric("Most bent", stats['currently_inside'])
    
    # DWH KPI-k csak ha minden tábla létezik
    if not missing_tables:
        st.divider()
        st.subheader("🏢 Adattárház (DWH) Státusz")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            dim_member = supabase_get("dim_member")
            dim_date = supabase_get("dim_date")
            st.metric("Dim_member rekordok", len(dim_member))
            st.metric("Dim_date rekordok", len(dim_date))
        
        with col2:
            fact_visits = supabase_get("fact_visits")
            fact_revenue = supabase_get("fact_revenue")
            st.metric("Fact_visits rekordok", len(fact_visits))
            st.metric("Fact_revenue rekordok", len(fact_revenue))
        
        with col3:
            if not fact_visits.empty:
                avg_duration = fact_visits[fact_visits['duration_minutes'] > 0]['duration_minutes'].mean()
                total_duration = fact_visits['duration_minutes'].sum()
            else:
                avg_duration = 0
                total_duration = 0
            
            st.metric("Átlag edzésidő", f"{avg_duration:.0f} perc" if avg_duration > 0 else "N/A")
            st.metric("Összes edzésidő", f"{total_duration:,.0f} perc")
        
        with col4:
            if not fact_revenue.empty:
                total_revenue = fact_revenue['payment_amount'].sum()
                avg_payment = fact_revenue['payment_amount'].mean()
            else:
                total_revenue = 0
                avg_payment = 0
            
            st.metric("DWH összes bevétel", f"{total_revenue:,.0f} Ft")
            st.metric("Átlag fizetés", f"{avg_payment:.0f} Ft" if avg_payment > 0 else "N/A")
    
    # Aktív tagok táblázat
    st.divider()
    st.subheader("👥 Aktív Tagok")
    
    tab1, tab2 = st.tabs(["Aktív tagok", "Mai látogatások"])
    
    with tab1:
        members = supabase_get("members", filter_params={"status": "eq.ACTIVE"})
        if not members.empty:
            st.dataframe(members[['member_id', 'first_name', 'last_name', 'email', 'status']], use_container_width=True)
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
                    st.dataframe(visits_with_names[['first_name', 'last_name', 'check_in_time', 'check_out_time']], use_container_width=True)
            else:
                st.info("Ma még nem volt látogatás")

def show_reception():
    """Be/kiléptetés"""
    st.header("🚪 Recepció - Be/Kiléptetés")
    
    tab1, tab2, tab3 = st.tabs(["🔓 Beléptetés", "🔒 Kiléptetés", "👁️ Jelenlegi Státusz"])
    
    with tab1:
        st.subheader("Tag beléptetése")
        
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
                    st.caption(f"📧 {member['email']} | 📱 {member.get('phone', 'N/A')}")
                
                with col2:
                    if st.button("🔓 Beléptet", key=f"in_{member['member_id']}"):
                        # Ellenőrzés: már bent van-e
                        existing = supabase_get("check_ins", filter_params={"member_id": f"eq.{member['member_id']}"})
                        
                        active_checkin = False
                        if not existing.empty:
                            active_checkin = not existing[pd.isna(existing['check_out_time'])].empty
                        
                        if active_checkin:
                            st.error(f"❌ {member['first_name']} már bent van!")
                        else:
                            check_in_data = {
                                "member_id": int(member['member_id']),
                                "check_in_time": datetime.now().isoformat()
                            }
                            if supabase_insert("check_ins", check_in_data):
                                st.success(f"✅ {member['first_name']} sikeresen beléptetve!")
                                time.sleep(1)
                                st.rerun()
                
                st.divider()
        else:
            st.info("Nincsenek aktív tagok.")
    
    with tab2:
        st.subheader("Tag kiléptetése")
        
        check_ins = supabase_get("check_ins")
        if not check_ins.empty:
            active = check_ins[pd.isna(check_ins['check_out_time'])]
            
            if not active.empty:
                members = supabase_get("members")
                active_with_names = active.merge(
                    members[['member_id', 'first_name', 'last_name', 'email']], 
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
                        st.caption(f"📧 {checkin['email']} | ⏰ Bent: {hours}ó {minutes}p")
                    
                    with col2:
                        if st.button("🔒 Kiléptet", key=f"out_{checkin['checkin_id']}"):
                            update_data = {"check_out_time": datetime.now().isoformat()}
                            if supabase_update("check_ins", "checkin_id", checkin['checkin_id'], update_data):
                                st.success(f"✅ {checkin['first_name']} sikeresen kiléptetve!")
                                time.sleep(1)
                                st.rerun()
                    
                    st.divider()
            else:
                st.info("🏠 Jelenleg nincs bent senki.")
        else:
            st.info("📝 Még nem volt látogatás.")
    
    with tab3:
        st.subheader("Jelenlegi bent lévők")
        
        check_ins = supabase_get("check_ins")
        if not check_ins.empty:
            active = check_ins[pd.isna(check_ins['check_out_time'])]
            st.metric("🏠 Bent lévők száma", len(active))
            
            if not active.empty:
                members = supabase_get("members")
                active_details = active.merge(
                    members[['member_id', 'first_name', 'last_name', 'email']], 
                    on='member_id',
                    how='left'
                )
                
                active_details['check_in_time'] = pd.to_datetime(active_details['check_in_time'])
                active_details['duration'] = (pd.Timestamp.now() - active_details['check_in_time']).dt.total_seconds() / 60
                active_details['duration_str'] = active_details['duration'].apply(
                    lambda x: f"{int(x//60)}ó {int(x%60)}p"
                )
                
                display_df = active_details[['first_name', 'last_name', 'email', 'check_in_time', 'duration_str']].copy()
                display_df.columns = ['Keresztnév', 'Vezetéknév', 'Email', 'Belépés ideje', 'Bent töltött idő']
                
                st.dataframe(display_df, use_container_width=True)

def show_members():
    """Tag kezelés (OLTP műveletek)"""
    st.header("👥 Tag Kezelés - OLTP Műveletek")
    
    tab1, tab2, tab3, tab4 = st.tabs(["👀 Tag Lista", "➕ Új Tag", "✏️ Tag Módosítás", "🗑️ Tag Törlés"])
    
    with tab1:
        st.subheader("Tagok listája")
        
        col1, col2 = st.columns(2)
        with col1:
            status_filter = st.selectbox("Státusz szűrő", ["Mind", "ACTIVE", "INACTIVE"])
        with col2:
            search_term = st.text_input("Keresés (név/email)")
        
        if status_filter == "Mind":
            members = supabase_get("members")
        else:
            members = supabase_get("members", filter_params={"status": f"eq.{status_filter}"})
        
        if not members.empty:
            # Szűrés keresési kifejezésre
            if search_term:
                mask = (
                    members['first_name'].str.contains(search_term, case=False, na=False) |
                    members['last_name'].str.contains(search_term, case=False, na=False) |
                    members['email'].str.contains(search_term, case=False, na=False)
                )
                members = members[mask]
            
            st.dataframe(members, use_container_width=True)
        else:
            st.info("Nincsenek tagok az adatbázisban.")
    
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
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("❌ Hiba a regisztráció során!")
                else:
                    st.error("❌ Kötelező mezők kitöltése szükséges!")
    
    with tab3:
        st.subheader("Tag adatok módosítása")
        
        members = supabase_get("members")
        if not members.empty:
            # Tag kiválasztása
            member_options = {
                f"{row['first_name']} {row['last_name']} ({row['email']})": row['member_id']
                for _, row in members.iterrows()
            }
            
            selected_member_name = st.selectbox("Válassz tagot", list(member_options.keys()))
            selected_member_id = member_options[selected_member_name]
            
            # Jelenlegi adatok betöltése
            selected_member = members[members['member_id'] == selected_member_id].iloc[0]
            
            with st.form("edit_member_form"):
                col1, col2 = st.columns(2)
                
                with col1:
                    first_name = st.text_input("Keresztnév", value=selected_member['first_name'])
                    last_name = st.text_input("Vezetéknév", value=selected_member['last_name'])
                    email = st.text_input("Email", value=selected_member['email'])
                
                with col2:
                    phone = st.text_input("Telefon", value=selected_member.get('phone', ''))
                    birth_date = st.date_input(
                        "Születési dátum", 
                        value=pd.to_datetime(selected_member['birth_date']).date() if pd.notna(selected_member.get('birth_date')) else None
                    )
                    status = st.selectbox("Státusz", ["ACTIVE", "INACTIVE"], 
                                        index=0 if selected_member['status'] == 'ACTIVE' else 1)
                
                submitted = st.form_submit_button("💾 Módosítások mentése")
                
                if submitted:
                    updated_data = {
                        "first_name": first_name,
                        "last_name": last_name,
                        "email": email,
                        "phone": phone,
                        "birth_date": birth_date.isoformat() if birth_date else None,
                        "status": status
                    }
                    
                    if supabase_update("members", "member_id", selected_member_id, updated_data):
                        st.success("✅ Tag adatai frissítve!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("❌ Hiba a frissítés során!")
    
    with tab4:
        st.subheader("Tag törlése")
        st.warning("⚠️ FIGYELEM: A tag törlése végleges és visszafordíthatatlan!")
        
        members = supabase_get("members")
        if not members.empty:
            # Tag kiválasztása
            member_options = {
                f"{row['first_name']} {row['last_name']} ({row['email']})": row['member_id']
                for _, row in members.iterrows()
            }
            
            selected_member_name = st.selectbox("Válassz törlendő tagot", list(member_options.keys()))
            selected_member_id = member_options[selected_member_name]
            
            if st.button("🗑️ Tag végleges törlése", type="primary"):
                if supabase_delete("members", "member_id", selected_member_id):
                    st.success("✅ Tag sikeresen törölve!")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("❌ Hiba a törlés során!")

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
    """ETL folyamatok kezelése"""
    st.header("⚙️ ETL Folyamatok Kezelése")
    
    # DWH táblák ellenőrzése
    missing_tables = check_dwh_tables()
    if missing_tables:
        st.error(f"❌ Hiányzó DWH táblák: {', '.join(missing_tables)}")
        st.warning("⚠️ Az ETL folyamatok csak a DWH táblák létrehozása után futtathatók!")
        
        with st.expander("📋 DWH táblák létrehozása"):
            st.markdown("""
            ### Lépések:
            1. Menj a Supabase Dashboard-ra
            2. Nyisd meg a **SQL Editor**-t
            3. Futtasd le az alábbi SQL script-et:
            """)
            
            st.code("""
CREATE TABLE IF NOT EXISTS public.dim_member (
    member_key SERIAL PRIMARY KEY,
    member_id INTEGER NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    age_group VARCHAR(20),
    member_since_days INTEGER DEFAULT 0,
    member_status VARCHAR(20),
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS public.dim_date (
    date_key INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20),
    day_of_week INTEGER,
    is_weekend BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS public.fact_visits (
    visit_key VARCHAR PRIMARY KEY,
    date_key INTEGER,
    time_key INTEGER,
    member_key INTEGER,
    check_in_time TIMESTAMPTZ,
    check_out_time TIMESTAMPTZ,
    duration_minutes INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS public.fact_revenue (
    revenue_key VARCHAR PRIMARY KEY,
    date_key INTEGER,
    member_key INTEGER,
    payment_amount DECIMAL(10,2),
    payment_type VARCHAR(50),
    payment_date TIMESTAMPTZ
);
            """, language="sql")
        
        return
    
    st.markdown("""
    ### 📋 ETL Folyamat Leírása
    
    **Extract-Transform-Load** folyamat az OLTP rendszerből az adattárházba:
    
    1. **Extract**: Adatok kinyerése az OLTP táblákból (members, check_ins, payments)
    2. **Transform**: Adatok átalakítása DWH formátumra (dimenziók, fact táblák)
    3. **Load**: Adatok betöltése az adattárház táblákba (SCD2, inkrementális)
    """)
    
    # ETL státusz
    st.divider()
    st.subheader("📊 ETL Státusz")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        members_oltp = len(supabase_get("members"))
        members_dwh = len(supabase_get("dim_member"))
        st.metric("OLTP Tagok", members_oltp)
        st.metric("DWH Dim_member", members_dwh)
    
    with col2:
        checkins_oltp = len(supabase_get("check_ins"))
        visits_dwh = len(supabase_get("fact_visits"))
        st.metric("OLTP Check-ins", checkins_oltp)
        st.metric("DWH Fact_visits", visits_dwh)
    
    with col3:
        payments_oltp = len(supabase_get("payments"))
        revenue_dwh = len(supabase_get("fact_revenue"))
        st.metric("OLTP Payments", payments_oltp)
        st.metric("DWH Fact_revenue", revenue_dwh)
    
    with col4:
        dates_dwh = len(supabase_get("dim_date"))
        st.metric("DWH Dim_date", dates_dwh)
        st.metric("Utolsó ETL", "Manuális")
    
    # ETL futtatás
    st.divider()
    st.subheader("🚀 ETL Futtatása")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 Teljes ETL Futtatása", type="primary", use_container_width=True):
            with st.spinner("ETL folyamat futtatása..."):
                results = run_full_etl()
                
                st.success("✅ ETL folyamat befejezve!")
                
                # Eredmények megjelenítése
                st.markdown("### 📈 ETL Eredmények:")
                for table, count in results.items():
                    if count > 0:
                        st.write(f"- **{table}**: {count} rekord feldolgozva")
                    else:
                        st.write(f"- **{table}**: Nincs új rekord")
    
    with col2:
        st.markdown("""
        ### ℹ️ ETL Részletek
        
        **SCD Type 2**: Tag dimenzió történet követése
        **Inkrementális**: Csak új rekordok betöltése
        **Fact táblák**: Látogatások és bevételek elemzése
        
        ⚠️ **Fontos**: Az ETL minden futtatáskor ellenőrzi az OLTP változásokat.
        """)
    
    # Egyszerű ETL is elérhető
    st.divider()
    st.subheader("⚡ Gyors ETL Műveletek")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("👥 Csak Dim_member"):
            with st.spinner("Tag dimenzió frissítése..."):
                count = etl_dim_member()
                st.success(f"✅ {count} tag rekord frissítve")
    
    with col2:
        if st.button("📅 Csak Dim_date"):
            with st.spinner("Dátum dimenzió frissítése..."):
                count = etl_dim_date()
                st.success(f"✅ {count} dátum rekord hozzáadva")
    
    with col3:
        if st.button("🚪 Csak Fact_visits"):
            with st.spinner("Látogatási adatok frissítése..."):
                count = etl_fact_visits()
                st.success(f"✅ {count} látogatás rekord hozzáadva")

def show_dwh_analysis():
    """DWH elemzések és jelentések"""
    st.header("📈 Adattárház Elemzések")
    
    tab1, tab2, tab3 = st.tabs(["📊 Alapvető Lekérdezések", "📈 Idősorok", "📋 Jelentések"])
    
    with tab1:
        st.subheader("Alapvető DWH Lekérdezések")
        
        # Legaktívabb tagok
        fact_visits = supabase_get("fact_visits")
        dim_member = supabase_get("dim_member")
        
        if not fact_visits.empty and not dim_member.empty:
            # Tag aktivitás számítása
            member_stats = fact_visits.groupby('member_key').agg({
                'visit_key': 'count',
                'duration_minutes': ['mean', 'sum']
            }).reset_index()
            
            member_stats.columns = ['member_key', 'visit_count', 'avg_duration', 'total_duration']
            
            # Dimenzióval összekapcsolás
            member_report = member_stats.merge(
                dim_member[dim_member['is_current'] == True][
                    ['member_id', 'first_name', 'last_name', 'age_group']
                ],
                left_on='member_key',
                right_on='member_id',
                how='left'
            )
            
            # Top 10 legaktívabb
            st.subheader("🏆 Top 10 Legaktívabb Tag")
            top_members = member_report.nlargest(10, 'visit_count')[
                ['first_name', 'last_name', 'visit_count', 'avg_duration', 'total_duration', 'age_group']
            ].copy()
            
            # Formázás
            top_members['avg_duration'] = top_members['avg_duration'].round(0).astype(int)
            top_members.columns = ['Keresztnév', 'Vezetéknév', 'Látogatások', 'Átlag időtartam (p)', 'Összes idő (p)', 'Korosztály']
            
            st.dataframe(top_members, use_container_width=True)
            
            # Korosztály szerinti aktivitás
            st.subheader("📊 Aktivitás Korosztály Szerint")
            age_group_stats = member_report.groupby('age_group').agg({
                'visit_count': ['mean', 'sum'],
                'avg_duration': 'mean'
            }).round(1).reset_index()
            
            age_group_stats.columns = ['Korosztály', 'Átlag látogatás/fő', 'Összes látogatás', 'Átlag időtartam']
            st.dataframe(age_group_stats, use_container_width=True)
        else:
            st.info("Nincs elég adat az elemzéshez. Futtassa az ETL folyamatot!")
    
    with tab2:
        st.subheader("Idősorok és Trendek")
        
        # Napi látogatások trend
        if not fact_visits.empty:
            fact_visits['visit_date'] = pd.to_datetime(fact_visits['check_in_time']).dt.date
            
            daily_visits = fact_visits.groupby('visit_date').agg({
                'visit_key': 'count',
                'member_key': 'nunique',
                'duration_minutes': 'mean'
            }).reset_index()
            
            daily_visits.columns = ['Dátum', 'Látogatások', 'Egyedi tagok', 'Átlag időtartam']
            
            st.subheader("📅 Napi Látogatási Trend (Utolsó 14 nap)")
            st.dataframe(daily_visits.tail(14), use_container_width=True)
            
            # Óránkénti eloszlás
            fact_visits['hour'] = pd.to_datetime(fact_visits['check_in_time']).dt.hour
            hourly_dist = fact_visits.groupby('hour').size().reset_index(name='visits')
            
            st.subheader("⏰ Órák Szerinti Eloszlás")
            st.dataframe(hourly_dist, use_container_width=True)
            
            # Csúcsidő elemzés
            peak_hours = hourly_dist[hourly_dist['visits'] >= hourly_dist['visits'].quantile(0.8)]
            st.write(f"**Csúcsidő órák**: {', '.join(map(str, peak_hours['hour'].tolist()))}")
        else:
            st.info("Nincs látogatási adat a DWH-ban.")
        
        # Bevételi trendek
        fact_revenue = supabase_get("fact_revenue")
        if not fact_revenue.empty:
            fact_revenue['revenue_month'] = pd.to_datetime(fact_revenue['payment_date']).dt.to_period('M')
            
            monthly_revenue = fact_revenue.groupby('revenue_month').agg({
                'payment_amount': ['sum', 'count', 'mean']
            }).reset_index()
            
            monthly_revenue.columns = ['Hónap', 'Összes bevétel', 'Tranzakciók', 'Átlag tranzakció']
            monthly_revenue['Hónap'] = monthly_revenue['Hónap'].astype(str)
            
            st.subheader("💰 Havi Bevételi Trend")
            st.dataframe(monthly_revenue, use_container_width=True)
    
    with tab3:
        st.subheader("Üzleti Jelentések")
        
        # Tag retenciós elemzés
        if not fact_visits.empty and not dim_member.empty:
            # Új vs visszatérő tagok
            member_first_visit = fact_visits.groupby('member_key')['check_in_time'].min().reset_index()
            member_first_visit['first_visit_date'] = pd.to_datetime(member_first_visit['check_in_time']).dt.date
            
            # Utolsó 30 napban csatlakozott új tagok
            thirty_days_ago = datetime.now().date() - timedelta(days=30)
            new_members = member_first_visit[member_first_visit['first_visit_date'] >= thirty_days_ago]
            
            st.subheader("👥 Tag Retenciós Elemzés")
            st.write(f"**Új tagok (30 nap)**: {len(new_members)}")
            
            # Visszatérési arány
            repeat_visitors = fact_visits.groupby('member_key').size()
            single_visit = len(repeat_visitors[repeat_visitors == 1])
            multiple_visits = len(repeat_visitors[repeat_visitors > 1])
            
            retention_rate = (multiple_visits / len(repeat_visitors) * 100) if len(repeat_visitors) > 0 else 0
            
            st.write(f"**Egyszeri látogatók**: {single_visit}")
            st.write(f"**Visszatérő látogatók**: {multiple_visits}")
            st.write(f"**Visszatérési arány**: {retention_rate:.1f}%")
            
            # Hétköznap vs hétvége
            fact_visits['weekday'] = pd.to_datetime(fact_visits['check_in_time']).dt.weekday
            fact_visits['is_weekend'] = fact_visits['weekday'] >= 5
            
            weekend_stats = fact_visits.groupby('is_weekend').agg({
                'visit_key': 'count',
                'duration_minutes': 'mean'
            }).reset_index()
            
            weekend_stats['day_type'] = weekend_stats['is_weekend'].map({True: 'Hétvége', False: 'Hétköznap'})
            weekend_stats = weekend_stats[['day_type', 'visit_key', 'duration_minutes']]
            weekend_stats.columns = ['Nap típusa', 'Látogatások', 'Átlag időtartam']
            
            st.subheader("📅 Hétköznap vs Hétvége")
            st.dataframe(weekend_stats, use_container_width=True)

if __name__ == "__main__":
    main()
