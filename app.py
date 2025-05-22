# app.py - FitZone Adattárház Projekt
import streamlit as st
import pandas as pd
import requests
import json
from datetime import datetime, timedelta

st.set_page_config(
    page_title="FitZone Adattárház",
    page_icon="🏋️",
    layout="wide"
)

# Supabase konfiguráció
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_ANON_KEY"]

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

# === ALAPVETŐ API FÜGGVÉNYEK ===
def supabase_query(table, method="GET", data=None, select="*", filters=None):
    """Univerzális Supabase API hívás"""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    
    if method == "GET":
        url += f"?select={select}"
        if filters:
            for key, value in filters.items():
                url += f"&{key}={value}"
        response = requests.get(url, headers=headers)
    
    elif method == "POST":
        response = requests.post(url, headers=headers, data=json.dumps(data))
    
    elif method == "PATCH":
        if filters:
            for key, value in filters.items():
                url += f"?{key}=eq.{value}"
        response = requests.patch(url, headers=headers, data=json.dumps(data))
    
    elif method == "DELETE":
        if filters:
            for key, value in filters.items():
                url += f"?{key}=eq.{value}"
        response = requests.delete(url, headers=headers)
    
    if response.ok:
        return response.json() if method == "GET" else True
    else:
        st.error(f"API hiba: {response.text}")
        return [] if method == "GET" else False

def get_df(table, filters=None):
    """DataFrame lekérése"""
    data = supabase_query(table, filters=filters)
    return pd.DataFrame(data) if data else pd.DataFrame()

# === ETL FOLYAMAT ===
def run_etl_process():
    """Teljes ETL folyamat futtatása"""
    st.info("🔄 ETL folyamat futtatása...")
    
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
    """Tag dimenzió ETL (SCD Type 2)"""
    # OLTP tagok lekérése
    members_df = get_df("members")
    if members_df.empty:
        return 0
    
    # Jelenlegi DWH dimenzió
    existing_dim = get_df("dim_member")
    
    processed = 0
    
    for _, member in members_df.iterrows():
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
        
        # Tag az óta napok
        member_since_days = 0
        if pd.notna(member.get('join_date')):
            join_date = pd.to_datetime(member['join_date'])
            member_since_days = (datetime.now() - join_date).days
        
        new_record = {
            "member_id": int(member['member_id']),
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
        
        # SCD2 logika
        if not existing_dim.empty:
            current_records = existing_dim[
                (existing_dim['member_id'] == member['member_id']) & 
                (existing_dim['is_current'] == True)
            ]
            
            if not current_records.empty:
                current_record = current_records.iloc[0]
                
                # Változás ellenőrzése
                changed = (
                    current_record['first_name'] != member['first_name'] or
                    current_record['last_name'] != member['last_name'] or
                    current_record['email'] != member['email'] or
                    current_record['member_status'] != member['status']
                )
                
                if changed:
                    # Régi rekord lezárása
                    supabase_query("dim_member", "PATCH", 
                                 {"valid_to": datetime.now().date().isoformat(), "is_current": False},
                                 filters={"member_key": current_record['member_key']})
                    
                    # Új rekord beszúrása
                    if supabase_query("dim_member", "POST", new_record):
                        processed += 1
                # Ha nincs változás, nem csinálunk semmit
            else:
                # Új tag
                if supabase_query("dim_member", "POST", new_record):
                    processed += 1
        else:
            # Első betöltés
            if supabase_query("dim_member", "POST", new_record):
                processed += 1
    
    return processed

def etl_dim_date():
    """Dátum dimenzió feltöltése"""
    existing_dates = get_df("dim_date")
    
    # Utolsó 1 év + következő 6 hónap
    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now() + timedelta(days=180)
    
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    new_records = []
    existing_keys = set(existing_dates['date_key'].astype(str)) if not existing_dates.empty else set()
    
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
    
    # Tömeges beszúrás
    if new_records:
        if supabase_query("dim_date", "POST", new_records):
            return len(new_records)
    
    return 0

def etl_fact_visits():
    """Látogatás fact tábla feltöltése"""
    # OLTP check_ins lekérése
    check_ins_df = get_df("check_ins")
    if check_ins_df.empty:
        return 0
    
    # Meglévő fact rekordok
    existing_visits = get_df("fact_visits")
    existing_keys = set(existing_visits['visit_key']) if not existing_visits.empty else set()
    
    new_records = []
    
    for _, visit in check_ins_df.iterrows():
        if pd.notna(visit.get('check_in_time')):
            check_in_time = pd.to_datetime(visit['check_in_time'])
            
            # Kulcsok generálása
            date_key = int(check_in_time.strftime('%Y%m%d'))
            time_key = check_in_time.hour * 100 + (check_in_time.minute // 15) * 15
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
                    "time_key": time_key,
                    "member_key": int(visit['member_id']),
                    "check_in_time": check_in_time.isoformat(),
                    "check_out_time": pd.to_datetime(visit['check_out_time']).isoformat() if pd.notna(visit.get('check_out_time')) else None,
                    "duration_minutes": duration_minutes
                }
                new_records.append(record)
    
    if new_records:
        if supabase_query("fact_visits", "POST", new_records):
            return len(new_records)
    
    return 0

def etl_fact_revenue():
    """Bevétel fact tábla feltöltése"""
    # OLTP payments lekérése
    payments_df = get_df("payments")
    if payments_df.empty:
        return 0
    
    # Meglévő fact rekordok
    existing_revenue = get_df("fact_revenue")
    existing_keys = set(existing_revenue['revenue_key']) if not existing_revenue.empty else set()
    
    new_records = []
    
    for _, payment in payments_df.iterrows():
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
                new_records.append(record)
    
    if new_records:
        if supabase_query("fact_revenue", "POST", new_records):
            return len(new_records)
    
    return 0

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
            "⚙️ ETL Folyamatok",
            "📈 DWH Lekérdezések"
        ]
    )
    
    if page == "📊 Dashboard & KPI":
        show_dashboard()
    elif page == "👥 Tag Kezelés (OLTP)":
        show_member_management()
    elif page == "🚪 Be/Kiléptetés (OLTP)":
        show_checkin_checkout()
    elif page == "⚙️ ETL Folyamatok":
        show_etl_management()
    elif page == "📈 DWH Lekérdezések":
        show_dwh_queries()

def show_dashboard():
    """KPI Dashboard"""
    st.header("📊 Főoldal - KPI Mutatók")
    
    # OLTP KPI-k
    st.subheader("🔧 OLTP Rendszer Státusz")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        members_df = get_df("members")
        total_members = len(members_df)
        active_members = len(members_df[members_df['status'] == 'ACTIVE']) if not members_df.empty else 0
        st.metric("Összes tag", total_members)
        st.metric("Aktív tagok", active_members)
    
    with col2:
        check_ins_df = get_df("check_ins")
        today = datetime.now().date()
        if not check_ins_df.empty:
            check_ins_df['check_in_date'] = pd.to_datetime(check_ins_df['check_in_time']).dt.date
            today_visits = len(check_ins_df[check_ins_df['check_in_date'] == today])
            currently_inside = len(check_ins_df[
                (check_ins_df['check_in_date'] == today) & 
                pd.isna(check_ins_df['check_out_time'])
            ])
        else:
            today_visits = 0
            currently_inside = 0
        
        st.metric("Mai látogatások", today_visits)
        st.metric("Most bent", currently_inside)
    
    with col3:
        payments_df = get_df("payments")
        if not payments_df.empty:
            this_month = datetime.now().replace(day=1).date()
            payments_df['payment_date'] = pd.to_datetime(payments_df['payment_date']).dt.date
            monthly_revenue = payments_df[payments_df['payment_date'] >= this_month]['amount'].sum()
            total_payments = len(payments_df)
        else:
            monthly_revenue = 0
            total_payments = 0
        
        st.metric("Havi bevétel", f"{monthly_revenue:,.0f} Ft")
        st.metric("Összes fizetés", total_payments)
    
    with col4:
        memberships_df = get_df("memberships")
        membership_types_df = get_df("membership_types")
        
        active_memberships = len(memberships_df) if not memberships_df.empty else 0
        membership_types_count = len(membership_types_df) if not membership_types_df.empty else 0
        
        st.metric("Aktív tagságok", active_memberships)
        st.metric("Tagság típusok", membership_types_count)
    
    # DWH KPI-k
    st.divider()
    st.subheader("🏢 Adattárház (DWH) Státusz")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        dim_member_df = get_df("dim_member")
        dim_date_df = get_df("dim_date")
        
        dim_member_count = len(dim_member_df) if not dim_member_df.empty else 0
        dim_date_count = len(dim_date_df) if not dim_date_df.empty else 0
        
        st.metric("Dim_member rekordok", dim_member_count)
        st.metric("Dim_date rekordok", dim_date_count)
    
    with col2:
        fact_visits_df = get_df("fact_visits")
        fact_revenue_df = get_df("fact_revenue")
        
        fact_visits_count = len(fact_visits_df) if not fact_visits_df.empty else 0
        fact_revenue_count = len(fact_revenue_df) if not fact_revenue_df.empty else 0
        
        st.metric("Fact_visits rekordok", fact_visits_count)
        st.metric("Fact_revenue rekordok", fact_revenue_count)
    
    with col3:
        if not fact_visits_df.empty:
            avg_visit_duration = fact_visits_df[fact_visits_df['duration_minutes'] > 0]['duration_minutes'].mean()
            total_visit_time = fact_visits_df['duration_minutes'].sum()
        else:
            avg_visit_duration = 0
            total_visit_time = 0
        
        st.metric("Átlag edzésidő", f"{avg_visit_duration:.0f} perc" if avg_visit_duration > 0 else "N/A")
        st.metric("Összes edzésidő", f"{total_visit_time:,.0f} perc")
    
    with col4:
        if not fact_revenue_df.empty:
            total_dwh_revenue = fact_revenue_df['payment_amount'].sum()
            avg_payment = fact_revenue_df['payment_amount'].mean()
        else:
            total_dwh_revenue = 0
            avg_payment = 0
        
        st.metric("DWH összes bevétel", f"{total_dwh_revenue:,.0f} Ft")
        st.metric("Átlag fizetés", f"{avg_payment:.0f} Ft" if avg_payment > 0 else "N/A")

def show_member_management():
    """Tag kezelés (OLTP műveletek)"""
    st.header("👥 Tag Kezelés - OLTP Műveletek")
    
    tab1, tab2, tab3, tab4 = st.tabs(["👀 Tag Lista", "➕ Új Tag", "✏️ Tag Módosítás", "🗑️ Tag Törlés"])
    
    with tab1:
        st.subheader("Tagok listája")
        members_df = get_df("members")
        
        if not members_df.empty:
            # Szűrők
            col1, col2 = st.columns(2)
            with col1:
                status_filter = st.selectbox("Státusz", ["Mind", "ACTIVE", "INACTIVE"])
            with col2:
                search_term = st.text_input("Keresés (név/email)")
            
            # Szűrés
            filtered_df = members_df.copy()
            if status_filter != "Mind":
                filtered_df = filtered_df[filtered_df['status'] == status_filter]
            
            if search_term:
                mask = (
                    filtered_df['first_name'].str.contains(search_term, case=False, na=False) |
                    filtered_df['last_name'].str.contains(search_term, case=False, na=False) |
                    filtered_df['email'].str.contains(search_term, case=False, na=False)
                )
                filtered_df = filtered_df[mask]
            
            st.dataframe(filtered_df, use_container_width=True)
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
                    
                    if supabase_query("members", "POST", new_member):
                        st.success("✅ Tag sikeresen regisztrálva!")
                        st.rerun()
                    else:
                        st.error("❌ Hiba a regisztráció során!")
                else:
                    st.error("❌ Kötelező mezők kitöltése szükséges!")
    
    with tab3:
        st.subheader("Tag adatok módosítása")
        
        members_df = get_df("members")
        if not members_df.empty:
            # Tag kiválasztása
            member_options = {
                f"{row['first_name']} {row['last_name']} ({row['email']})": row['member_id']
                for _, row in members_df.iterrows()
            }
            
            selected_member_name = st.selectbox("Válassz tagot", list(member_options.keys()))
            selected_member_id = member_options[selected_member_name]
            
            # Jelenlegi adatok betöltése
            selected_member = members_df[members_df['member_id'] == selected_member_id].iloc[0]
            
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
                    
                    if supabase_query("members", "PATCH", updated_data, filters={"member_id": selected_member_id}):
                        st.success("✅ Tag adatai frissítve!")
                        st.rerun()
                    else:
                        st.error("❌ Hiba a frissítés során!")
    
    with tab4:
        st.subheader("Tag törlése")
        
        members_df = get_df("members")
        if not members_df.empty:
            # Tag kiválasztása
            member_options = {
                f"{row['first_name']} {row['last_name']} ({row['email']})": row['member_id']
                for _, row in members_df.iterrows()
            }
            
            selected_member_name = st.selectbox("Válassz törlendő tagot", list(member_options.keys()))
            selected_member_id = member_options[selected_member_name]
            
            st.warning("⚠️ FIGYELEM: A tag törlése végleges és visszafordíthatatlan!")
            
            if st.button("🗑️ Tag végleges törlése", type="primary"):
                if supabase_query("members", "DELETE", filters={"member_id": selected_member_id}):
                    st.success("✅ Tag sikeresen törölve!")
                    st.rerun()
                else:
                    st.error("❌ Hiba a törlés során!")

def show_checkin_checkout():
    """Be/kiléptetés kezelése"""
    st.header("🚪 Be/Kiléptetés - OLTP Műveletek")
    
    tab1, tab2, tab3 = st.tabs(["🔓 Beléptetés", "🔒 Kiléptetés", "👁️ Jelenlegi Státusz"])
    
    with tab1:
        st.subheader("Tag beléptetése")
        
        members_df = get_df("members", filters={"status": "eq.ACTIVE"})
        
        if not members_df.empty:
            # Keresés
            search_term = st.text_input("🔍 Tag keresése (név vagy email)")
            
            if search_term:
                mask = (
                    members_df['first_name'].str.contains(search_term, case=False, na=False) |
                    members_df['last_name'].str.contains(search_term, case=False, na=False) |
                    members_df['email'].str.contains(search_term, case=False, na=False)
                )
                filtered_members = members_df[mask]
            else:
                filtered_members = members_df.head(10)  # Első 10 tag
            
            # Tagok listája beléptetés gombokkal
            for _, member in filtered_members.iterrows():
                col1, col2 = st.columns([4, 1])
                
                with col1:
                    st.write(f"**{member['first_name']} {member['last_name']}**")
                    st.caption(f"📧 {member['email']} | 📱 {member.get('phone', 'N/A')}")
                
                with col2:
                    if st.button("🔓 Beléptet", key=f"checkin_{member['member_id']}"):
                        # Ellenőrzés: már bent van-e
                        existing_checkins = get_df("check_ins", filters={"member_id": f"eq.{member['member_id']}"})
                        
                        active_checkin = None
                        if not existing_checkins.empty:
                            active_checkin = existing_checkins[pd.isna(existing_checkins['check_out_time'])]
                        
                        if active_checkin is not None and not active_checkin.empty:
                            st.error(f"❌ {member['first_name']} már bent van!")
                        else:
                            # Beléptetés
                            checkin_data = {
                                "member_id": int(member['member_id']),
                                "check_in_time": datetime.now().isoformat()
                            }
                            
                            if supabase_query("check_ins", "POST", checkin_data):
                                st.success(f"✅ {member['first_name']} sikeresen beléptetve!")
                                st.rerun()
                            else:
                                st.error("❌ Beléptetési hiba!")
                
                st.divider()
        else:
            st.info("Nincsenek aktív tagok.")
    
    with tab2:
        st.subheader("Tag kiléptetése")
        
        # Bent lévő tagok lekérése
        check_ins_df = get_df("check_ins")
        
        if not check_ins_df.empty:
            # Csak a ki nem lépett tagok
            active_checkins = check_ins_df[pd.isna(check_ins_df['check_out_time'])]
            
            if not active_checkins.empty:
                # Tag adatok hozzákapcsolása
                members_df = get_df("members")
                
                active_with_members = active_checkins.merge(
                    members_df[['member_id', 'first_name', 'last_name', 'email']], 
                    on='member_id',
                    how='left'
                )
                
                for _, checkin in active_with_members.iterrows():
                    col1, col2 = st.columns([4, 1])
                    
                    with col1:
                        check_in_time = pd.to_datetime(checkin['check_in_time'])
                        duration = datetime.now() - check_in_time
                        hours = int(duration.total_seconds() // 3600)
                        minutes = int((duration.total_seconds() % 3600) // 60)
                        
                        st.write(f"**{checkin['first_name']} {checkin['last_name']}**")
                        st.caption(f"📧 {checkin['email']} | ⏰ Bent: {hours}ó {minutes}p")
                    
                    with col2:
                        if st.button("🔒 Kiléptet", key=f"checkout_{checkin['checkin_id']}"):
                            checkout_data = {
                                "check_out_time": datetime.now().isoformat()
                            }
                            
                            if supabase_query("check_ins", "PATCH", checkout_data, 
                                            filters={"checkin_id": checkin['checkin_id']}):
                                st.success(f"✅ {checkin['first_name']} sikeresen kiléptetve!")
                                st.rerun()
                            else:
                                st.error("❌ Kiléptetési hiba!")
                    
                    st.divider()
            else:
                st.info("🏠 Jelenleg nincs bent senki.")
        else:
            st.info("📝 Még nem volt látogatás.")
    
    with tab3:
        st.subheader("Jelenlegi bent lévők")
        
        check_ins_df = get_df("check_ins")
        
        if not check_ins_df.empty:
            active_checkins = check_ins_df[pd.isna(check_ins_df['check_out_time'])]
            
            if not active_checkins.empty:
                # Tag adatok hozzákapcsolása
                members_df = get_df("members")
                
                status_data = active_checkins.merge(
                    members_df[['member_id', 'first_name', 'last_name', 'email']], 
                    on='member_id',
                    how='left'
                )
                
                # Időtartam számítása
                status_data['check_in_time'] = pd.to_datetime(status_data['check_in_time'])
                status_data['duration'] = (datetime.now() - status_data['check_in_time']).dt.total_seconds() / 60
                status_data['duration_str'] = status_data['duration'].apply(
                    lambda x: f"{int(x//60)}ó {int(x%60)}p"
                )
                
                # Megjelenítés
                display_df = status_data[['first_name', 'last_name', 'email', 'check_in_time', 'duration_str']].copy()
                display_df.columns = ['Keresztnév', 'Vezetéknév', 'Email', 'Belépés ideje', 'Bent töltött idő']
                
                st.metric("🏠 Bent lévők száma", len(status_data))
                st.dataframe(display_df, use_container_width=True)
            else:
                st.info("🏠 Jelenleg nincs bent senki.")
        else:
            st.info("📝 Még nem volt látogatás.")

def show_etl_management():
    """ETL folyamatok kezelése"""
    st.header("⚙️ ETL Folyamatok Kezelése")
    
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
        members_oltp = len(get_df("members"))
        members_dwh = len(get_df("dim_member"))
        st.metric("OLTP Tagok", members_oltp)
        st.metric("DWH Dim_member", members_dwh)
    
    with col2:
        checkins_oltp = len(get_df("check_ins"))
        visits_dwh = len(get_df("fact_visits"))
        st.metric("OLTP Check-ins", checkins_oltp)
        st.metric("DWH Fact_visits", visits_dwh)
    
    with col3:
        payments_oltp = len(get_df("payments"))
        revenue_dwh = len(get_df("fact_revenue"))
        st.metric("OLTP Payments", payments_oltp)
        st.metric("DWH Fact_revenue", revenue_dwh)
    
    with col4:
        dates_dwh = len(get_df("dim_date"))
        st.metric("DWH Dim_date", dates_dwh)
        st.metric("Utolsó ETL", "Manuális")
    
    # ETL futtatás
    st.divider()
    st.subheader("🚀 ETL Futtatása")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 Teljes ETL Futtatása", type="primary", use_container_width=True):
            with st.spinner("ETL folyamat futtatása..."):
                results = run_etl_process()
                
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
    
    # ETL log/history (egyszerűsített)
    st.divider()
    st.subheader("📝 ETL Információk")
    
    tab1, tab2 = st.tabs(["🔍 Adatminőség", "📋 Táblák Sémája"])
    
    with tab1:
        st.markdown("### 🔍 Adatminőség Ellenőrzés")
        
        # OLTP ellenőrzések
        members_df = get_df("members")
        check_ins_df = get_df("check_ins")
        
        if not members_df.empty:
            missing_emails = members_df['email'].isna().sum()
            missing_birthdates = members_df['birth_date'].isna().sum()
            
            st.write(f"📧 **Hiányzó email címek**: {missing_emails}")
            st.write(f"🎂 **Hiányzó születési dátumok**: {missing_birthdates}")
        
        if not check_ins_df.empty:
            incomplete_visits = check_ins_df['check_out_time'].isna().sum()
            st.write(f"🚪 **Nem lezárt látogatások**: {incomplete_visits}")
        
        # DWH ellenőrzések
        dim_member_df = get_df("dim_member")
        fact_visits_df = get_df("fact_visits")
        
        if not dim_member_df.empty:
            active_members = len(dim_member_df[dim_member_df['is_current'] == True])
            st.write(f"👥 **Aktív tag rekordok (DWH)**: {active_members}")
        
        if not fact_visits_df.empty:
            zero_duration = len(fact_visits_df[fact_visits_df['duration_minutes'] == 0])
            st.write(f"⏱️ **Nulla időtartamú látogatások**: {zero_duration}")
    
    with tab2:
        st.markdown("### 📋 Adatbázis Sémák")
        
        schema_info = """
        **OLTP Táblák:**
        - `members`: Tagok alapadatai
        - `check_ins`: Be/kilépési rekordok  
        - `payments`: Fizetési tranzakciók
        - `memberships`: Tagság kapcsolatok
        - `membership_types`: Tagság típusok
        
        **DWH Táblák:**
        - `dim_member`: Tag dimenzió (SCD2)
        - `dim_date`: Dátum dimenzió
        - `fact_visits`: Látogatási tény tábla
        - `fact_revenue`: Bevételi tény tábla
        """
        
        st.markdown(schema_info)

def show_dwh_queries():
    """DWH lekérdezések és jelentések"""
    st.header("📈 Adattárház Lekérdezések")
    
    tab1, tab2, tab3 = st.tabs(["📊 Alapvető Lekérdezések", "📈 Idősorok", "📋 Jelentések"])
    
    with tab1:
        st.subheader("Alapvető DWH Lekérdezések")
        
        # Legaktívabb tagok
        fact_visits_df = get_df("fact_visits")
        dim_member_df = get_df("dim_member")
        
        if not fact_visits_df.empty and not dim_member_df.empty:
            # Tagok látogatási statisztikái
            member_stats = fact_visits_df.groupby('member_key').agg({
                'visit_key': 'count',
                'duration_minutes': ['mean', 'sum']
            }).reset_index()
            
            member_stats.columns = ['member_key', 'visit_count', 'avg_duration', 'total_duration']
            
            # Dimenzióval összekapcsolás
            member_report = member_stats.merge(
                dim_member_df[dim_member_df['is_current'] == True][
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
    
    with tab2:
        st.subheader("Idősorok és Trendek")
        
        # Napi látogatások trend
        if not fact_visits_df.empty:
            fact_visits_df['visit_date'] = pd.to_datetime(fact_visits_df['check_in_time']).dt.date
            
            daily_visits = fact_visits_df.groupby('visit_date').agg({
                'visit_key': 'count',
                'member_key': 'nunique',
                'duration_minutes': 'mean'
            }).reset_index()
            
            daily_visits.columns = ['Dátum', 'Látogatások', 'Egyedi tagok', 'Átlag időtartam']
            
            st.subheader("📅 Napi Látogatási Trend")
            st.dataframe(daily_visits.tail(14), use_container_width=True)  # Utolsó 2 hét
            
            # Heti összesítő
            fact_visits_df['week'] = pd.to_datetime(fact_visits_df['check_in_time']).dt.isocalendar().week
            fact_visits_df['year'] = pd.to_datetime(fact_visits_df['check_in_time']).dt.year
            
            weekly_stats = fact_visits_df.groupby(['year', 'week']).agg({
                'visit_key': 'count',
                'member_key': 'nunique',
                'duration_minutes': 'mean'
            }).reset_index()
            
            weekly_stats.columns = ['Év', 'Hét', 'Látogatások', 'Egyedi tagok', 'Átlag időtartam']
            
            st.subheader("📊 Heti Összesítő")
            st.dataframe(weekly_stats.tail(8), use_container_width=True)  # Utolsó 8 hét
        
        # Bevételi trendek
        fact_revenue_df = get_df("fact_revenue")
        if not fact_revenue_df.empty:
            fact_revenue_df['revenue_date'] = pd.to_datetime(fact_revenue_df['payment_date']).dt.date
            fact_revenue_df['month'] = pd.to_datetime(fact_revenue_df['payment_date']).dt.to_period('M')
            
            monthly_revenue = fact_revenue_df.groupby('month').agg({
                'payment_amount': ['sum', 'count', 'mean']
            }).reset_index()
            
            monthly_revenue.columns = ['Hónap', 'Összes bevétel', 'Tranzakciók', 'Átlag tranzakció']
            monthly_revenue['Hónap'] = monthly_revenue['Hónap'].astype(str)
            
            st.subheader("💰 Havi Bevételi Trend")
            st.dataframe(monthly_revenue, use_container_width=True)
    
    with tab3:
        st.subheader("Üzleti Jelentések")
        
        # Csúcsidő elemzés
        if not fact_visits_df.empty:
            fact_visits_df['hour'] = pd.to_datetime(fact_visits_df['check_in_time']).dt.hour
            
            hourly_distribution = fact_visits_df.groupby('hour').size().reset_index(name='visits')
            peak_hours = hourly_distribution[hourly_distribution['visits'] >= hourly_distribution['visits'].quantile(0.8)]
            
            st.subheader("⏰ Csúcsidő Elemzés")
            st.write(f"**Csúcsidő órák**: {', '.join(map(str, peak_hours['hour'].tolist()))}")
            
            total_visits = hourly_distribution['visits'].sum()
            peak_visits = peak_hours['visits'].sum()
            peak_ratio = (peak_visits / total_visits * 100) if total_visits > 0 else 0
            
            st.write(f"**Csúcsidő kihasználtság**: {peak_ratio:.1f}%")
            
            st.dataframe(hourly_distribution, use_container_width=True)
        
        # Tag retenciós elemzés
        if not fact_visits_df.empty and not dim_member_df.empty:
            # Új vs visszatérő tagok elemzése
            member_first_visit = fact_visits_df.groupby('member_key')['check_in_time'].min().reset_index()
            member_first_visit['first_visit_date'] = pd.to_datetime(member_first_visit['check_in_time']).dt.date
            
            # Utolsó 30 napban csatlakozott új tagok
            thirty_days_ago = datetime.now().date() - timedelta(days=30)
            new_members = member_first_visit[member_first_visit['first_visit_date'] >= thirty_days_ago]
            
            st.subheader("👥 Tag Retenciós Elemzés")
            st.write(f"**Új tagok (30 nap)**: {len(new_members)}")
            
            # Visszatérési arány
            repeat_visitors = fact_visits_df.groupby('member_key').size()
            single_visit = len(repeat_visitors[repeat_visitors == 1])
            multiple_visits = len(repeat_visitors[repeat_visitors > 1])
            
            retention_rate = (multiple_visits / len(repeat_visitors) * 100) if len(repeat_visitors) > 0 else 0
            
            st.write(f"**Egyszeri látogatók**: {single_visit}")
            st.write(f"**Visszatérő látogatók**: {multiple_visits}")
            st.write(f"**Visszatérési arány**: {retention_rate:.1f}%")

if __name__ == "__main__":
    main()
