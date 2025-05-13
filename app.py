# app.py - FitZone Teljes Alkalmazás
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import json
from datetime import datetime, timedelta
import time

st.set_page_config(
    page_title="FitZone Management System",
    page_icon="🏋️",
    layout="wide",
    initial_sidebar_state="expanded"
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
    """Adatok beszúrása"""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    response = requests.post(url, headers=headers, data=json.dumps(data))
    return response.ok

def supabase_update(table, id_field, id_value, data):
    """Adatok frissítése"""
    url = f"{SUPABASE_URL}/rest/v1/{table}?{id_field}=eq.{id_value}"
    response = requests.patch(url, headers=headers, data=json.dumps(data))
    return response.ok

# ETL Funkciók
def etl_update_dim_member():
    """Tag dimenzió frissítése - SCD Type 1"""
    with st.spinner("Tag dimenzió frissítése..."):
        # OLTP adatok lekérése
        members = supabase_get("members")
        memberships = supabase_get("memberships")
        membership_types = supabase_get("membership_types")
        
        if members.empty:
            return 0
        
        # Aktív tagságok meghatározása
        current_date = pd.Timestamp.now()
        active_memberships = memberships[
            (pd.to_datetime(memberships['start_date']) <= current_date) & 
            (pd.to_datetime(memberships['end_date']) >= current_date)
        ].copy()
        
        # Összekapcsolás
        member_with_type = members.merge(
            active_memberships[['member_id', 'type_id']], 
            on='member_id', 
            how='left'
        )
        
        if not membership_types.empty:
            member_with_type = member_with_type.merge(
                membership_types[['type_id', 'type_name']], 
                on='type_id', 
                how='left'
            )
        
        # Dim_member készítése
        dim_members = []
        for _, row in member_with_type.iterrows():
            # Életkor számítás
            age_group = 'Unknown'
            if pd.notna(row.get('birth_date')):
                birth_date = pd.to_datetime(row['birth_date'])
                age = (current_date - birth_date).days // 365
                if age < 25: age_group = '<25'
                elif age < 35: age_group = '25-35'
                elif age < 45: age_group = '35-45'
                elif age < 55: age_group = '45-55'
                else: age_group = '55+'
            
            # Tagság időtartam
            join_date = pd.to_datetime(row.get('join_date', current_date))
            member_since_days = (current_date - join_date).days
            
            dim_member = {
                "member_id": int(row['member_id']),
                "first_name": row['first_name'],
                "last_name": row['last_name'],
                "email": row['email'],
                "age_group": age_group,
                "member_since_days": member_since_days,
                "current_membership_type": row.get('type_name', 'None'),
                "member_status": row['status'],
                "is_current": True,
                "last_updated": current_date.isoformat()
            }
            dim_members.append(dim_member)
        
        # Batch insert (egyszerűsített - Supabase API korlátozások miatt egyesével)
        success_count = 0
        for member in dim_members:
            if supabase_insert("dim_member", member):
                success_count += 1
        
        return success_count

def etl_update_fact_visits():
    """Látogatás tények frissítése"""
    with st.spinner("Látogatás tények frissítése..."):
        # Új látogatások az utolsó ETL óta
        check_ins = supabase_get("check_ins")
        dim_members = supabase_get("dim_member")
        
        if check_ins.empty or dim_members.empty:
            return 0
        
        # Az utolsó 24 óra látogatásai
        cutoff_time = pd.Timestamp.now() - pd.Timedelta(hours=24)
        recent_checkins = check_ins[
            pd.to_datetime(check_ins['check_in_time']) > cutoff_time
        ].copy()
        
        fact_visits = []
        for _, checkin in recent_checkins.iterrows():
            # Dim_member keresése
            member_match = dim_members[dim_members['member_id'] == checkin['member_id']]
            
            if not member_match.empty:
                # Időpontok
                checkin_time = pd.to_datetime(checkin['check_in_time'])
                date_key = int(checkin_time.strftime('%Y%m%d'))
                time_key = checkin_time.hour * 100 + checkin_time.minute
                
                # Időtartam
                duration_minutes = None
                if pd.notna(checkin.get('check_out_time')):
                    checkout_time = pd.to_datetime(checkin['check_out_time'])
                    duration_minutes = int((checkout_time - checkin_time).total_seconds() / 60)
                
                fact_visit = {
                    "date_key": date_key,
                    "time_key": time_key,
                    "member_key": int(member_match.iloc[0]['member_key']),
                    "check_in_time": checkin_time.isoformat(),
                    "check_out_time": checkin.get('check_out_time'),
                    "duration_minutes": duration_minutes
                }
                fact_visits.append(fact_visit)
        
        # Beszúrás
        success_count = 0
        for visit in fact_visits:
            if supabase_insert("fact_visits", visit):
                success_count += 1
        
        return success_count

# Fő üzleti folyamatok automatizált elemzése
def analyze_business_metrics():
    """Fő üzleti metrikák automatizált elemzése"""
    metrics = {}
    
    # 1. Kihasználtság elemzés
    check_ins = supabase_get("check_ins")
    if not check_ins.empty:
        # Jelenleg bent lévők
        now = pd.Timestamp.now()
        active_checkins = check_ins[
            (pd.notna(check_ins['check_in_time'])) & 
            (pd.isna(check_ins['check_out_time']))
        ]
        metrics['current_occupancy'] = len(active_checkins)
        
        # Mai látogatók
        today_checkins = check_ins[
            pd.to_datetime(check_ins['check_in_time']).dt.date == now.date()
        ]
        metrics['today_visitors'] = len(today_checkins['member_id'].unique())
    
    # 2. Tagság elemzés
    members = supabase_get("members")
    memberships = supabase_get("memberships")
    
    if not members.empty:
        metrics['active_members'] = len(members[members['status'] == 'ACTIVE'])
        metrics['inactive_members'] = len(members[members['status'] == 'INACTIVE'])
        
        # Lejáró tagságok
        if not memberships.empty:
            next_week = now + pd.Timedelta(days=7)
            expiring = memberships[
                (pd.to_datetime(memberships['end_date']) <= next_week) &
                (pd.to_datetime(memberships['end_date']) >= now)
            ]
            metrics['expiring_memberships'] = len(expiring)
    
    # 3. Bevétel elemzés
    payments = supabase_get("payments")
    if not payments.empty:
        # Havi bevétel
        this_month = payments[
            pd.to_datetime(payments['payment_date']).dt.to_period('M') == 
            now.to_period('M')
        ]
        metrics['monthly_revenue'] = this_month['amount'].sum()
    
    return metrics

# Streamlit alkalmazás
def main():
    st.title("🏋️ FitZone Management System")
    
    # Sidebar navigáció
    st.sidebar.title("Navigáció")
    page = st.sidebar.selectbox(
        "Válassz funkciót",
        ["📊 Dashboard", "🚪 Recepció", "👥 Tagok", "💰 Pénzügy", "⚙️ ETL Admin"]
    )
    
    # Automatizált ETL (minden oldalfrissítésnél)
    if st.sidebar.checkbox("Auto ETL", value=True):
        last_etl = st.session_state.get('last_etl', datetime.min)
        if datetime.now() - last_etl > timedelta(minutes=5):
            with st.sidebar:
                st.info("ETL futtatása...")
                etl_update_dim_member()
                etl_update_fact_visits()
                st.session_state['last_etl'] = datetime.now()
                st.success("ETL kész!")
    
    # Oldalak
    if page == "📊 Dashboard":
        show_dashboard()
    elif page == "🚪 Recepció":
        show_reception()
    elif page == "👥 Tagok":
        show_members()
    elif page == "💰 Pénzügy":
        show_finance()
    elif page == "⚙️ ETL Admin":
        show_etl_admin()

def show_dashboard():
    """Fő dashboard - automatizált üzleti elemzések"""
    st.header("Üzleti Áttekintés")
    
    # KPI metrikák
    metrics = analyze_business_metrics()
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "🏃 Jelenleg bent", 
            metrics.get('current_occupancy', 0),
            help="Aktuálisan az edzőteremben tartózkodók"
        )
    with col2:
        st.metric(
            "📅 Mai látogatók", 
            metrics.get('today_visitors', 0),
            delta=f"+{metrics.get('current_occupancy', 0)}"
        )
    with col3:
        st.metric(
            "👥 Aktív tagok", 
            metrics.get('active_members', 0),
            delta=f"-{metrics.get('inactive_members', 0)} inaktív"
        )
    with col4:
        st.metric(
            "💵 Havi bevétel", 
            f"{metrics.get('monthly_revenue', 0):,.0f} Ft",
            help="Aktuális havi bevétel"
        )
    
    # Riasztások
    if metrics.get('expiring_memberships', 0) > 0:
        st.warning(f"⚠️ {metrics['expiring_memberships']} tagság lejár a következő héten!")
    
    # Grafikonok
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Napi látogatási trend")
        check_ins = supabase_get("check_ins")
        if not check_ins.empty:
            check_ins['date'] = pd.to_datetime(check_ins['check_in_time']).dt.date
            daily_visits = check_ins.groupby('date').size().reset_index(name='visits')
            
            fig = px.line(daily_visits, x='date', y='visits',
                         title="Látogatások az elmúlt 30 napban")
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Óránkénti kihasználtság")
        if not check_ins.empty:
            check_ins['hour'] = pd.to_datetime(check_ins['check_in_time']).dt.hour
            hourly_dist = check_ins.groupby('hour').size().reset_index(name='visits')
            
            fig = px.bar(hourly_dist, x='hour', y='visits',
                        title="Átlagos kihasználtság óránként")
            st.plotly_chart(fig, use_container_width=True)
    
    # Tagság típusok megoszlása
    st.subheader("Tagság típusok elemzése")
    dim_members = supabase_get("dim_member", filter_params={"is_current": "eq.true"})
    if not dim_members.empty:
        membership_dist = dim_members['current_membership_type'].value_counts()
        
        col1, col2 = st.columns([2, 1])
        with col1:
            fig = px.pie(values=membership_dist.values, names=membership_dist.index,
                        title="Aktív tagságok megoszlása")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.dataframe(membership_dist.reset_index(name='count'))

def show_reception():
    """Recepció - be/kiléptetés"""
    st.header("🚪 Recepció")
    
    tab1, tab2, tab3 = st.tabs(["Check-in", "Check-out", "Aktuális státusz"])
    
    with tab1:
        st.subheader("Beléptetés")
        members = supabase_get("members", filter_params={"status": "eq.ACTIVE"})
        
        if not members.empty:
            # Tag kiválasztása
            member_names = members.apply(lambda x: f"{x['first_name']} {x['last_name']} ({x['email']})", axis=1)
            selected_name = st.selectbox("Válassz tagot", member_names)
            selected_idx = member_names[member_names == selected_name].index[0]
            selected_member = members.iloc[selected_idx]
            
            # Tagság ellenőrzése
            memberships = supabase_get("memberships", 
                filter_params={"member_id": f"eq.{selected_member['member_id']}"})
            
            valid_membership = False
            if not memberships.empty:
                current_date = pd.Timestamp.now().date()
                for _, ms in memberships.iterrows():
                    if (pd.to_datetime(ms['start_date']).date() <= current_date <= 
                        pd.to_datetime(ms['end_date']).date()):
                        valid_membership = True
                        break
            
            if valid_membership:
                if st.button("✅ Beléptetés", type="primary"):
                    check_in_data = {
                        "member_id": int(selected_member['member_id']),
                        "check_in_time": datetime.now().isoformat()
                    }
                    if supabase_insert("check_ins", check_in_data):
                        st.success(f"✅ {selected_member['first_name']} {selected_member['last_name']} belépett!")
                        time.sleep(1)
                        st.rerun()
            else:
                st.error("❌ Érvénytelen vagy lejárt tagság!")
    
    with tab2:
        st.subheader("Kiléptetés")
        # Aktuálisan bent lévők
        check_ins = supabase_get("check_ins")
        if not check_ins.empty:
            active_checkins = check_ins[pd.isna(check_ins['check_out_time'])]
            
            if not active_checkins.empty:
                # Tagok adatainak lekérése
                members = supabase_get("members")
                active_with_names = active_checkins.merge(
                    members[['member_id', 'first_name', 'last_name']], 
                    on='member_id'
                )
                
                # Kiválasztás
                active_names = active_with_names.apply(
                    lambda x: f"{x['first_name']} {x['last_name']} (Belépés: {pd.to_datetime(x['check_in_time']).strftime('%H:%M')})", 
                    axis=1
                )
                selected_active = st.selectbox("Válassz kilépő tagot", active_names)
                selected_idx = active_names[active_names == selected_active].index[0]
                selected_checkin = active_with_names.iloc[selected_idx]
                
                if st.button("🚪 Kiléptetés", type="primary"):
                    update_data = {"check_out_time": datetime.now().isoformat()}
                    if supabase_update("check_ins", "checkin_id", selected_checkin['checkin_id'], update_data):
                        st.success(f"✅ {selected_checkin['first_name']} {selected_checkin['last_name']} kilépett!")
                        time.sleep(1)
                        st.rerun()
            else:
                st.info("Jelenleg nincs bent senki.")
    
    with tab3:
        st.subheader("Aktuális státusz")
        check_ins = supabase_get("check_ins")
        members = supabase_get("members")
        
        if not check_ins.empty:
            active_checkins = check_ins[pd.isna(check_ins['check_out_time'])]
            
            if not active_checkins.empty:
                active_with_details = active_checkins.merge(
                    members[['member_id', 'first_name', 'last_name', 'email']], 
                    on='member_id'
                )
                
                # Időtartam számítás
                active_with_details['duration'] = (
                    pd.Timestamp.now() - pd.to_datetime(active_with_details['check_in_time'])
                ).dt.total_seconds() / 60
                
                active_with_details['duration_str'] = active_with_details['duration'].apply(
                    lambda x: f"{int(x//60)}ó {int(x%60)}p"
                )
                
                # Megjelenítés
                display_df = active_with_details[[
                    'first_name', 'last_name', 'check_in_time', 'duration_str'
                ]].copy()
                display_df.columns = ['Vezetéknév', 'Keresztnév', 'Belépés', 'Időtartam']
                
                st.dataframe(display_df, use_container_width=True)
                st.info(f"Összesen {len(active_checkins)} tag van bent.")
            else:
                st.info("Jelenleg üres az edzőterem.")

def show_members():
    """Tagok kezelése"""
    st.header("👥 Tagok kezelése")
    
    tab1, tab2, tab3 = st.tabs(["Tag lista", "Új tag", "Státusz váltás"])
    
    with tab1:
        # Szűrők
        col1, col2, col3 = st.columns(3)
        with col1:
            status_filter = st.selectbox("Státusz", ["Mind", "ACTIVE", "INACTIVE"])
        
        # Tagok lekérése
        if status_filter == "Mind":
            members = supabase_get("members")
        else:
            members = supabase_get("members", filter_params={"status": f"eq.{status_filter}"})
        
        if not members.empty:
            # Tagságok hozzáadása
            memberships = supabase_get("memberships")
            membership_types = supabase_get("membership_types")
            
            # Aktív tagságok meghatározása
            if not memberships.empty and not membership_types.empty:
                current_date = pd.Timestamp.now()
                active_memberships = memberships[
                    (pd.to_datetime(memberships['start_date']) <= current_date) & 
                    (pd.to_datetime(memberships['end_date']) >= current_date)
                ]
                
                members_with_type = members.merge(
                    active_memberships[['member_id', 'type_id']], 
                    on='member_id', 
                    how='left'
                )
                
                members_with_type = members_with_type.merge(
                    membership_types[['type_id', 'type_name']], 
                    on='type_id', 
                    how='left'
                )
                
                members_with_type['membership'] = members_with_type['type_name'].fillna('Nincs aktív')
            else:
                members_with_type = members.copy()
                members_with_type['membership'] = 'Nincs aktív'
            
            # Megjelenítés
            display_columns = ['member_id', 'first_name', 'last_name', 'email', 'status', 'membership', 'join_date']
            display_df = members_with_type[display_columns].copy()
            display_df.columns = ['ID', 'Vezetéknév', 'Keresztnév', 'Email', 'Státusz', 'Tagság', 'Csatlakozás']
            
            st.dataframe(display_df, use_container_width=True)
            
            # Export
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Letöltés CSV-ben", csv, "members.csv", "text/csv")
    
    with tab2:
        st.subheader("Új tag regisztrálása")
        
        with st.form("new_member"):
            col1, col2 = st.columns(2)
            with col1:
                first_name = st.text_input("Keresztnév*")
                last_name = st.text_input("Vezetéknév*")
                email = st.text_input("Email*")
            with col2:
                phone = st.text_input("Telefon")
                birth_date = st.date_input("Születési dátum", 
                    min_value=datetime(1900, 1, 1),
                    max_value=datetime.now() - timedelta(days=365*16))
            
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
                    st.error("Kérjük töltsd ki a kötelező mezőket!")
    
    with tab3:
        st.subheader("Státusz váltás")
        
        members = supabase_get("members")
        if not members.empty:
            # Tag kiválasztása
            member_names = members.apply(lambda x: f"{x['first_name']} {x['last_name']} ({x['status']})", axis=1)
            selected_name = st.selectbox("Válassz tagot", member_names)
            selected_idx = member_names[member_names == selected_name].index[0]
            selected_member = members.iloc[selected_idx]
            
            # Jelenlegi státusz
            current_status = selected_member['status']
            new_status = "INACTIVE" if current_status == "ACTIVE" else "ACTIVE"
            
            st.info(f"Jelenlegi státusz: **{current_status}**")
            
            if st.button(f"Váltás: {new_status}", type="primary"):
                if supabase_update("members", "member_id", selected_member['member_id'], {"status": new_status}):
                    st.success(f"✅ Státusz sikeresen módosítva: {new_status}")
                    time.sleep(1)
                    st.rerun()

def show_finance():
    """Pénzügyi áttekintés"""
    st.header("💰 Pénzügyi áttekintés")
    
    # Időszak választó
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Kezdő dátum", datetime.now().date() - timedelta(days=30))
    with col2:
        end_date = st.date_input("Záró dátum", datetime.now().date())
    
    # Bevételek lekérése
    payments = supabase_get("payments")
    
    if not payments.empty:
        # Szűrés dátumra
        payments['payment_date'] = pd.to_datetime(payments['payment_date'])
        filtered_payments = payments[
            (payments['payment_date'].dt.date >= start_date) & 
            (payments['payment_date'].dt.date <= end_date)
        ]
        
        if not filtered_payments.empty:
            # Összesítés
            total_revenue = filtered_payments['amount'].sum()
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Összes bevétel", f"{total_revenue:,.0f} Ft")
            with col2:
                st.metric("Tranzakciók száma", len(filtered_payments))
            with col3:
                avg_amount = filtered_payments['amount'].mean()
                st.metric("Átlagos összeg", f"{avg_amount:,.0f} Ft")
            
            # Napi bevételek
            st.subheader("Napi bevételek")
            daily_revenue = filtered_payments.groupby(
                filtered_payments['payment_date'].dt.date
            )['amount'].sum().reset_index()
            daily_revenue.columns = ['date', 'revenue']
            
            fig = px.line(daily_revenue, x='date', y='revenue',
                         title="Bevétel alakulása")
            st.plotly_chart(fig, use_container_width=True)
            
            # Bevétel típusonként
            st.subheader("Bevétel típusonként")
            revenue_by_type = filtered_payments.groupby('payment_type')['amount'].sum()
            
            col1, col2 = st.columns([2, 1])
            with col1:
                fig = px.pie(values=revenue_by_type.values, names=revenue_by_type.index,
                           title="Bevétel megoszlás")
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                st.dataframe(revenue_by_type.reset_index(name='összeg'))
            
            # Részletes tranzakciók
            if st.checkbox("Részletes tranzakciók"):
                display_payments = filtered_payments[[
                    'payment_date', 'amount', 'payment_type', 'payment_method', 'status'
                ]].copy()
                display_payments.columns = ['Dátum', 'Összeg', 'Típus', 'Fizetési mód', 'Státusz']
                st.dataframe(display_payments, use_container_width=True)
        else:
            st.info("Nincs tranzakció a megadott időszakban")
    else:
        st.info("Még nincsenek pénzügyi adatok")

def show_etl_admin():
    """ETL adminisztráció"""
    st.header("⚙️ ETL Adminisztráció")
    
    # ETL státusz
    st.subheader("ETL Státusz")
    
    col1, col2, col3 = st.columns(3)
    
    # OLTP táblák
    with col1:
        st.markdown("**OLTP Táblák**")
        members_count = len(supabase_get("members"))
        checkins_count = len(supabase_get("check_ins"))
        memberships_count = len(supabase_get("memberships"))
        
        st.metric("Members", members_count)
        st.metric("Check-ins", checkins_count)
        st.metric("Memberships", memberships_count)
    
    # DWH táblák
    with col2:
        st.markdown("**DWH Táblák**")
        dim_member_count = len(supabase_get("dim_member"))
        fact_visits_count = len(supabase_get("fact_visits"))
        
        st.metric("Dim_member", dim_member_count)
        st.metric("Fact_visits", fact_visits_count)
    
    # Utolsó ETL
    with col3:
        st.markdown("**ETL Info**")
        last_etl = st.session_state.get('last_etl', 'Még nem futott')
        if isinstance(last_etl, datetime):
            st.info(f"Utolsó futás: {last_etl.strftime('%Y-%m-%d %H:%M')}")
        else:
            st.info(last_etl)
    
    # Manuális ETL
    st.subheader("Manuális ETL futtatás")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 Tag dimenzió frissítése", type="primary"):
            count = etl_update_dim_member()
            st.success(f"✅ {count} tag dimenzió frissítve")
            st.session_state['last_etl'] = datetime.now()
    
    with col2:
        if st.button("📊 Látogatás tények frissítése", type="primary"):
            count = etl_update_fact_visits()
            st.success(f"✅ {count} látogatás feldolgozva")
            st.session_state['last_etl'] = datetime.now()
    
    # Teljes ETL
    if st.button("⚡ Teljes ETL futtatása", type="primary"):
        with st.spinner("ETL folyamat fut..."):
            member_count = etl_update_dim_member()
            visit_count = etl_update_fact_visits()
            st.success(f"✅ ETL befejezve: {member_count} tag, {visit_count} látogatás")
            st.session_state['last_etl'] = datetime.now()
    
    # ETL ütemezés
    st.subheader("ETL Automatizáció")
    
    auto_etl = st.checkbox("Automatikus ETL engedélyezése", value=True)
    if auto_etl:
        interval = st.select_slider(
            "ETL futtatási gyakoriság",
            options=[1, 5, 15, 30, 60],
            value=5,
            format_func=lambda x: f"{x} perc"
        )
        st.info(f"Az ETL {interval} percenként fut automatikusan")
    
    # Teszt adatok
    st.subheader("Teszt adatok generálása")
    
    if st.button("🎲 Random belépések generálása"):
        members = supabase_get("members", filter_params={"status": "eq.ACTIVE"})
        if not members.empty:
            # Random check-in generálás
            import random
            for _ in range(10):
                member = members.sample(1).iloc[0]
                check_in_time = datetime.now() - timedelta(
                    hours=random.randint(0, 8),
                    minutes=random.randint(0, 59)
                )
                check_out_time = check_in_time + timedelta(
                    hours=random.randint(1, 3),
                    minutes=random.randint(0, 59)
                )
                
                check_in_data = {
                    "member_id": int(member['member_id']),
                    "check_in_time": check_in_time.isoformat(),
                    "check_out_time": check_out_time.isoformat() if random.random() > 0.3 else None
                }
                supabase_insert("check_ins", check_in_data)
            
            st.success("✅ 10 random belépés generálva")

# Alkalmazás futtatása
if __name__ == "__main__":
    main()
