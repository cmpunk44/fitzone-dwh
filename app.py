# app.py - FitZone valódi ETL-lel
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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

def supabase_delete(table, filter_params):
    """Adatok törlése"""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    if filter_params:
        params = "&".join([f"{k}={v}" for k, v in filter_params.items()])
        url += f"?{params}"
    
    response = requests.delete(url, headers=headers)
    return response.ok

# ETL Funkciók
class FitZoneETL:
    """Valódi ETL folyamatok"""
    
    @staticmethod
    def extract_transform_members():
        """Tagok ETL - dimenzió tábla frissítése"""
        try:
            # Extract - OLTP adatok
            members = supabase_get("members")
            memberships = supabase_get("memberships")
            membership_types = supabase_get("membership_types")
            
            if members.empty:
                st.warning("Nincsenek tagok az OLTP-ben")
                return 0
            
            # Transform
            current_date = pd.Timestamp.now()
            
            # Alapértelmezett értékek minden taghoz
            members_transformed = members.copy()
            members_transformed['type_name'] = 'None'
            
            # Aktív tagságok keresése
            if not memberships.empty:
                memberships['start_date'] = pd.to_datetime(memberships['start_date'])
                memberships['end_date'] = pd.to_datetime(memberships['end_date'])
                
                active_memberships = memberships[
                    (memberships['start_date'] <= current_date) & 
                    (memberships['end_date'] >= current_date)
                ]
                
                # Ha vannak aktív tagságok és tagság típusok
                if not active_memberships.empty and not membership_types.empty:
                    active_with_types = active_memberships.merge(
                        membership_types[['type_id', 'type_name']], 
                        on='type_id',
                        how='left'
                    )
                    
                    # Frissítsük a tagság típusokat ahol van
                    for _, row in active_with_types.iterrows():
                        members_transformed.loc[
                            members_transformed['member_id'] == row['member_id'], 
                            'type_name'
                        ] = row['type_name']
            
            # Életkor csoport számítás
            members_transformed['birth_date'] = pd.to_datetime(members_transformed['birth_date'], errors='coerce')
            members_transformed['age'] = members_transformed['birth_date'].apply(
                lambda x: (current_date - x).days // 365 if pd.notna(x) else None
            )
            
            def get_age_group(age):
                if pd.isna(age):
                    return 'Unknown'
                elif age < 25:
                    return '<25'
                elif age < 35:
                    return '25-35'
                elif age < 45:
                    return '35-45'
                elif age < 55:
                    return '45-55'
                else:
                    return '55+'
            
            members_transformed['age_group'] = members_transformed['age'].apply(get_age_group)
            
            # Tag óta eltelt napok
            members_transformed['join_date'] = pd.to_datetime(members_transformed['join_date'], errors='coerce')
            members_transformed['member_since_days'] = members_transformed['join_date'].apply(
                lambda x: (current_date - x).days if pd.notna(x) else 0
            )
            
            # Load - Dimenzió tábla frissítése
            # Először töröljük a régieket
            supabase_delete("dim_member", {"is_current": "eq.true"})
            
            # Új rekordok beszúrása
            success_count = 0
            for idx, member in members_transformed.iterrows():
                try:
                    dim_member = {
                        "member_id": int(member['member_id']),
                        "first_name": str(member['first_name']),
                        "last_name": str(member['last_name']),
                        "email": str(member['email']),
                        "age_group": str(member['age_group']),
                        "member_since_days": int(member['member_since_days']),
                        "current_membership_type": str(member['type_name']),
                        "member_status": str(member['status']),
                        "is_current": True,
                        "valid_from": current_date.date().isoformat(),
                        "valid_to": "2099-12-31"
                    }
                    
                    if supabase_insert("dim_member", dim_member):
                        success_count += 1
                    else:
                        st.error(f"Nem sikerült beszúrni: {member['email']}")
                        
                except Exception as e:
                    st.error(f"Hiba a tag feldolgozásakor: {e}")
                    continue
            
            return success_count
            
        except Exception as e:
            st.error(f"ETL hiba: {e}")
            return 0
    
    @staticmethod
    def extract_transform_visits(days_back=1):
        """Látogatások ETL - tény tábla frissítése"""
        try:
            # Extract
            check_ins = supabase_get("check_ins")
            dim_members = supabase_get("dim_member", filter_params={"is_current": "eq.true"})
            
            if check_ins.empty:
                st.warning("Nincsenek check-in adatok")
                return 0
                
            if dim_members.empty:
                st.warning("Nincsenek adatok a dim_member táblában. Futtasd először a tag dimenzió ETL-t!")
                return 0
            
            # Transform
            cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days_back)
            check_ins['check_in_time'] = pd.to_datetime(check_ins['check_in_time'])
            
            recent_visits = check_ins[check_ins['check_in_time'] >= cutoff_date]
            
            if recent_visits.empty:
                st.info("Nincs új látogatás a megadott időszakban")
                return 0
            
            # Check melyik fact_visits már létezik
            existing_facts = supabase_get("fact_visits")
            existing_set = set()
            
            if not existing_facts.empty:
                existing_facts['check_in_time'] = pd.to_datetime(existing_facts['check_in_time'])
                existing_set = set(existing_facts['check_in_time'].dt.to_pydatetime())
            
            # Új látogatások
            new_visits = recent_visits[
                ~recent_visits['check_in_time'].dt.to_pydatetime().isin(existing_set)
            ]
            
            if new_visits.empty:
                st.info("Nincs új látogatás feldolgozásra")
                return 0
            
            # Load
            success_count = 0
            for _, visit in new_visits.iterrows():
                try:
                    # Találjuk meg a megfelelő dim_member rekordot
                    member_match = dim_members[dim_members['member_id'] == visit['member_id']]
                    
                    if member_match.empty:
                        st.warning(f"Nem található dim_member a member_id={visit['member_id']} számára")
                        continue
                    
                    check_in_time = visit['check_in_time']
                    date_key = int(check_in_time.strftime('%Y%m%d'))
                    time_key = check_in_time.hour * 100 + check_in_time.minute
                    
                    # Időtartam számítás
                    duration = None
                    if pd.notna(visit.get('check_out_time')):
                        check_out_time = pd.to_datetime(visit['check_out_time'])
                        duration = int((check_out_time - check_in_time).total_seconds() / 60)
                    
                    fact_visit = {
                        "date_key": date_key,
                        "time_key": time_key,
                        "member_key": int(member_match.iloc[0]['member_key']),
                        "check_in_time": check_in_time.isoformat(),
                        "check_out_time": visit.get('check_out_time'),
                        "duration_minutes": duration
                    }
                    
                    if supabase_insert("fact_visits", fact_visit):
                        success_count += 1
                    else:
                        st.error(f"Nem sikerült beszúrni a látogatást: {check_in_time}")
                        
                except Exception as e:
                    st.error(f"Hiba a látogatás feldolgozásakor: {e}")
                    continue
            
            return success_count
            
        except Exception as e:
            st.error(f"ETL hiba: {e}")
            return 0
    
    @staticmethod
    def run_daily_analytics():
        """Napi aggregált elemzések"""
        # Extract
        fact_visits = supabase_get("fact_visits")
        dim_members = supabase_get("dim_member")
        
        if fact_visits.empty:
            return None
        
        # Transform - Mai adatok
        today_key = int(datetime.now().strftime('%Y%m%d'))
        today_visits = fact_visits[fact_visits['date_key'] == today_key]
        
        if today_visits.empty:
            return None
        
        # Csatlakozás a dimenzió táblával
        visits_with_members = today_visits.merge(
            dim_members[['member_key', 'current_membership_type', 'age_group']], 
            on='member_key',
            how='left'
        )
        
        # Aggregációk
        analytics = {
            "date": datetime.now().date().isoformat(),
            "total_visits": len(today_visits),
            "unique_visitors": today_visits['member_key'].nunique(),
            "avg_duration": today_visits['duration_minutes'].mean() if today_visits['duration_minutes'].notna().any() else 0,
            "peak_hour": today_visits.groupby(today_visits['time_key'] // 100)['visit_key'].count().idxmax(),
            "by_membership": visits_with_members['current_membership_type'].value_counts().to_dict(),
            "by_age_group": visits_with_members['age_group'].value_counts().to_dict()
        }
        
        return analytics

# Főalkalmazás
def main():
    st.title("🏋️ FitZone Management System")
    
    # Sidebar
    st.sidebar.title("Navigáció")
    page = st.sidebar.selectbox(
        "Válassz funkciót",
        ["📊 Dashboard", "🚪 Recepció", "👥 Tagok", "📈 Elemzések", "⚙️ ETL Admin"]
    )
    
    if page == "📊 Dashboard":
        show_dashboard()
    elif page == "🚪 Recepció":
        show_reception()
    elif page == "👥 Tagok":
        show_members()
    elif page == "📈 Elemzések":
        show_analytics()
    elif page == "⚙️ ETL Admin":
        show_etl_admin()

def show_dashboard():
    """Főoldal áttekintés"""
    st.header("Dashboard")
    
    # ETL futtatás gomb
    if st.button("🔄 Adatok frissítése", help="ETL folyamat futtatása"):
        with st.spinner("ETL folyamat fut..."):
            member_count = FitZoneETL.extract_transform_members()
            st.info(f"Tag dimenzió frissítve: {member_count} rekord")
            
            visit_count = FitZoneETL.extract_transform_visits(days_back=7)
            st.info(f"Látogatás tények frissítve: {visit_count} rekord")
            
            if member_count > 0 or visit_count > 0:
                st.success(f"✅ ETL befejezve!")
            else:
                st.warning("Nem volt feldolgozható adat")
    
    # KPI-k
    col1, col2, col3, col4 = st.columns(4)
    
    # Aktuális adatok
    check_ins = supabase_get("check_ins")
    members = supabase_get("members")
    
    with col1:
        if not check_ins.empty:
            active_now = check_ins[pd.isna(check_ins['check_out_time'])]
            st.metric("🏃 Most bent", len(active_now))
        else:
            st.metric("🏃 Most bent", 0)
    
    with col2:
        if not members.empty:
            active_members = members[members['status'] == 'ACTIVE']
            st.metric("👥 Aktív tagok", len(active_members))
        else:
            st.metric("👥 Aktív tagok", 0)
    
    with col3:
        if not check_ins.empty:
            today = pd.Timestamp.now().date()
            check_ins['date'] = pd.to_datetime(check_ins['check_in_time']).dt.date
            today_visits = check_ins[check_ins['date'] == today]
            st.metric("📅 Mai látogatók", today_visits['member_id'].nunique())
        else:
            st.metric("📅 Mai látogatók", 0)
    
    with col4:
        dim_members = supabase_get("dim_member")
        if not dim_members.empty:
            st.metric("🎯 DWH rekordok", len(dim_members))
        else:
            st.metric("🎯 DWH rekordok", 0)
    
    # Grafikonok csak ha van adat
    analytics = FitZoneETL.run_daily_analytics()
    if analytics and analytics.get('total_visits', 0) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            # Tagság típus megoszlás
            if analytics.get('by_membership'):
                membership_data = pd.DataFrame(
                    list(analytics['by_membership'].items()),
                    columns=['Tagság', 'Látogatók']
                )
                fig1 = px.pie(membership_data, values='Látogatók', names='Tagság',
                             title="Mai látogatók tagság szerint")
                st.plotly_chart(fig1, use_container_width=True)
            else:
                st.info("Nincs adat a tagság megoszláshoz")
        
        with col2:
            # Kor csoport megoszlás
            if analytics.get('by_age_group'):
                age_data = pd.DataFrame(
                    list(analytics['by_age_group'].items()),
                    columns=['Korcsoport', 'Látogatók']
                )
                fig2 = px.bar(age_data, x='Korcsoport', y='Látogatók',
                             title="Mai látogatók korcsoport szerint")
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info("Nincs adat a korcsoport megoszláshoz")
    else:
        st.info("Nincs még adat a grafikonokhoz. Futtasd az ETL-t!")
    
    # Debug info
    with st.expander("Debug információ"):
        st.write("OLTP táblák:")
        st.write(f"- members: {len(members)} rekord")
        st.write(f"- check_ins: {len(check_ins)} rekord")
        
        st.write("\nDWH táblák:")
        st.write(f"- dim_member: {len(dim_members)} rekord")
        fact_visits = supabase_get("fact_visits")
        st.write(f"- fact_visits: {len(fact_visits)} rekord")

def show_reception():
    """Recepció funkciók"""
    st.header("🚪 Recepció")
    
    tab1, tab2 = st.tabs(["Be/Kiléptetés", "Aktuális státusz"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Beléptetés")
            
            members = supabase_get("members", filter_params={"status": "eq.ACTIVE"})
            if not members.empty:
                search = st.text_input("🔍 Keresés", placeholder="Név vagy email")
                
                if search:
                    mask = (
                        members['first_name'].str.contains(search, case=False, na=False) |
                        members['last_name'].str.contains(search, case=False, na=False) |
                        members['email'].str.contains(search, case=False, na=False)
                    )
                    filtered = members[mask]
                else:
                    filtered = members.head(5)
                
                for _, member in filtered.iterrows():
                    st.write(f"**{member['first_name']} {member['last_name']}**")
                    st.caption(member['email'])
                    
                    if st.button(f"✅ Beléptet", key=f"in_{member['member_id']}"):
                        check_in_data = {
                            "member_id": int(member['member_id']),
                            "check_in_time": datetime.now().isoformat()
                        }
                        if supabase_insert("check_ins", check_in_data):
                            st.success("Beléptetés sikeres!")
                            time.sleep(1)
                            st.rerun()
                    st.divider()
        
        with col2:
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
                        check_in_time = pd.to_datetime(checkin['check_in_time'])
                        duration = datetime.now() - check_in_time
                        hours = int(duration.total_seconds() // 3600)
                        minutes = int((duration.total_seconds() % 3600) // 60)
                        
                        st.write(f"**{checkin['first_name']} {checkin['last_name']}**")
                        st.caption(f"Belépve: {hours}ó {minutes}p")
                        
                        if st.button(f"🚪 Kiléptet", key=f"out_{checkin['checkin_id']}"):
                            update_data = {"check_out_time": datetime.now().isoformat()}
                            if supabase_update("check_ins", "checkin_id", 
                                             checkin['checkin_id'], update_data):
                                st.success("Kiléptetés sikeres!")
                                time.sleep(1)
                                st.rerun()
                        st.divider()
                else:
                    st.info("Nincs bent látogató")
    
    with tab2:
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

def show_analytics():
    """Elemzések az adattárházból"""
    st.header("📈 Elemzések")
    
    # Adattárház adatok
    fact_visits = supabase_get("fact_visits")
    dim_members = supabase_get("dim_member")
    
    if fact_visits.empty or dim_members.empty:
        st.warning("Nincs elég adat az elemzéshez. Futtasd az ETL-t!")
        return
    
    # Dátum tartomány
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Kezdő dátum", datetime.now().date() - timedelta(days=7))
    with col2:
        end_date = st.date_input("Záró dátum", datetime.now().date())
    
    # Szűrés
    start_key = int(start_date.strftime('%Y%m%d'))
    end_key = int(end_date.strftime('%Y%m%d'))
    
    filtered_visits = fact_visits[
        (fact_visits['date_key'] >= start_key) & 
        (fact_visits['date_key'] <= end_key)
    ]
    
    # Látogatások trendje
    st.subheader("Látogatási trend")
    daily_visits = filtered_visits.groupby('date_key').agg({
        'visit_key': 'count',
        'member_key': 'nunique',
        'duration_minutes': 'mean'
    }).reset_index()
    
    daily_visits['date'] = pd.to_datetime(daily_visits['date_key'].astype(str), format='%Y%m%d')
    
    fig = px.line(daily_visits, x='date', y='visit_key',
                  title="Napi látogatások száma",
                  labels={'visit_key': 'Látogatások', 'date': 'Dátum'})
    st.plotly_chart(fig, use_container_width=True)
    
    # Óránkénti eloszlás
    st.subheader("Óránkénti kihasználtság")
    hourly = filtered_visits.groupby(filtered_visits['time_key'] // 100)['visit_key'].count()
    
    fig = px.bar(x=hourly.index, y=hourly.values,
                 title="Látogatások eloszlása óránként",
                 labels={'x': 'Óra', 'y': 'Látogatások'})
    st.plotly_chart(fig, use_container_width=True)
    
    # Tagság típus elemzés
    st.subheader("Tagság típusok aktivitása")
    visits_with_dim = filtered_visits.merge(
        dim_members[['member_key', 'current_membership_type']], 
        on='member_key',
        how='left'
    )
    
    membership_activity = visits_with_dim.groupby('current_membership_type').agg({
        'visit_key': 'count',
        'member_key': 'nunique',
        'duration_minutes': 'mean'
    }).reset_index()
    
    col1, col2 = st.columns(2)
    with col1:
        fig = px.pie(membership_activity, values='visit_key', names='current_membership_type',
                    title="Látogatások megoszlása tagság szerint")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.bar(membership_activity, x='current_membership_type', y='duration_minutes',
                    title="Átlagos tartózkodási idő tagság szerint",
                    labels={'duration_minutes': 'Perc'})
        st.plotly_chart(fig, use_container_width=True)

def show_etl_admin():
    """ETL adminisztráció"""
    st.header("⚙️ ETL Adminisztráció")
    
    # ETL státusz
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("OLTP Adatok")
        members = supabase_get("members")
        check_ins = supabase_get("check_ins")
        memberships = supabase_get("memberships")
        
        st.metric("Tagok", len(members))
        st.metric("Belépések", len(check_ins))
        st.metric("Tagságok", len(memberships))
    
    with col2:
        st.subheader("DWH Adatok")
        dim_members = supabase_get("dim_member")
        fact_visits = supabase_get("fact_visits")
        
        st.metric("Dim Member", len(dim_members))
        st.metric("Fact Visits", len(fact_visits))
    
    with col3:
        st.subheader("ETL Info")
        if 'last_etl' in st.session_state:
            st.info(f"Utolsó ETL: {st.session_state['last_etl']}")
        else:
            st.info("Még nem futott ETL")
    
    # ETL műveletek
    st.divider()
    st.subheader("ETL Műveletek")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔄 Tag dimenzió frissítése", use_container_width=True):
            with st.spinner("ETL fut..."):
                count = FitZoneETL.extract_transform_members()
                st.success(f"✅ {count} rekord feldolgozva")
                st.session_state['last_etl'] = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    with col2:
        days = st.number_input("Napok száma", min_value=1, max_value=30, value=7)
        if st.button("📊 Látogatások betöltése", use_container_width=True):
            with st.spinner("ETL fut..."):
                count = FitZoneETL.extract_transform_visits(days_back=days)
                st.success(f"✅ {count} látogatás betöltve")
                st.session_state['last_etl'] = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    with col3:
        if st.button("🚀 Teljes ETL", use_container_width=True):
            with st.spinner("Teljes ETL fut..."):
                member_count = FitZoneETL.extract_transform_members()
                visit_count = FitZoneETL.extract_transform_visits(days_back=30)
                st.success(f"✅ Kész: {member_count} tag, {visit_count} látogatás")
                st.session_state['last_etl'] = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # Analytics
    st.divider()
    if st.button("📈 Napi elemzés futtatása"):
        analytics = FitZoneETL.run_daily_analytics()
        if analytics:
            st.json(analytics)
        else:
            st.warning("Nincs adat a mai napra")

if __name__ == "__main__":
    main()
