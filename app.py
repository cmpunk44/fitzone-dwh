# app.py - FitZone Gyakorlati Alkalmazás
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import json
from datetime import datetime, timedelta
import time

st.set_page_config(
    page_title="FitZone Recepcio",
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

# Üzleti logika
def get_current_visitors():
    """Jelenleg bent lévők száma"""
    check_ins = supabase_get("check_ins")
    if check_ins.empty:
        return 0
    
    active = check_ins[pd.isna(check_ins['check_out_time'])]
    return len(active)

def check_membership_validity(member_id):
    """Tagság érvényességének ellenőrzése"""
    memberships = supabase_get("memberships", filter_params={"member_id": f"eq.{member_id}"})
    
    if memberships.empty:
        return False, "Nincs tagság"
    
    current_date = pd.Timestamp.now().date()
    for _, membership in memberships.iterrows():
        try:
            start_date = pd.to_datetime(membership['start_date']).date()
            end_date = pd.to_datetime(membership['end_date']).date()
            
            if start_date <= current_date <= end_date:
                return True, f"Érvényes ({end_date})"
        except:
            continue
    
    return False, "Lejárt tagság"

def calculate_daily_summary():
    """Napi összesítő a recepciónak"""
    summary = {}
    
    # Mai látogatók
    check_ins = supabase_get("check_ins")
    if not check_ins.empty:
        today = pd.Timestamp.now().date()
        check_ins['date'] = pd.to_datetime(check_ins['check_in_time']).dt.date
        today_visits = check_ins[check_ins['date'] == today]
        
        summary['total_visits'] = len(today_visits)
        summary['unique_visitors'] = today_visits['member_id'].nunique()
        summary['current_inside'] = len(today_visits[pd.isna(today_visits['check_out_time'])])
    else:
        summary['total_visits'] = 0
        summary['unique_visitors'] = 0
        summary['current_inside'] = 0
    
    # Lejáró tagságok
    memberships = supabase_get("memberships")
    if not memberships.empty:
        next_week = pd.Timestamp.now().date() + timedelta(days=7)
        memberships['end_date'] = pd.to_datetime(memberships['end_date']).dt.date
        expiring = memberships[
            (memberships['end_date'] <= next_week) & 
            (memberships['end_date'] >= pd.Timestamp.now().date())
        ]
        summary['expiring_memberships'] = len(expiring)
    else:
        summary['expiring_memberships'] = 0
    
    return summary

def main():
    st.title("🏋️ FitZone Recepció")
    
    # Napi összesítő header
    summary = calculate_daily_summary()
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("🏃 Most bent", summary['current_inside'], 
                 delta=f"/{summary['total_visits']} ma")
    with col2:
        st.metric("👥 Mai látogatók", summary['unique_visitors'])
    with col3:
        st.metric("⚠️ Lejáró tagságok", summary['expiring_memberships'],
                 help="Következő 7 napban")
    with col4:
        current_hour = datetime.now().hour
        if 6 <= current_hour <= 9 or 17 <= current_hour <= 20:
            st.metric("⏰ Időszak", "CSÚCSIDŐ", delta="Több személyzet kell")
        else:
            st.metric("⏰ Időszak", "Normál")
    
    # Fő funkciók
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🚪 Be/Kiléptetés", 
        "👤 Új Tag", 
        "💳 Tagság Kezelés",
        "📊 Napi Jelentés",
        "⚙️ Adminisztráció"
    ])
    
    with tab1:
        show_check_in_out()
    
    with tab2:
        show_new_member()
    
    with tab3:
        show_membership_management()
    
    with tab4:
        show_daily_report()
    
    with tab5:
        show_admin()

def show_check_in_out():
    """Be- és kiléptetés"""
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🟢 Beléptetés")
        
        # Aktív tagok
        members = supabase_get("members", filter_params={"status": "eq.ACTIVE"})
        if not members.empty:
            # Keresés
            search = st.text_input("Keresés (név vagy email)")
            
            if search:
                mask = (
                    members['first_name'].str.contains(search, case=False, na=False) |
                    members['last_name'].str.contains(search, case=False, na=False) |
                    members['email'].str.contains(search, case=False, na=False)
                )
                filtered_members = members[mask]
            else:
                filtered_members = members.head(10)  # Első 10 tag
            
            if not filtered_members.empty:
                # Tagok listája
                for _, member in filtered_members.iterrows():
                    col_a, col_b, col_c = st.columns([3, 2, 1])
                    
                    with col_a:
                        st.write(f"**{member['first_name']} {member['last_name']}**")
                        st.caption(member['email'])
                    
                    with col_b:
                        valid, status = check_membership_validity(member['member_id'])
                        if valid:
                            st.success(status)
                        else:
                            st.error(status)
                    
                    with col_c:
                        if valid:
                            if st.button("Beléptet", key=f"in_{member['member_id']}"):
                                check_in_data = {
                                    "member_id": int(member['member_id']),
                                    "check_in_time": datetime.now().isoformat()
                                }
                                if supabase_insert("check_ins", check_in_data):
                                    st.success("✅")
                                    time.sleep(1)
                                    st.rerun()
                        else:
                            st.button("❌", key=f"invalid_{member['member_id']}", disabled=True)
                    
                    st.divider()
    
    with col2:
        st.subheader("🔴 Kiléptetés")
        
        # Bent lévők
        check_ins = supabase_get("check_ins")
        if not check_ins.empty:
            active_checkins = check_ins[pd.isna(check_ins['check_out_time'])]
            
            if not active_checkins.empty:
                members = supabase_get("members")
                active_with_names = active_checkins.merge(
                    members[['member_id', 'first_name', 'last_name']], 
                    on='member_id',
                    how='left'
                )
                
                # Lista
                for _, checkin in active_with_names.iterrows():
                    col_a, col_b, col_c = st.columns([3, 2, 1])
                    
                    with col_a:
                        st.write(f"**{checkin['first_name']} {checkin['last_name']}**")
                        check_in_time = pd.to_datetime(checkin['check_in_time'])
                        duration = datetime.now() - check_in_time
                        hours = int(duration.total_seconds() // 3600)
                        minutes = int((duration.total_seconds() % 3600) // 60)
                        st.caption(f"Belépve: {check_in_time.strftime('%H:%M')} ({hours}ó {minutes}p)")
                    
                    with col_b:
                        if hours >= 3:
                            st.warning("Régóta bent")
                    
                    with col_c:
                        if st.button("Kiléptet", key=f"out_{checkin['checkin_id']}"):
                            update_data = {"check_out_time": datetime.now().isoformat()}
                            if supabase_update("check_ins", "checkin_id", 
                                             checkin['checkin_id'], update_data):
                                st.success("✅")
                                time.sleep(1)
                                st.rerun()
                    
                    st.divider()
            else:
                st.info("Jelenleg nincs bent senki")

def show_new_member():
    """Új tag regisztráció"""
    st.subheader("Új tag regisztrálása")
    
    with st.form("new_member_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            first_name = st.text_input("Keresztnév*")
            last_name = st.text_input("Vezetéknév*")
            email = st.text_input("Email*")
            phone = st.text_input("Telefon")
        
        with col2:
            birth_date = st.date_input("Születési dátum")
            
            # Tagság típus
            membership_types = supabase_get("membership_types")
            if not membership_types.empty:
                type_options = {
                    f"{t['type_name']} ({t['price']} Ft/{t['duration_months']} hó)": t['type_id']
                    for _, t in membership_types.iterrows()
                }
                selected_type = st.selectbox("Tagság típus*", list(type_options.keys()))
                
            start_date = st.date_input("Tagság kezdete", datetime.now().date())
        
        submitted = st.form_submit_button("Regisztráció és tagság aktiválás")
        
        if submitted:
            if not (first_name and last_name and email):
                st.error("Kérjük töltse ki a kötelező mezőket!")
            else:
                # 1. Tag létrehozása
                new_member = {
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": email,
                    "phone": phone,
                    "birth_date": birth_date.isoformat() if birth_date else None,
                    "status": "ACTIVE"
                }
                
                if supabase_insert("members", new_member):
                    # 2. Megkeressük az új tag ID-ját
                    created_member = supabase_get("members", 
                        filter_params={"email": f"eq.{email}"})
                    
                    if not created_member.empty:
                        member_id = created_member.iloc[0]['member_id']
                        
                        # 3. Tagság létrehozása
                        if selected_type and type_options:
                            type_id = type_options[selected_type]
                            membership_type = membership_types[
                                membership_types['type_id'] == type_id
                            ].iloc[0]
                            
                            duration_months = int(membership_type['duration_months'])
                            end_date = start_date + timedelta(days=30 * duration_months)
                            
                            new_membership = {
                                "member_id": int(member_id),
                                "type_id": int(type_id),
                                "start_date": start_date.isoformat(),
                                "end_date": end_date.isoformat(),
                                "payment_status": "PENDING"
                            }
                            
                            if supabase_insert("memberships", new_membership):
                                st.success(f"✅ Tag és tagság sikeresen létrehozva! ID: {member_id}")
                                st.info(f"Tagság érvényes: {start_date} - {end_date}")
                            else:
                                st.error("Hiba a tagság létrehozásakor")
                else:
                    st.error("Hiba a tag létrehozásakor")

def show_membership_management():
    """Tagság kezelés"""
    st.subheader("Tagság kezelés")
    
    tab1, tab2, tab3 = st.tabs(["Megújítás", "Státusz váltás", "Tagság történet"])
    
    with tab1:
        st.markdown("### Tagság megújítása")
        
        # Lejáró tagságok
        memberships = supabase_get("memberships")
        members = supabase_get("members")
        membership_types = supabase_get("membership_types")
        
        if not memberships.empty and not members.empty:
            # Lejáró/lejárt tagságok
            today = pd.Timestamp.now().date()
            next_month = today + timedelta(days=30)
            
            memberships['end_date'] = pd.to_datetime(memberships['end_date']).dt.date
            expiring = memberships[
                (memberships['end_date'] <= next_month) & 
                (memberships['end_date'] >= today - timedelta(days=7))  # Már lejártak is
            ]
            
            if not expiring.empty:
                expiring_with_details = expiring.merge(
                    members[['member_id', 'first_name', 'last_name', 'email']], 
                    on='member_id'
                ).merge(
                    membership_types[['type_id', 'type_name', 'price']], 
                    on='type_id'
                )
                
                for _, membership in expiring_with_details.iterrows():
                    col1, col2, col3 = st.columns([3, 2, 2])
                    
                    with col1:
                        st.write(f"**{membership['first_name']} {membership['last_name']}**")
                        st.caption(f"{membership['type_name']} - Lejár: {membership['end_date']}")
                    
                    with col2:
                        if membership['end_date'] < today:
                            st.error("Lejárt")
                        elif membership['end_date'] <= today + timedelta(days=7):
                            st.warning("Hamarosan lejár")
                        else:
                            st.info("Lejár 30 napon belül")
                    
                    with col3:
                        if st.button("Megújít", key=f"renew_{membership['membership_id']}"):
                            # Új tagság létrehozása
                            new_start = membership['end_date'] + timedelta(days=1)
                            new_end = new_start + timedelta(days=30 * membership['duration_months'])
                            
                            new_membership = {
                                "member_id": int(membership['member_id']),
                                "type_id": int(membership['type_id']),
                                "start_date": new_start.isoformat(),
                                "end_date": new_end.isoformat(),
                                "payment_status": "PENDING"
                            }
                            
                            if supabase_insert("memberships", new_membership):
                                st.success("✅ Megújítva")
                                time.sleep(1)
                                st.rerun()
                    
                    st.divider()
            else:
                st.info("Nincs lejáró tagság")
    
    with tab2:
        st.markdown("### Státusz váltás")
        
        members = supabase_get("members")
        if not members.empty:
            search = st.text_input("Tag keresése")
            
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
                col1, col2, col3 = st.columns([3, 2, 2])
                
                with col1:
                    st.write(f"**{member['first_name']} {member['last_name']}**")
                    st.caption(member['email'])
                
                with col2:
                    if member['status'] == 'ACTIVE':
                        st.success("Aktív")
                    else:
                        st.error("Inaktív")
                
                with col3:
                    new_status = "INACTIVE" if member['status'] == "ACTIVE" else "ACTIVE"
                    button_text = "Inaktiválás" if member['status'] == "ACTIVE" else "Aktiválás"
                    
                    if st.button(button_text, key=f"status_{member['member_id']}"):
                        if supabase_update("members", "member_id", 
                                         member['member_id'], {"status": new_status}):
                            st.success("✅")
                            time.sleep(1)
                            st.rerun()
                
                st.divider()
    
    with tab3:
        st.markdown("### Tagság történet")
        
        # Tag kiválasztása
        members = supabase_get("members")
        if not members.empty:
            member_names = {
                f"{m['first_name']} {m['last_name']} ({m['email']})": m['member_id']
                for _, m in members.iterrows()
            }
            
            selected = st.selectbox("Válassz tagot", list(member_names.keys()))
            
            if selected:
                member_id = member_names[selected]
                memberships = supabase_get("memberships", 
                    filter_params={"member_id": f"eq.{member_id}"})
                
                if not memberships.empty:
                    membership_types = supabase_get("membership_types")
                    history = memberships.merge(
                        membership_types[['type_id', 'type_name', 'price']], 
                        on='type_id',
                        how='left'
                    )
                    
                    history = history.sort_values('start_date', ascending=False)
                    
                    for _, record in history.iterrows():
                        col1, col2, col3 = st.columns([2, 2, 1])
                        
                        with col1:
                            st.write(f"**{record['type_name']}**")
                            st.caption(f"{record['start_date']} - {record['end_date']}")
                        
                        with col2:
                            st.write(f"💰 {record['price']} Ft")
                            
                        with col3:
                            end_date = pd.to_datetime(record['end_date']).date()
                            if end_date >= datetime.now().date():
                                st.success("Aktív")
                            else:
                                st.error("Lejárt")
                        
                        st.divider()
                else:
                    st.info("Nincs tagság történet")

def show_daily_report():
    """Napi jelentés"""
    st.subheader("📊 Napi jelentés")
    
    # Dátumválasztó
    report_date = st.date_input("Jelentés dátuma", datetime.now().date())
    
    # Adatok lekérése
    check_ins = supabase_get("check_ins")
    members = supabase_get("members")
    
    if not check_ins.empty:
        # Szűrés a kiválasztott napra
        check_ins['date'] = pd.to_datetime(check_ins['check_in_time']).dt.date
        daily_data = check_ins[check_ins['date'] == report_date]
        
        if not daily_data.empty:
            # Alap statisztikák
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Összes belépés", len(daily_data))
            with col2:
                st.metric("Egyedi látogatók", daily_data['member_id'].nunique())
            with col3:
                avg_duration = daily_data[pd.notna(daily_data['check_out_time'])].apply(
                    lambda x: (pd.to_datetime(x['check_out_time']) - 
                              pd.to_datetime(x['check_in_time'])).total_seconds() / 60,
                    axis=1
                ).mean()
                st.metric("Átl. tartózkodás", f"{avg_duration:.0f} perc" if pd.notna(avg_duration) else "N/A")
            
            # Óránkénti eloszlás
            st.subheader("Óránkénti látogatások")
            hourly = daily_data.copy()
            hourly['hour'] = pd.to_datetime(hourly['check_in_time']).dt.hour
            hourly_count = hourly.groupby('hour').size().reset_index(name='count')
            
            fig = px.bar(hourly_count, x='hour', y='count',
                        title=f"Látogatások eloszlása - {report_date}")
            st.plotly_chart(fig, use_container_width=True)
            
            # Részletes lista
            if st.checkbox("Részletes látogatási lista"):
                detailed = daily_data.merge(
                    members[['member_id', 'first_name', 'last_name']], 
                    on='member_id'
                )
                
                detailed['duration'] = detailed.apply(
                    lambda x: (pd.to_datetime(x['check_out_time']) - 
                              pd.to_datetime(x['check_in_time'])).total_seconds() / 60
                    if pd.notna(x['check_out_time']) else None,
                    axis=1
                )
                
                display_df = detailed[[
                    'first_name', 'last_name', 'check_in_time', 
                    'check_out_time', 'duration'
                ]].copy()
                
                display_df.columns = ['Keresztnév', 'Vezetéknév', 'Belépés', 'Kilépés', 'Időtartam (perc)']
                st.dataframe(display_df, use_container_width=True)
        else:
            st.info(f"Nincs adat {report_date} napra")
    
    # ETL ajánlás
    st.divider()
    st.subheader("🤖 Automatikus elemzések")
    
    if st.button("Napi elemzés futtatása"):
        with st.spinner("Elemzés..."):
            # Egyszerű ETL - napi összesítő
            if not check_ins.empty:
                today_data = check_ins[check_ins['date'] == datetime.now().date()]
                
                summary = {
                    "date": datetime.now().date().isoformat(),
                    "total_visits": len(today_data),
                    "unique_visitors": today_data['member_id'].nunique(),
                    "peak_hour": today_data['hour'].mode().iloc[0] if not today_data.empty else None
                }
                
                # Itt lehetne menteni a summary-t egy fact táblába
                st.json(summary)
                st.success("✅ Elemzés kész")

def show_admin():
    """Admin funkciók"""
    st.subheader("⚙️ Adminisztráció")
    
    tab1, tab2 = st.tabs(["Rendszer állapot", "Karbantartás"])
    
    with tab1:
        st.markdown("### Rendszer állapot")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            members = supabase_get("members")
            st.metric("Összes tag", len(members))
            st.metric("Aktív tagok", len(members[members['status'] == 'ACTIVE']))
        
        with col2:
            memberships = supabase_get("memberships")
            if not memberships.empty:
                active_memberships = memberships[
                    pd.to_datetime(memberships['end_date']) >= datetime.now()
                ]
                st.metric("Aktív tagságok", len(active_memberships))
        
        with col3:
            check_ins = supabase_get("check_ins")
            if not check_ins.empty:
                today_visits = check_ins[
                    pd.to_datetime(check_ins['check_in_time']).dt.date == datetime.now().date()
                ]
                st.metric("Mai látogatások", len(today_visits))
    
    with tab2:
        st.markdown("### Karbantartás")
        
        # Teszt adatok
        if st.button("🎲 Teszt adatok generálása"):
            # Néhány teszt tag
            test_members = [
                {"first_name": "Teszt", "last_name": "Elek", "email": f"teszt{datetime.now().timestamp()}@test.com", "status": "ACTIVE"},
                {"first_name": "Próba", "last_name": "Béla", "email": f"proba{datetime.now().timestamp()}@test.com", "status": "ACTIVE"}
            ]
            
            for member in test_members:
                supabase_insert("members", member)
            
            st.success("✅ Teszt adatok létrehozva")
        
        # Tisztítás
        if st.button("🧹 Régi adatok tisztítása"):
            st.info("Funkció fejlesztés alatt...")

if __name__ == "__main__":
    main()
