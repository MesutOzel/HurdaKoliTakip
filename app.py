import streamlit as st
import pandas as pd
import plotly.express as px
from passlib.hash import pbkdf2_sha256
from datetime import datetime, timedelta, date
from io import BytesIO

from reportlab.lib.pagesizes import A6
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm

from db import (
    init_db, get_conn, ensure_default_users,
    last_year_total_for, totals, group_totals_by
)

APP_TITLE = "LC Waikiki - Hurda Koli Takip Sistemi"
PRIMARY_BLUE = "#1E50FF"
PRIMARY_RED  = "#E11D2E"
ACCENT_YELLOW = "#FFD54F"
BROWN = "#4b3b2b"

MAX_ONCE = 15
MAX_YEAR = 45

# Kullanıcı adı -> Vardiya Amiri adı eşlemesi
USERNAME_TO_LEADER = {
    "mesut.ozel": "Mesut Özel",
    "serhan.atilla": "Serhan Atilla",
    "erdal.adiguzel.bicer": "Erdal Adıgüzel Biçer",
    "levent.sengul": "Levent Şengül",
    "firat.kullu": "Fırat Küllü",
    "ozkan.kilic": "Özkan Kılıç",
    "busra.cici": "Büşra Cici",
    "cahit.altun": "Cahit Altun",
    "emrah.dubaz": "Emrah Dubaz",
    "halit.kaya": "Halit Kaya",
    "senol.ogras": "Şenol Oğraş",
    "baris.orhan": "Barış Orhan",
    "yusuf.sayan": "Yusuf Sayan",
}

CUSTOM_CSS = f"""
<style>
.stApp {{
  background: linear-gradient(180deg, {ACCENT_YELLOW} 0%, #fdecec 30%, #f6f8ff 100%) !important;
}}
.header {{
  background: linear-gradient(90deg, {PRIMARY_BLUE} 0%, {PRIMARY_RED} 50%, {ACCENT_YELLOW} 100%);
  color: white; padding: 18px 20px; border-radius: 16px;
  display:flex; align-items:center; gap:14px;
}}
.header .logo {{
  width:56px;height:56px;border-radius:50%;
  background:white; color:{PRIMARY_RED}; font-weight:800;
  display:flex;align-items:center;justify-content:center;
}}
section[data-testid="stSidebar"] > div {{
  background: linear-gradient(180deg, {PRIMARY_BLUE} 0%, {PRIMARY_RED} 60%, {ACCENT_YELLOW} 100%);
}}
section[data-testid="stSidebar"] * {{ color: #fff !important; }}
.stButton>button, .stDownloadButton>button {{
  background: linear-gradient(90deg, {PRIMARY_RED}, {PRIMARY_BLUE});
  color: #fff; border: 0; border-radius: 10px; padding: 10px 14px; font-weight: 700;
}}
.metric-card {{
  background: linear-gradient(90deg, {PRIMARY_BLUE} 0%, {PRIMARY_RED} 100%);
  color:#fff; padding:14px; border-radius:12px; text-align:center;
}}
.info-card {{
  background:{ACCENT_YELLOW}; border:1px solid #e9e9ef; border-radius:12px; padding:12px; color:{BROWN};
}}
.info-card .big {{ font-size:22px; font-weight:800; }}

/* Personeller & tablolar — yatay scroll garantisi */
div[data-testid="stDataEditor"] div[role="grid"] {{ overflow:auto !important; }}
</style>
"""

# ---------- yardımcılar
def hash_it(s: str) -> str:
    return pbkdf2_sha256.hash(s)

def verify_it(pw: str, hashed: str) -> bool:
    return pbkdf2_sha256.verify(pw, hashed)

def normalize_date(d) -> date:
    if isinstance(d, tuple) and len(d) >= 1:
        return d[0]
    return d

def authenticate(username, password, role_choice):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE username=?", (username,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return False, "Kullanıcı bulunamadı."
    if row["role"] != role_choice:
        return False, "Rol uyuşmuyor. Doğru giriş tipini seçin."
    if not verify_it(password, row["password_hash"]):
        return False, "Şifre hatalı."
    return True, {"id": row["id"], "username": row["username"], "role": row["role"]}

def register_user(username, password, role_choice):
    if len(username) < 3 or len(password) < 6:
        return False, "Kullanıcı adı ≥3, şifre ≥6 karakter olmalı."
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("INSERT INTO users(username, password_hash, role) VALUES(?,?,?)",
                    (username, hash_it(password), role_choice))
        conn.commit()
        conn.close()
        return True, "Kayıt başarılı."
    except Exception as e:
        return False, f"Kayıt alınamadı: {e}"

def reset_password(username):
    temp = "Sifirla_" + datetime.now().strftime("%H%M%S")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE username=?", (username,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return False, "Kullanıcı bulunamadı."
    cur.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_it(temp), row["id"]))
    conn.commit(); conn.close()
    return True, f"Geçici şifre: {temp}"

def upsert_person_minimal(harmony_ref, vardiya_amiri, depo):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO personnel(harmony_ref, adi, soyadi, ad_soyad, vardiya_amiri, depo)
        VALUES(?,?,?,?,?,?)
    """, (harmony_ref, "", "", "", vardiya_amiri, depo))
    if cur.rowcount == 0:
        cur.execute("UPDATE personnel SET vardiya_amiri=?, depo=? WHERE harmony_ref=?",
                    (vardiya_amiri, depo, harmony_ref))
    conn.commit(); conn.close()

def record_scrap(harmony_ref, koli_sayisi, vardiya_amiri, depo, form_serial):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO scrap_records(harmony_ref, koli_sayisi, vardiya_amiri, depo, form_serial)
        VALUES (?,?,?,?,?)
    """,(harmony_ref, int(koli_sayisi), vardiya_amiri, depo, form_serial))
    last_id = cur.lastrowid
    conn.commit()
    conn.close()
    return last_id

def monthly_total_for(harmony_ref: str) -> int:
    conn = get_conn()
    cur = conn.cursor()
    start = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    cur.execute("""
        SELECT COALESCE(SUM(koli_sayisi),0) AS total
        FROM scrap_records
        WHERE harmony_ref=? AND created_at >= ? AND strftime('%Y-%m', created_at)=?
    """, (harmony_ref, start.strftime("%Y-%m-%d %H:%M:%S"), datetime.now().strftime("%Y-%m")))
    row = cur.fetchone()
    conn.close()
    return int(row["total"] if row and row["total"] is not None else 0)

def yearly_total_excluding(harmony_ref: str, exclude_id: int) -> int:
    conn = get_conn()
    cur = conn.cursor()
    since = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("""
        SELECT COALESCE(SUM(koli_sayisi),0) AS total
        FROM scrap_records
        WHERE harmony_ref=? AND created_at >= ? AND id != ?
    """, (harmony_ref, since, exclude_id))
    row = cur.fetchone()
    conn.close()
    return int(row["total"] if row and row["total"] is not None else 0)

def make_receipt_pdf(record: dict) -> bytes:
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A6)
    width, height = A6
    margin = 8 * mm
    y = height - margin

    c.setFillColorRGB(0.12, 0.35, 1.0)
    c.rect(0, height-18*mm, width, 18*mm, fill=1, stroke=0)
    c.setFillColorRGB(1,1,1)
    c.setFont("Helvetica-Bold", 14)
    c.drawString(margin, height-12*mm, "LC Waikiki - Hurda Koli Fişi")

    y -= 10 * mm
    c.setFillColorRGB(0, 0, 0)
    c.setFont("Helvetica", 10)
    lines = [
        ("Form Seri No", record["form_serial"]),
        ("Tarih", record["created_at"]),
        ("Harmony Ref", record["harmony_ref"]),
        ("Koli Sayısı", str(record["koli_sayisi"])),
        ("Vardiya Amiri", record["vardiya_amiri"]),
        ("Depo", record["depo"]),
        ("Kaydı Giren", record.get("created_by","-")),
    ]
    for label, value in lines:
        c.drawString(margin, y, f"{label}: {value}")
        y -= 6 * mm

    c.setFont("Helvetica-Oblique", 9)
    c.drawString(margin, y, "Bu fiş sistem tarafından otomatik üretilmiştir.")
    c.showPage()
    c.save()
    pdf = buf.getvalue()
    buf.close()
    return pdf

# ---------- APP ----------
st.set_page_config(page_title=APP_TITLE, page_icon="📦", layout="wide")
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

init_db()
ensure_default_users(hash_it)

# Header
st.markdown(f"""
<div class="header">
  <div class="logo">LC</div>
  <div>
    <div style="font-size:22px; font-weight:800;">LC Waikiki</div>
    <div style="opacity:0.9;">Hurda Koli Takip Sistemi</div>
  </div>
</div>
""", unsafe_allow_html=True)
st.write("")

# ---------- GİRİŞ ----------
if "user" not in st.session_state:
    with st.container(border=True):
        st.subheader("Giriş")

        role_choice = st.radio(
            "Rol",
            options=["admin", "security"],
            index=1,
            format_func=lambda x: "Yetkili Girişi" if x == "admin" else "Güvenlik Girişi",
            horizontal=True
        )
        username = st.text_input("Kullanıcı Adı", placeholder="admin, guvenlik veya mesut.ozel")
        password = st.text_input("Şifre", type="password", placeholder="••••••••")

        c1, c2, c3 = st.columns([1, 1, 1])
        with c1:
            if st.button("Manuel Giriş"):
                ok, res = authenticate(username.strip(), password, role_choice)
                if ok:
                    st.session_state.user = res
                    st.rerun()
                else:
                    st.error(res)
        with c2:
            with st.popover("Hesap Oluştur"):
                new_u = st.text_input("Yeni Kullanıcı Adı", key="r_u")
                new_p = st.text_input("Şifre", type="password", key="r_p")
                new_role = st.selectbox("Rol", options=["admin","security"], index=1)
                if st.button("Kaydı Tamamla", type="primary"):
                    ok, msg = register_user(new_u.strip(), new_p, new_role)
                    (st.success if ok else st.error)(msg)
        with c3:
            with st.popover("Şifremi Unuttum"):
                u = st.text_input("Kullanıcı Adı", key="f_u")
                if st.button("Geçici Şifre Oluştur"):
                    ok, msg = reset_password(u.strip())
                    (st.success if ok else st.error)(msg)
    st.stop()

# ---------- Sidebar ----------
role = st.session_state.user["role"]
menu_items = ["Kayıtlar"] if role != "admin" else ["Dashboard","Koli Ver","Personeller","Kayıtlar","Excel Yükle","Raporlar","İstatistikler"]
st.sidebar.title("Menü")
page = st.sidebar.radio("Modüller", menu_items, index=0)
st.sidebar.info(f"Giriş: **{st.session_state.user['username']}** ({'Yetkili' if role=='admin' else 'Güvenlik'})")
if st.sidebar.button("Çıkış"):
    st.session_state.clear(); st.rerun()

# ---------- SAYFALAR ----------
if page == "Dashboard":
    # Üst metrikler
    t_koli, t_pers, t_rec = totals()
    c1, c2, c3 = st.columns(3)
    c1.markdown(f'<div class="metric-card"><div>Toplam Koli</div><div style="font-size:26px;font-weight:800">{t_koli}</div></div>', unsafe_allow_html=True)
    c2.markdown(f'<div class="metric-card"><div>Personel</div><div style="font-size:26px;font-weight:800">{t_pers}</div></div>', unsafe_allow_html=True)
    c3.markdown(f'<div class="metric-card"><div>Kayıt</div><div style="font-size:26px;font-weight:800">{t_rec}</div></div>', unsafe_allow_html=True)

    st.write("")

    # ── Vardiya Amiri Kırılımı — Son 365 Gün (yüzde + adet)
    st.markdown("### Vardiya Amiri Kırılımı – Son 365 Gün (yüzde + adet)")
    conn = get_conn()
    since = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")
    df_amir = pd.read_sql_query(
        """
        SELECT vardiya_amiri AS Amir, SUM(koli_sayisi) AS Koli
        FROM scrap_records
        WHERE created_at >= ?
        GROUP BY vardiya_amiri
        ORDER BY Koli DESC
        """,
        conn, params=(since,)
    )
    # ── Depo Kırılımı — Son 365 Gün (yüzde + adet)
    df_depo = pd.read_sql_query(
        """
        SELECT depo AS Depo, SUM(koli_sayisi) AS Koli
        FROM scrap_records
        WHERE created_at >= ?
        GROUP BY depo
        ORDER BY Koli DESC
        """,
        conn, params=(since,)
    )
    conn.close()

    if not df_amir.empty and df_amir["Koli"].sum() > 0:
        df_amir["Etiket"] = df_amir.apply(lambda r: f"{r['Amir']} ({int(r['Koli'])})", axis=1)
        fig_amir = px.pie(df_amir, names="Etiket", values="Koli")
        fig_amir.update_traces(textinfo="percent+label",
                               hovertemplate="%{label}<br>Koli: %{value}<br>Pay: %{percent}")
        st.plotly_chart(fig_amir, use_container_width=True)
    else:
        st.info("Son 365 günde amir kırılımında veri yok.")

    st.markdown("### Depo Kırılımı – Son 365 Gün (yüzde + adet)")
    if not df_depo.empty and df_depo["Koli"].sum() > 0:
        df_depo["Etiket"] = df_depo.apply(lambda r: f"{r['Depo']} ({int(r['Koli'])})", axis=1)
        fig_depo = px.pie(df_depo, names="Etiket", values="Koli")
        fig_depo.update_traces(textinfo="percent+label",
                               hovertemplate="%{label}<br>Koli: %{value}<br>Pay: %{percent}")
        st.plotly_chart(fig_depo, use_container_width=True)
    else:
        st.info("Son 365 günde depo kırılımında veri yok.")

elif page == "Koli Ver":
    st.subheader("Koli Ver / Kayıt Oluştur")

    # Özet kutuları için Harmony Ref
    hr = st.text_input("Harmony Ref *", key="hr_input", placeholder="Örn: HRM123456").strip()
    if hr:
        used_year = last_year_total_for(hr)
        conn = get_conn(); cur = conn.cursor()
        start = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        cur.execute("""
            SELECT COALESCE(SUM(koli_sayisi),0) AS total
            FROM scrap_records
            WHERE harmony_ref=? AND created_at >= ? AND strftime('%Y-%m', created_at)=?
        """, (hr, start.strftime("%Y-%m-%d %H:%M:%S"), datetime.now().strftime("%Y-%m")))
        used_month = int((cur.fetchone() or {"total":0})["total"]); conn.close()
        left = max(0, MAX_YEAR - used_year)
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="info-card">Bu Ay<br><span class="big">{used_month}</span> koli</div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="info-card">Son 365 Gün<br><span class="big">{used_year}</span> koli</div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="info-card">Kalan Hak<br><span class="big">{left}</span> / {MAX_YEAR}</div>', unsafe_allow_html=True)

    # Listeler
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT name FROM shift_leaders ORDER BY name;")
    leaders = [r["name"] for r in cur.fetchall()]
    cur.execute("SELECT name FROM warehouses ORDER BY name;")
    warehouses = [r["name"] for r in cur.fetchall()]
    conn.close()

    uname = st.session_state.user["username"].lower()
    current_leader = USERNAME_TO_LEADER.get(uname, uname.replace(".", " ").title())

    if role == "admin" and st.session_state.user["username"] != "admin" and current_leader in leaders:
        leader_options = [current_leader]; leader_disabled = True; leader_index = 0
    else:
        leader_options = leaders; leader_disabled = False; leader_index = 0

    with st.form("koli_form", border=True):
        form_serial = st.text_input("Form Seri No *", placeholder="Örn: FSN-2025-000123").strip()
        koli = st.number_input("Koli Sayısı *", min_value=1, max_value=MAX_ONCE, step=1, value=1)
        vardiya = st.selectbox("Vardiya Amiri *", options=leader_options, index=leader_index, disabled=leader_disabled)
        depo = st.selectbox("Depo *", options=warehouses, index=0)
        submitted = st.form_submit_button("Kaydı Oluştur")

        if submitted:
            if not hr or not form_serial or not vardiya or not depo:
                st.error("Tüm alanlar zorunludur. Lütfen eksikleri tamamlayın.")
            else:
                # amir hesabında sunucu tarafında da kilit
                if role == "admin" and st.session_state.user["username"] != "admin" and current_leader in leaders:
                    vardiya = current_leader

                used_year = last_year_total_for(hr)
                remaining = MAX_YEAR - used_year
                if koli > MAX_ONCE:
                    st.error(f"Tek seferde en fazla {MAX_ONCE} koli verilebilir.")
                elif remaining <= 0:
                    st.error(f"Yıllık limit ({MAX_YEAR}) dolmuş. Yeni koli verilemez.")
                elif koli > remaining:
                    st.error(f"Yıllık limit aşılıyor. Kalan hak: {remaining} koli.")
                else:
                    upsert_person_minimal(hr, vardiya, depo)
                    rec_id = record_scrap(hr, koli, vardiya, depo, form_serial)
                    st.success("Kayıt eklendi.")

                    conn = get_conn(); cur = conn.cursor()
                    cur.execute("""
                        SELECT id, harmony_ref, koli_sayisi, vardiya_amiri, depo, form_serial,
                               datetime(created_at) AS created_at
                        FROM scrap_records WHERE id=?
                    """, (rec_id,))
                    r = cur.fetchone(); conn.close()
                    rec = dict(r); rec["created_by"] = st.session_state.user["username"]
                    st.session_state["last_pdf_bytes"] = make_receipt_pdf(rec)
                    st.session_state["last_pdf_name"]  = f"HKTS_FIS_{rec['id']}.pdf"

    if st.session_state.get("last_pdf_bytes"):
        st.download_button("PDF Fişi İndir",
            data=st.session_state["last_pdf_bytes"],
            file_name=st.session_state.get("last_pdf_name","HKTS_FIS.pdf"),
            mime="application/pdf")

elif page == "Personeller":
    st.subheader("Personeller")
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM personnel ORDER BY harmony_ref", conn)
    conn.close()
    st.data_editor(df, height=520, use_container_width=True, disabled=True, hide_index=False, num_rows="fixed")

elif page == "Kayıtlar":
    st.subheader("Kayıtlar (Filtreli Görünüm)")
    c1, c2, c3, c4 = st.columns(4)
    ref_f  = c1.text_input("Harmony Ref ile ara")
    amir_f = c2.text_input("Vardiya Amiri ile ara")
    depo_f = c3.text_input("Depo ile ara")
    tarih  = c4.date_input("Tarih Aralığı", value=(datetime.now()-timedelta(days=30), datetime.now()))

    q = """
    SELECT r.id, r.form_serial AS 'Form Seri No', r.harmony_ref AS 'Harmony Ref',
           p.ad_soyad AS 'Ad Soyad', r.koli_sayisi AS 'Koli',
           r.vardiya_amiri AS 'Vardiya Amiri', r.depo AS 'Depo',
           r.created_at AS 'Oluşturma'
    FROM scrap_records r
    LEFT JOIN personnel p ON p.harmony_ref = r.harmony_ref
    WHERE 1=1
    """
    params = []
    if ref_f.strip():
        q += " AND r.harmony_ref LIKE ?"; params.append(f"%{ref_f.strip()}%")
    if amir_f.strip():
        q += " AND r.vardiya_amiri LIKE ?"; params.append(f"%{amir_f.strip()}%")
    if depo_f.strip():
        q += " AND r.depo LIKE ?"; params.append(f"%{depo_f.strip()}%")
    if isinstance(tarih, tuple) and len(tarih)==2:
        start = datetime.combine(tarih[0], datetime.min.time())
        end   = datetime.combine(tarih[1], datetime.max.time())
        q += " AND r.created_at BETWEEN ? AND ?"
        params.extend([start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")])

    conn = get_conn()
    df = pd.read_sql_query(q + " ORDER BY r.created_at DESC", conn, params=tuple(params))
    conn.close()

    # Sadece görüntüleme + CSV indirme (Düzenle/Sil kaldırıldı)
    st.data_editor(df, height=420, use_container_width=True, disabled=True)

    if role == "admin":
        st.download_button(
            "CSV Olarak İndir",
            df.to_csv(index=False).encode("utf-8"),
            file_name="kayitlar.csv",
            mime="text/csv"
        )

elif page == "Raporlar":
    st.subheader("Raporlar – Amir • Depo • Tarih kırılımı")

    # Filtre bileşenleri (varsayılan: boş = tümü)
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT name FROM shift_leaders ORDER BY name;")
    all_leaders = [r["name"] for r in cur.fetchall()]
    cur.execute("SELECT name FROM warehouses ORDER BY name;")
    all_depos = [r["name"] for r in cur.fetchall()]
    conn.close()

    c1, c2 = st.columns(2)
    leaders_sel = c1.multiselect("Vardiya Amiri (boş = tümü)", options=all_leaders, default=[])
    depos_sel   = c2.multiselect("Depo (boş = tümü)", options=all_depos, default=[])

    use_date = st.toggle("Tarih filtresi kullan", value=False)
    if use_date:
        c3, c4 = st.columns(2)
        date_from = c3.date_input("Başlangıç", value=date.today().replace(day=1))
        date_to   = c4.date_input("Bitiş", value=date.today())
    else:
        date_from = None
        date_to = None

    # Sorgu
    q = """
    SELECT r.harmony_ref, p.ad_soyad, r.koli_sayisi, r.vardiya_amiri, r.depo, r.form_serial,
           datetime(r.created_at) AS created_at
    FROM scrap_records r
    LEFT JOIN personnel p ON p.harmony_ref = r.harmony_ref
    WHERE 1=1
    """
    params = []
    if leaders_sel:
        q += " AND r.vardiya_amiri IN ({})".format(",".join("?"*len(leaders_sel))); params.extend(leaders_sel)
    if depos_sel:
        q += " AND r.depo IN ({})".format(",".join("?"*len(depos_sel))); params.extend(depos_sel)
    if date_from and date_to:
        start = datetime.combine(normalize_date(date_from), datetime.min.time())
        end   = datetime.combine(normalize_date(date_to), datetime.max.time())
        q += " AND r.created_at BETWEEN ? AND ?"
        params.extend([start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")])

    conn = get_conn()
    df = pd.read_sql_query(q + " ORDER BY r.created_at DESC", conn, params=tuple(params))
    conn.close()

    st.markdown("#### Detay Kayıtlar")
    st.data_editor(df, height=320, use_container_width=True, disabled=True)

    if not df.empty:
        st.markdown("#### Kırılım Tablosu (Amir x Depo)")
        pivot = pd.pivot_table(df, values="koli_sayisi", index="vardiya_amiri", columns="depo",
                               aggfunc="sum", fill_value=0, margins=True, margins_name="TOPLAM")
        st.data_editor(pivot, height=280, use_container_width=True, disabled=True)

        out = BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as xw:
            df.to_excel(xw, index=False, sheet_name="Detay")
            pivot.to_excel(xw, sheet_name="Kirilim")
        st.download_button("Excel İndir (Detay + Kırılım)", data=out.getvalue(), file_name="HKTS_Rapor.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("Kayıt bulunamadı.")

elif page == "Excel Yükle":
    st.subheader("Haftalık Personel Listesi Yükle")
    st.caption("Şablon sütunları: Servis Lokasyonu, Harmony Ref, Kayıt No, Adı, Soyadı, Görevi, Telefon, İş Telefonu, Dahili, İşe Giriş Tarihi, İşten Çıkış, Tarihi, Güzergah, Cadde, Durak, Adres, ilçe, Ana Süreç, Detay Süreç, Giriş Lokasyonu, Çıkış Lokasyonu, Beyaz Yaka, Servis, Ad Soyad")

    with st.container(border=True):
        f = st.file_uploader("Excel (.xlsx) seçin ve yükleyin", type=["xlsx"])
        if f is None:
            st.info("Henüz dosya seçilmedi.")
        else:
            try:
                df = pd.read_excel(f, dtype=str).fillna("")
                expected = ["Servis Lokasyonu","Harmony Ref","Kayıt No","Adı","Soyadı","Görevi","Telefon",
                            "İş Telefonu","Dahili","İşe Giriş Tarihi","İşten Çıkış","Tarihi","Güzergah",
                            "Cadde","Durak","Adres","ilçe","Ana Süreç","Detay Süreç","Giriş Lokasyonu",
                            "Çıkış Lokasyonu","Beyaz Yaka","Servis","Ad Soyad"]
                missing = [c for c in expected if c not in df.columns]
                if missing:
                    st.error(f"Eksik sütun(lar): {missing}")
                else:
                    conn = get_conn(); cur = conn.cursor()
                    cnt=0
                    for _, r in df.iterrows():
                        if not str(r["Harmony Ref"]).strip():
                            continue
                        cur.execute("""
                            INSERT INTO personnel(
                                harmony_ref,kayit_no,adi,soyadi,gorevi,telefon,is_telefonu,dahili,
                                ise_giris_tarihi,isten_cikis_tarihi,tarihi,guzergah,cadde,durak,adres,
                                ilce,ana_surec,detay_surec,giris_lokasyonu,cikis_lokasyonu,beyaz_yaka,
                                servis,ad_soyad,servis_lokasyonu
                            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            ON CONFLICT(harmony_ref) DO UPDATE SET
                                kayit_no=excluded.kayit_no,
                                adi=excluded.adi,
                                soyadi=excluded.soyadi,
                                gorevi=excluded.gorevi,
                                telefon=excluded.telefon,
                                is_telefonu=excluded.is_telefonu,
                                dahili=excluded.dahili,
                                ise_giris_tarihi=excluded.ise_giris_tarihi,
                                isten_cikis_tarihi=excluded.isten_cikis_tarihi,
                                tarihi=excluded.tarihi,
                                guzergah=excluded.guzergah,
                                cadde=excluded.cadde,
                                durak=excluded.durak,
                                adres=excluded.adres,
                                ilce=excluded.ilce,
                                ana_surec=excluded.ana_surec,
                                detay_surec=excluded.detay_surec,
                                giris_lokasyonu=excluded.giris_lokasyonu,
                                cikis_lokasyonu=excluded.cikis_lokasyonu,
                                beyaz_yaka=excluded.beyaz_yaka,
                                servis=excluded.servis,
                                ad_soyad=excluded.ad_soyad,
                                servis_lokasyonu=excluded.servis_lokasyonu
                        """, (
                            r["Harmony Ref"].strip(), r["Kayıt No"], r["Adı"], r["Soyadı"], r["Görevi"],
                            r["Telefon"], r["İş Telefonu"], r["Dahili"], r["İşe Giriş Tarihi"], r["İşten Çıkış"],
                            r["Tarihi"], r["Güzergah"], r["Cadde"], r["Durak"], r["Adres"], r["ilçe"],
                            r["Ana Süreç"], r["Detay Süreç"], r["Giriş Lokasyonu"], r["Çıkış Lokasyonu"],
                            int(r["Beyaz Yaka"]) if str(r["Beyaz Yaka"]).strip().isdigit() else None,
                            r["Servis"], r["Ad Soyad"], r.get("Servis Lokasyonu","")
                        ))
                        cnt += 1
                    conn.commit(); conn.close()
                    st.success(f"Yükleme tamamlandı. Güncellenen/eklenen kişi sayısı: {cnt}")
            except Exception as e:
                st.error(f"Yükleme hatası: {e}")

elif page == "İstatistikler":
    st.subheader("Aylık Toplam Koli")
    conn = get_conn()
    q = """
    SELECT strftime('%Y-%m', created_at) AS Ay, SUM(koli_sayisi) AS Toplam
    FROM scrap_records
    GROUP BY strftime('%Y-%m', created_at)
    ORDER BY Ay DESC
    """
    df = pd.read_sql_query(q, conn)
    conn.close()
    st.data_editor(df, height=380, use_container_width=True, disabled=True)
