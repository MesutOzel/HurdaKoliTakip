import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

DB_PATH = Path(__file__).with_name("hkts.db")

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _column_exists(cur, table, col):
    cur.execute(f"PRAGMA table_info({table});")
    return any(r["name"] == col for r in cur.fetchall())

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # USERS
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT CHECK(role IN ('admin','security')) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # PERSONNEL
    cur.execute("""
    CREATE TABLE IF NOT EXISTS personnel (
        harmony_ref TEXT PRIMARY KEY,
        kayit_no TEXT,
        adi TEXT,
        soyadi TEXT,
        gorevi TEXT,
        telefon TEXT,
        is_telefonu TEXT,
        dahili TEXT,
        ise_giris_tarihi TEXT,
        isten_cikis_tarihi TEXT,
        tarihi TEXT,
        guzergah TEXT,
        cadde TEXT,
        durak TEXT,
        adres TEXT,
        ilce TEXT,
        ana_surec TEXT,
        detay_surec TEXT,
        giris_lokasyonu TEXT,
        cikis_lokasyonu TEXT,
        beyaz_yaka INTEGER,
        servis TEXT,
        ad_soyad TEXT,
        servis_lokasyonu TEXT,
        vardiya_amiri TEXT,
        depo TEXT
    );
    """)

    # SCRAP RECORDS
    cur.execute("""
    CREATE TABLE IF NOT EXISTS scrap_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        harmony_ref TEXT NOT NULL,
        koli_sayisi INTEGER NOT NULL,
        vardiya_amiri TEXT NOT NULL,
        depo TEXT NOT NULL,
        form_serial TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (harmony_ref) REFERENCES personnel(harmony_ref)
    );
    """)

    # Eski DB'de form_serial yoksa ekle (migrasyon)
    if not _column_exists(cur, "scrap_records", "form_serial"):
        cur.execute("ALTER TABLE scrap_records ADD COLUMN form_serial TEXT;")
        cur.execute("UPDATE scrap_records SET form_serial = COALESCE(form_serial, 'GECMISIYUKLEME');")

    # Referans tablolar
    cur.execute("""CREATE TABLE IF NOT EXISTS shift_leaders (name TEXT PRIMARY KEY);""")
    cur.execute("""CREATE TABLE IF NOT EXISTS warehouses (name TEXT PRIMARY KEY);""")

    leaders = [
        "Mesut Özel","Serhan Atilla","Erdal Adıgüzel Biçer","Levent Şengül","Fırat Küllü",
        "Özkan Kılıç","Büşra Cici","Cahit Altun","Emrah Dubaz","Halit Kaya",
        "Şenol Oğraş","Barış Orhan","Yusuf Sayan"
    ]
    for l in leaders:
        cur.execute("INSERT OR IGNORE INTO shift_leaders(name) VALUES(?)", (l,))

    warehouses = ["Lm Depo","Poyraz Depo","Eroğlu Depo","Titiz Depo","Yalova Depo","Aksaray Depo","Yılmaz Depo"]
    for w in warehouses:
        cur.execute("INSERT OR IGNORE INTO warehouses(name) VALUES(?)", (w,))

    conn.commit()
    conn.close()

def ensure_default_users(hasher):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO users(username, password_hash, role) VALUES(?,?,?)",
                    ("admin", hasher("admin123"), "admin"))
    except Exception:
        pass
    try:
        cur.execute("INSERT INTO users(username, password_hash, role) VALUES(?,?,?)",
                    ("guvenlik", hasher("guvenlik123"), "security"))
    except Exception:
        pass
    conn.commit()
    conn.close()

def last_year_total_for(harmony_ref: str) -> int:
    """Son 365 gün içinde verilen koli toplamı."""
    conn = get_conn()
    cur = conn.cursor()
    since = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("""
        SELECT COALESCE(SUM(koli_sayisi),0) AS total
        FROM scrap_records
        WHERE harmony_ref=? AND created_at >= ?
    """, (harmony_ref, since))
    row = cur.fetchone()
    conn.close()
    return int(row["total"] if row and row["total"] is not None else 0)

def totals():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(SUM(koli_sayisi),0) AS t FROM scrap_records;")
    t = cur.fetchone()["t"]
    cur.execute("SELECT COUNT(*) AS c FROM personnel;")
    p = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) AS c FROM scrap_records;")
    r = cur.fetchone()["c"]
    conn.close()
    return int(t), int(p), int(r)

def group_totals_by(field: str):
    """field: 'vardiya_amiri' veya 'depo'"""
    assert field in ("vardiya_amiri", "depo")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT COALESCE({field}, '(Belirtilmedi)') AS grp, SUM(koli_sayisi) AS toplam
        FROM scrap_records
        GROUP BY COALESCE({field}, '(Belirtilmedi)')
        ORDER BY toplam DESC;
    """)
    rows = cur.fetchall()
    conn.close()
    labels = [r["grp"] for r in rows]
    values = [int(r["toplam"] or 0) for r in rows]
    return labels, values
