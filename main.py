"""
Kompyuter xonaga ro'yxatga oluvchi Telegram bot.

Render'ning bepul veb-servisida ishlaydi:
  • Telegram yangilanishlari webhook orqali keladi (polling emas)
  • "/" manzili UptimeRobot uchun — servis uxlab qolmasligi uchun
  • Ma'lumotlar DATABASE_URL (Postgres) da saqlanadi; u bo'lmasa lokal SQLite

Oqim:
  /start → xona haqida ma'lumot + [Ro'yxatdan o'tish]
         → bo'sh vaqtlar → vaqt tanlanadi
         → Ism → Familiya → Telefon → tasdiq
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import requests
from flask import Flask, jsonify, request

# --------------------------------------------------------------------------
# Sozlamalar
# --------------------------------------------------------------------------

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "hook").strip()
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
SQLITE_PATH = os.environ.get("SQLITE_PATH", "combot.db")
TZ_OFFSET = int(os.environ.get("TZ_OFFSET", "5"))          # Toshkent = UTC+5
REPORT_HOUR = int(os.environ.get("REPORT_HOUR", "20"))      # kunlik hisobot soati
SLOT_CAPACITY = max(1, int(os.environ.get("SLOT_CAPACITY", "10")))  # bitta vaqtga nechta joy
ADMIN_IDS = [
    int(x) for x in re.split(r"[,;\s]+", os.environ.get("ADMIN_IDS", "")) if x.strip().lstrip("-").isdigit()
]

API = f"https://api.telegram.org/bot{BOT_TOKEN}"
IS_PG = DATABASE_URL.startswith(("postgres://", "postgresql://"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s")
log = logging.getLogger("combot")

if IS_PG:
    import psycopg
    from psycopg.rows import dict_row
else:
    import sqlite3

DEFAULT_INFO = """🖥 <b>Kompyuter xona</b>

Bu yerda zamonaviy kompyuterlarda ishlashingiz mumkin.

💵 <b>Narx:</b> soatiga 5 000 so'm

✨ <b>Nimalar bor:</b>
• Google Gemini Plus obunasi
• Tez internet
• Printer va skaner
• Qulay ish stoli

📍 <b>Manzil:</b> (admin o'zgartiradi)
📞 <b>Aloqa:</b> (admin o'zgartiradi)

Quyidagi tugmani bosib, o'zingizga qulay vaqtni band qiling 👇"""


# --------------------------------------------------------------------------
# Ma'lumotlar bazasi (Postgres yoki SQLite)
# --------------------------------------------------------------------------

_sqlite_lock = threading.Lock()


@contextmanager
def _conn():
    """Har so'rov uchun qisqa ulanish — Neon'ning bepul soatlarini tejaydi."""
    if IS_PG:
        with psycopg.connect(DATABASE_URL, row_factory=dict_row, connect_timeout=15) as connection:
            yield connection
    else:
        with _sqlite_lock:
            connection = sqlite3.connect(SQLITE_PATH)
            connection.row_factory = sqlite3.Row
            try:
                yield connection
                connection.commit()
            finally:
                connection.close()


def _sql(statement: str) -> str:
    return statement.replace("?", "%s") if IS_PG else statement


def query(statement: str, params: Iterable[Any] = ()) -> list[dict]:
    with _conn() as connection:
        cursor = connection.execute(_sql(statement), tuple(params))
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


def query_one(statement: str, params: Iterable[Any] = ()) -> dict | None:
    rows = query(statement, params)
    return rows[0] if rows else None


def execute(statement: str, params: Iterable[Any] = ()) -> None:
    with _conn() as connection:
        connection.execute(_sql(statement), tuple(params))


def insert(statement: str, params: Iterable[Any] = ()) -> int:
    """INSERT qiladi va yangi qatorning id sini qaytaradi."""
    with _conn() as connection:
        if IS_PG:
            cursor = connection.execute(_sql(statement) + " RETURNING id", tuple(params))
            row = cursor.fetchone()
            return int(row["id"]) if row else 0
        cursor = connection.execute(statement, tuple(params))
        return int(cursor.lastrowid or 0)


SCHEMA_SQLITE = [
    """CREATE TABLE IF NOT EXISTS settings (
         key TEXT PRIMARY KEY,
         value TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS slots (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         label TEXT NOT NULL,
         starts_at TEXT NOT NULL,
         ends_at TEXT NOT NULL,
         capacity INTEGER NOT NULL DEFAULT 10,
         created_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS bookings (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         slot_id INTEGER NOT NULL,
         tg_id INTEGER NOT NULL,
         username TEXT NOT NULL DEFAULT '',
         first_name TEXT NOT NULL,
         last_name TEXT NOT NULL,
         phone TEXT NOT NULL,
         created_at TEXT NOT NULL,
         UNIQUE (slot_id, tg_id))""",
    """CREATE TABLE IF NOT EXISTS states (
         tg_id INTEGER PRIMARY KEY,
         step TEXT NOT NULL,
         slot_id INTEGER,
         first_name TEXT,
         last_name TEXT,
         updated_at TEXT NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS idx_slots_start ON slots (starts_at)",
    "CREATE INDEX IF NOT EXISTS idx_bookings_slot ON bookings (slot_id)",
]

SCHEMA_PG = [
    """CREATE TABLE IF NOT EXISTS settings (
         key TEXT PRIMARY KEY,
         value TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS slots (
         id SERIAL PRIMARY KEY,
         label TEXT NOT NULL,
         starts_at TEXT NOT NULL,
         ends_at TEXT NOT NULL,
         capacity INTEGER NOT NULL DEFAULT 10,
         created_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS bookings (
         id SERIAL PRIMARY KEY,
         slot_id INTEGER NOT NULL,
         tg_id BIGINT NOT NULL,
         username TEXT NOT NULL DEFAULT '',
         first_name TEXT NOT NULL,
         last_name TEXT NOT NULL,
         phone TEXT NOT NULL,
         created_at TEXT NOT NULL,
         UNIQUE (slot_id, tg_id))""",
    """CREATE TABLE IF NOT EXISTS states (
         tg_id BIGINT PRIMARY KEY,
         step TEXT NOT NULL,
         slot_id INTEGER,
         first_name TEXT,
         last_name TEXT,
         updated_at TEXT NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS idx_slots_start ON slots (starts_at)",
    "CREATE INDEX IF NOT EXISTS idx_bookings_slot ON bookings (slot_id)",
]


def migrate() -> None:
    """Eski bazani yangi tuzilishga o'tkazadi (bitta vaqt → bir nechta joy).

    Avval `bookings.slot_id` UNIQUE edi — bitta vaqtga bitta odam. Endi joylar
    soni `slots.capacity` bilan belgilanadi, takrorlanish esa (slot_id, tg_id)
    juftligi bo'yicha to'siladi.
    """
    if IS_PG:
        execute(f"ALTER TABLE slots ADD COLUMN IF NOT EXISTS capacity INTEGER NOT NULL DEFAULT {SLOT_CAPACITY}")
        # Faqat slot_id ustiga qo'yilgan UNIQUE cheklovni nomi bo'yicha topib o'chiramiz
        stale = query(
            "SELECT c.conname FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid"
            " WHERE t.relname = 'bookings' AND c.contype = 'u'"
            " AND array_length(c.conkey, 1) = 1"
            " AND (SELECT a.attname FROM pg_attribute a"
            "      WHERE a.attrelid = t.oid AND a.attnum = c.conkey[1]) = 'slot_id'"
        )
        for row in stale:
            execute(f'ALTER TABLE bookings DROP CONSTRAINT "{row["conname"]}"')
            log.info("Eski cheklov o'chirildi: %s", row["conname"])
        execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_bookings_slot_user ON bookings (slot_id, tg_id)")
        return

    columns = [row["name"] for row in query("PRAGMA table_info(slots)")]
    if "capacity" not in columns:
        execute(f"ALTER TABLE slots ADD COLUMN capacity INTEGER NOT NULL DEFAULT {SLOT_CAPACITY}")

    # SQLite'da inline UNIQUE ni olib tashlash uchun jadvalni qayta qurish kerak
    indexes = query("PRAGMA index_list(bookings)")
    needs_rebuild = False
    for index in indexes:
        if not index.get("unique"):
            continue
        cols = [c["name"] for c in query(f"PRAGMA index_info({index['name']})")]
        if cols == ["slot_id"]:
            needs_rebuild = True
    if needs_rebuild:
        with _conn() as connection:
            connection.executescript(
                """
                ALTER TABLE bookings RENAME TO bookings_old;
                CREATE TABLE bookings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slot_id INTEGER NOT NULL,
                    tg_id INTEGER NOT NULL,
                    username TEXT NOT NULL DEFAULT '',
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    phone TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (slot_id, tg_id));
                INSERT INTO bookings SELECT id, slot_id, tg_id, username, first_name,
                    last_name, phone, created_at FROM bookings_old;
                DROP TABLE bookings_old;
                CREATE INDEX IF NOT EXISTS idx_bookings_slot ON bookings (slot_id);
                """
            )
        log.info("bookings jadvali yangilandi (slot_id UNIQUE olib tashlandi)")


def init_db() -> None:
    for statement in SCHEMA_PG if IS_PG else SCHEMA_SQLITE:
        execute(statement)
    migrate()
    log.info("Baza tayyor (%s) · bitta vaqtga %d joy",
             "Postgres" if IS_PG else f"SQLite: {SQLITE_PATH}", SLOT_CAPACITY)


def get_setting(key: str, fallback: str) -> str:
    row = query_one("SELECT value FROM settings WHERE key = ?", (key,))
    return row["value"] if row else fallback


def set_setting(key: str, value: str) -> None:
    execute(
        "INSERT INTO settings (key, value) VALUES (?, ?)"
        " ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        (key, value),
    )


# --------------------------------------------------------------------------
# Vaqt
# --------------------------------------------------------------------------

MONTHS = ["yanvar", "fevral", "mart", "aprel", "may", "iyun",
          "iyul", "avgust", "sentabr", "oktabr", "noyabr", "dekabr"]
WEEKDAYS = ["dushanba", "seshanba", "chorshanba", "payshanba", "juma", "shanba", "yakshanba"]


def now_local() -> str:
    """Mahalliy vaqt 'YYYY-MM-DD HH:MM' ko'rinishida."""
    return (datetime.now(timezone.utc) + timedelta(hours=TZ_OFFSET)).strftime("%Y-%m-%d %H:%M")


def today_local() -> str:
    return now_local()[:10]


def humanize(starts_at: str, ends_at: str) -> str:
    """'2026-10-15 14:00' → '15-oktabr (payshanba), 14:00–16:00'"""
    date_part, start = starts_at.split(" ")
    end = ends_at.split(" ")[1]
    year, month, day = (int(x) for x in date_part.split("-"))
    weekday = WEEKDAYS[datetime(year, month, day).weekday()]
    return f"{day}-{MONTHS[month - 1]} ({weekday}), {start}–{end}"


SLOT_RE_DMY = re.compile(
    r"^(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\s+(\d{1,2}):(\d{2})\s*(?:[-–—]|\s)\s*(\d{1,2}):(\d{2})$"
)
SLOT_RE_ISO = re.compile(
    r"^(\d{4})-(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{2})\s*(?:[-–—]|\s)\s*(\d{1,2}):(\d{2})$"
)


CAPACITY_RE = re.compile(r"\s*[x×*]\s*(\d{1,3})$", re.IGNORECASE)


def parse_slot_line(line: str) -> tuple[str, str, str, int] | str:
    """Bitta qatorni o'qiydi. Natija: (label, starts_at, ends_at, joy soni) yoki xato matni.

    Qabul qilinadi: '15.10 14:00-16:00', '15.10.2027 ...', '2026-10-15 ...',
    '15/10 14:00 16:00'. Yil yozilmasa — joriy yil, sana o'tgan bo'lsa keyingisi.
    Oxiriga 'x5' qo'shilsa, shu vaqtga 5 joy ajratiladi (standart: SLOT_CAPACITY).
    """
    text = " ".join(line.split())
    if not text:
        return "bo'sh qator"

    capacity = SLOT_CAPACITY
    tail = CAPACITY_RE.search(text)
    if tail:
        capacity = int(tail.group(1))
        if capacity < 1:
            return f'"{text}" — joy soni kamida 1 bo\'lishi kerak'
        text = text[: tail.start()].strip()

    iso = SLOT_RE_ISO.match(text)
    dmy = None if iso else SLOT_RE_DMY.match(text)
    if not iso and not dmy:
        return f'"{text}" tushunarsiz. Namuna: 15.10 14:00-16:00'

    if iso:
        year, month, day = int(iso[1]), int(iso[2]), int(iso[3])
        start_h, start_m, end_h, end_m = int(iso[4]), int(iso[5]), int(iso[6]), int(iso[7])
        year_given = True
    else:
        day, month = int(dmy[1]), int(dmy[2])
        raw_year = dmy[3]
        year_given = raw_year is not None
        if raw_year:
            year = int(f"20{raw_year}") if len(raw_year) == 2 else int(raw_year)
        else:
            year = int(today_local()[:4])
        start_h, start_m, end_h, end_m = int(dmy[4]), int(dmy[5]), int(dmy[6]), int(dmy[7])

    if not (1 <= month <= 12 and 1 <= day <= 31):
        return f'"{text}" — bunday sana yo\'q'
    if start_h > 23 or end_h > 23 or start_m > 59 or end_m > 59:
        return f'"{text}" — bunday soat yo\'q'

    def stamp(y: int, h: int, m: int) -> str:
        return f"{y:04d}-{month:02d}-{day:02d} {h:02d}:{m:02d}"

    starts_at = stamp(year, start_h, start_m)
    if not year_given and starts_at < now_local():
        year += 1
        starts_at = stamp(year, start_h, start_m)
    ends_at = stamp(year, end_h, end_m)

    if ends_at <= starts_at:
        return f'"{text}" — tugash vaqti kechroq bo\'lishi kerak'
    try:
        datetime(year, month, day)
    except ValueError:
        return f'"{text}" — bunday sana yo\'q'

    return humanize(starts_at, ends_at), starts_at, ends_at, capacity


# --------------------------------------------------------------------------
# Telegram
# --------------------------------------------------------------------------

def esc(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def tg(method: str, payload: dict) -> dict:
    try:
        response = requests.post(f"{API}/{method}", json=payload, timeout=20)
        data = response.json()
        if not data.get("ok"):
            log.error("Telegram %s: %s", method, data.get("description"))
        return data
    except Exception as error:  # noqa: BLE001 - tarmoq uzilishi botni to'xtatmasin
        log.error("Telegram %s xatosi: %s", method, error)
        return {"ok": False}


def send(chat_id: int, text: str, keyboard: list | None = None,
         reply_keyboard: dict | None = None, remove_keyboard: bool = False) -> dict:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text[:4000],
        "parse_mode": "HTML",
        "link_preview_options": {"is_disabled": True},
    }
    if keyboard:
        payload["reply_markup"] = {"inline_keyboard": keyboard}
    elif reply_keyboard:
        payload["reply_markup"] = reply_keyboard
    elif remove_keyboard:
        payload["reply_markup"] = {"remove_keyboard": True}
    return tg("sendMessage", payload)


def answer(callback_id: str, text: str = "", alert: bool = False) -> None:
    tg("answerCallbackQuery", {"callback_query_id": callback_id, "text": text, "show_alert": alert})


PHONE_KEYBOARD = {
    "keyboard": [[{"text": "📱 Raqamimni yuborish", "request_contact": True}]],
    "resize_keyboard": True,
    "one_time_keyboard": True,
}


def is_admin(tg_id: int) -> bool:
    return tg_id in ADMIN_IDS


def notify_admins(text: str) -> None:
    for admin_id in ADMIN_IDS:
        send(admin_id, text)


# --------------------------------------------------------------------------
# Suhbat holati
# --------------------------------------------------------------------------

def get_state(tg_id: int) -> dict | None:
    return query_one("SELECT * FROM states WHERE tg_id = ?", (tg_id,))


def set_state(tg_id: int, step: str, **patch: Any) -> None:
    current = get_state(tg_id) or {}
    slot_id = patch.get("slot_id", current.get("slot_id"))
    first_name = patch.get("first_name", current.get("first_name"))
    last_name = patch.get("last_name", current.get("last_name"))
    execute(
        "INSERT INTO states (tg_id, step, slot_id, first_name, last_name, updated_at)"
        " VALUES (?, ?, ?, ?, ?, ?)"
        " ON CONFLICT (tg_id) DO UPDATE SET step = EXCLUDED.step, slot_id = EXCLUDED.slot_id,"
        " first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name,"
        " updated_at = EXCLUDED.updated_at",
        (tg_id, step, slot_id, first_name, last_name, now_local()),
    )


def clear_state(tg_id: int) -> None:
    execute("DELETE FROM states WHERE tg_id = ?", (tg_id,))


# --------------------------------------------------------------------------
# Vaqtlar va yozuvlar
# --------------------------------------------------------------------------

# Har bir vaqt yoniga nechta odam yozilganini qo'shib beradi
WITH_TAKEN = (
    "SELECT s.*, COALESCE(c.n, 0) AS taken FROM slots s"
    " LEFT JOIN (SELECT slot_id, COUNT(*) AS n FROM bookings GROUP BY slot_id) c"
    " ON c.slot_id = s.id"
)


def free_slots() -> list[dict]:
    """Hali joyi bor va o'tib ketmagan vaqtlar."""
    return query(
        WITH_TAKEN + " WHERE s.starts_at > ? AND COALESCE(c.n, 0) < s.capacity"
        " ORDER BY s.starts_at LIMIT 40",
        (now_local(),),
    )


def upcoming_slots() -> list[dict]:
    """Admin uchun: barcha kelgusi vaqtlar, to'lganlari ham."""
    return query(
        WITH_TAKEN + " WHERE s.starts_at > ? ORDER BY s.starts_at LIMIT 50",
        (now_local(),),
    )


def get_slot(slot_id: int) -> dict | None:
    return query_one(WITH_TAKEN + " WHERE s.id = ?", (slot_id,))


def slot_places_left(slot: dict) -> int:
    return max(0, int(slot.get("capacity") or SLOT_CAPACITY) - int(slot.get("taken") or 0))


def already_booked(slot_id: int, tg_id: int) -> bool:
    return query_one(
        "SELECT id FROM bookings WHERE slot_id = ? AND tg_id = ?", (slot_id, tg_id)
    ) is not None


def slot_members(slot_id: int) -> list[dict]:
    return query("SELECT * FROM bookings WHERE slot_id = ? ORDER BY id", (slot_id,))


def create_booking(slot_id: int, tg_id: int, username: str,
                   first_name: str, last_name: str, phone: str) -> tuple[int, str]:
    """Yozuv qo'shadi. Natija: (booking_id, xato matni).

    Joylarni sanash va yozish bitta tranzaksiyada bajariladi, aks holda ikki
    kishi bir vaqtda oxirgi joyni olib qo'yishi mumkin edi.
    """
    with _conn() as connection:
        if IS_PG:
            # Shu vaqt qatorini qulflaymiz — parallel so'rovlar navbatga turadi
            row = connection.execute(
                "SELECT capacity FROM slots WHERE id = %s FOR UPDATE", (slot_id,)
            ).fetchone()
        else:
            row = connection.execute(
                "SELECT capacity FROM slots WHERE id = ?", (slot_id,)
            ).fetchone()
        if row is None:
            return 0, "Bu vaqt o'chirib yuborilgan"
        capacity = int(dict(row)["capacity"] or SLOT_CAPACITY)

        taken = int(dict(connection.execute(
            _sql("SELECT COUNT(*) AS n FROM bookings WHERE slot_id = ?"), (slot_id,)
        ).fetchone())["n"])
        if taken >= capacity:
            return 0, "Afsus, bu vaqtda joy qolmadi"

        mine = connection.execute(
            _sql("SELECT id FROM bookings WHERE slot_id = ? AND tg_id = ?"), (slot_id, tg_id)
        ).fetchone()
        if mine is not None:
            return 0, "Siz bu vaqtga allaqachon yozilgansiz"

        statement = ("INSERT INTO bookings (slot_id, tg_id, username, first_name,"
                     " last_name, phone, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
        params = (slot_id, tg_id, username, first_name, last_name, phone, now_local())
        if IS_PG:
            new = connection.execute(_sql(statement) + " RETURNING id", params).fetchone()
            return int(dict(new)["id"]), ""
        cursor = connection.execute(statement, params)
        return int(cursor.lastrowid or 0), ""


def upcoming_bookings() -> list[dict]:
    return query(
        "SELECT b.*, s.label, s.starts_at FROM bookings b JOIN slots s ON s.id = b.slot_id"
        " WHERE s.starts_at > ? ORDER BY s.starts_at LIMIT 50",
        (now_local(),),
    )


def bookings_for_day(day: str) -> list[dict]:
    return query(
        "SELECT b.*, s.label, s.starts_at FROM bookings b JOIN slots s ON s.id = b.slot_id"
        " WHERE s.starts_at LIKE ? ORDER BY s.starts_at",
        (f"{day}%",),
    )


def user_bookings(tg_id: int) -> list[dict]:
    return query(
        "SELECT b.*, s.label, s.starts_at FROM bookings b JOIN slots s ON s.id = b.slot_id"
        " WHERE b.tg_id = ? AND s.starts_at > ? ORDER BY s.starts_at",
        (tg_id, now_local()),
    )


def get_booking(booking_id: int) -> dict | None:
    return query_one(
        "SELECT b.*, s.label FROM bookings b JOIN slots s ON s.id = b.slot_id WHERE b.id = ?",
        (booking_id,),
    )


def delete_booking(booking_id: int) -> None:
    execute("DELETE FROM bookings WHERE id = ?", (booking_id,))


# --------------------------------------------------------------------------
# Foydalanuvchi oqimi
# --------------------------------------------------------------------------

def show_intro(chat_id: int) -> None:
    send(
        chat_id,
        get_setting("info_text", DEFAULT_INFO),
        keyboard=[[{"text": "📝 Ro'yxatdan o'tish", "callback_data": "reg"}]],
    )


def show_slots(chat_id: int) -> None:
    slots = free_slots()
    if not slots:
        send(chat_id, "Hozircha bo'sh vaqt yo'q 😔\n\nAdmin yangi vaqtlar qo'shgach qayta urinib ko'ring.")
        return
    keyboard = [
        [{"text": f"🕐 {s['label']} — {slot_places_left(s)} joy",
          "callback_data": f"slot:{s['id']}"}]
        for s in slots
    ]
    send(chat_id, "Qaysi vaqtga yozilasiz?\n\n👇 Joyi bor vaqtlar:", keyboard=keyboard)


def pick_slot(chat_id: int, tg_id: int, slot_id: int) -> str:
    slot = get_slot(slot_id)
    if not slot:
        return "Bu vaqt topilmadi"
    if slot["starts_at"] <= now_local():
        return "Bu vaqt o'tib ketgan"
    if already_booked(slot_id, tg_id):
        return "Siz bu vaqtga allaqachon yozilgansiz"
    places = slot_places_left(slot)
    if places <= 0:
        return "Afsus, bu vaqtda joy qolmadi"

    set_state(tg_id, "ism", slot_id=slot_id, first_name=None, last_name=None)
    send(
        chat_id,
        f"✅ Tanlandi: <b>{esc(slot['label'])}</b>\n"
        f"Qolgan joy: {places} ta\n\n1/3 — <b>Ismingizni</b> yozing:",
        remove_keyboard=True,
    )
    return ""


def clean_name(value: str) -> str | None:
    name = " ".join(value.split())
    if len(name) < 2 or len(name) > 50 or name.startswith("/"):
        return None
    return name


def clean_phone(value: str) -> str | None:
    phone = re.sub(r"[^\d+]", "", value)
    digits = re.sub(r"\D", "", phone)
    if not 7 <= len(digits) <= 15:
        return None
    return phone


def handle_step(chat_id: int, user: dict, step: str, text: str) -> None:
    tg_id = user["id"]

    if step == "ism":
        name = clean_name(text)
        if not name:
            send(chat_id, "Ismingizni to'g'ri yozing (kamida 2 harf):")
            return
        set_state(tg_id, "familiya", first_name=name)
        send(chat_id, "2/3 — <b>Familiyangizni</b> yozing:")
        return

    if step == "familiya":
        name = clean_name(text)
        if not name:
            send(chat_id, "Familiyangizni to'g'ri yozing (kamida 2 harf):")
            return
        set_state(tg_id, "telefon", last_name=name)
        send(
            chat_id,
            "3/3 — <b>Telefon raqamingizni</b> yozing yoki pastdagi tugmani bosing:",
            reply_keyboard=PHONE_KEYBOARD,
        )
        return

    if step == "telefon":
        phone = clean_phone(text)
        if not phone:
            send(chat_id, "Raqamni to'g'ri yozing, masalan: +998901234567", reply_keyboard=PHONE_KEYBOARD)
            return

        state = get_state(tg_id)
        if not state or not state.get("slot_id") or not state.get("first_name") or not state.get("last_name"):
            clear_state(tg_id)
            send(chat_id, "Nimadir chalkashdi. /start bosib qaytadan boshlang.", remove_keyboard=True)
            return

        slot = get_slot(int(state["slot_id"]))
        if not slot:
            clear_state(tg_id)
            send(chat_id, "Bu vaqt o'chirib yuborilgan. /start bosib boshqasini tanlang.", remove_keyboard=True)
            return

        try:
            booking_id, problem = create_booking(
                int(state["slot_id"]), tg_id, user.get("username") or "",
                state["first_name"], state["last_name"], phone,
            )
        except Exception as error:  # noqa: BLE001 - baza bilan bog'liq kutilmagan nosozlik
            log.exception("Yozuv yaratilmadi: %s", error)
            booking_id, problem = 0, "Texnik nosozlik yuz berdi"

        if problem:
            clear_state(tg_id)
            send(chat_id, f"😔 {problem}.\n/start bosib boshqa vaqtni tanlang.", remove_keyboard=True)
            return

        clear_state(tg_id)
        after = get_slot(int(state["slot_id"])) or slot
        send(
            chat_id,
            "✅ <b>Ro'yxatdan o'tdingiz!</b>\n\n"
            f"🧾 Raqam: <b>#{booking_id}</b>\n"
            f"👤 {esc(state['first_name'])} {esc(state['last_name'])}\n"
            f"📞 {esc(phone)}\n"
            f"🕐 {esc(slot['label'])}\n\n"
            "Belgilangan vaqtda keling. Fikringiz o'zgarsa — /bekor buyrug'i bilan bekor qiling.",
            remove_keyboard=True,
        )
        notify_admins(
            f"🆕 <b>Yangi yozuv #{booking_id}</b>\n\n"
            f"👤 {esc(state['first_name'])} {esc(state['last_name'])}\n"
            f"📞 {esc(phone)}\n"
            f"🕐 {esc(slot['label'])}\n"
            f"👥 Shu vaqtda: {after.get('taken', 0)}/{after.get('capacity', SLOT_CAPACITY)}\n"
            f"💬 {'@' + esc(user['username']) if user.get('username') else 'username yoʻq'} (ID {tg_id})"
        )


def show_my_bookings(chat_id: int, tg_id: int) -> None:
    rows = user_bookings(tg_id)
    if not rows:
        send(chat_id, "Sizda faol yozuv yo'q.\n/start bosib ro'yxatdan o'ting.")
        return
    text = "\n\n".join(
        f"🧾 #{r['id']} — {esc(r['label'])}\n👤 {esc(r['first_name'])} {esc(r['last_name'])}" for r in rows
    )
    keyboard = [[{"text": f"❌ {r['label']}", "callback_data": f"cancel:{r['id']}"}] for r in rows]
    send(chat_id, f"📋 <b>Sizning yozuvlaringiz</b>\n\n{text}\n\nBekor qilish uchun bosing:", keyboard=keyboard)


# --------------------------------------------------------------------------
# Admin
# --------------------------------------------------------------------------

def admin_menu() -> list:
    return [
        [{"text": "➕ Vaqt qo'shish", "callback_data": "a:add"}],
        [{"text": "🗓 Vaqtlar ro'yxati", "callback_data": "a:slots"}],
        [{"text": "📋 Yozilganlar", "callback_data": "a:list"}],
        [{"text": "✏️ Boshlang'ich matnni o'zgartirish", "callback_data": "a:text"}],
    ]


def format_bookings(rows: list[dict], title: str) -> str:
    if not rows:
        return f"{title}\n\nHech kim yozilmagan."
    body = "\n\n".join(
        f"{i + 1}. <b>{esc(r['first_name'])} {esc(r['last_name'])}</b>\n"
        f"   📞 {esc(r['phone'])}\n"
        f"   🕐 {esc(r['label'])}\n"
        f"   🧾 #{r['id']}" + (f" · @{esc(r['username'])}" if r.get("username") else "")
        for i, r in enumerate(rows)
    )
    return f"{title} — <b>{len(rows)} ta</b>\n\n{body}"


def admin_save_slots(chat_id: int, tg_id: int, text: str) -> None:
    added, skipped, errors = [], [], []
    for line in (l.strip() for l in text.splitlines()):
        if not line:
            continue
        parsed = parse_slot_line(line)
        if isinstance(parsed, str):
            errors.append(parsed)
            continue
        label, starts_at, ends_at, capacity = parsed
        if query_one("SELECT id FROM slots WHERE starts_at = ? AND ends_at = ?", (starts_at, ends_at)):
            skipped.append(label)
            continue
        insert(
            "INSERT INTO slots (label, starts_at, ends_at, capacity, created_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (label, starts_at, ends_at, capacity, now_local()),
        )
        added.append(f"{label} — {capacity} joy")

    clear_state(tg_id)
    parts = []
    if added:
        parts.append(f"✅ Qo'shildi ({len(added)}):\n" + "\n".join(f"• {esc(x)}" for x in added))
    if skipped:
        parts.append(f"⏭ Allaqachon bor ({len(skipped)}):\n" + "\n".join(f"• {esc(x)}" for x in skipped))
    if errors:
        parts.append("⚠️ Tushunilmadi:\n" + "\n".join(f"• {esc(x)}" for x in errors))
    send(chat_id, "\n\n".join(parts) or "Hech narsa qo'shilmadi.", keyboard=admin_menu())


def admin_show_slots(chat_id: int) -> None:
    rows = upcoming_slots()
    if not rows:
        send(chat_id, "Kelgusi vaqtlar yo'q. ➕ bilan qo'shing.", keyboard=admin_menu())
        return
    lines = []
    for row in rows:
        taken = int(row.get("taken") or 0)
        capacity = int(row.get("capacity") or SLOT_CAPACITY)
        mark = "🔴" if taken >= capacity else ("🟡" if taken else "🟢")
        lines.append(f"{mark} {esc(row['label'])} — {taken}/{capacity}")
        for member in slot_members(int(row["id"])):
            lines.append(f"     • {esc(member['first_name'])} {esc(member['last_name'])}"
                         f" · {esc(member['phone'])}")
    keyboard = [[{"text": f"🗑 {r['label']}", "callback_data": f"a:del:{r['id']}"}] for r in rows[:25]]
    keyboard.append([{"text": "⬅️ Admin menyu", "callback_data": "a:menu"}])
    send(chat_id, f"🗓 <b>Kelgusi vaqtlar ({len(rows)})</b>\n\n" + "\n".join(lines) +
         "\n\nO'chirish uchun bosing:", keyboard=keyboard)


def admin_delete_slot(chat_id: int, slot_id: int) -> None:
    slot = get_slot(slot_id)
    if not slot:
        send(chat_id, "Bu vaqt allaqachon o'chirilgan.", keyboard=admin_menu())
        return
    members = slot_members(slot_id)
    execute("DELETE FROM bookings WHERE slot_id = ?", (slot_id,))
    execute("DELETE FROM slots WHERE id = ?", (slot_id,))
    for member in members:
        send(int(member["tg_id"]),
             f"⚠️ Afsus, <b>{esc(slot['label'])}</b> vaqtidagi yozuvingiz bekor qilindi.\n"
             "Iltimos, /start bosib boshqa vaqtni tanlang.")
    send(chat_id, f"🗑 O'chirildi: {esc(slot['label'])}" +
         (f"\n({len(members)} ta yozilgan odamga xabar berildi)" if members else ""),
         keyboard=admin_menu())


def admin_show_bookings(chat_id: int) -> None:
    rows = upcoming_bookings()
    keyboard = [
        [{"text": f"❌ #{r['id']} {r['first_name']} {r['last_name']}", "callback_data": f"a:unbook:{r['id']}"}]
        for r in rows[:25]
    ]
    keyboard.append([{"text": "⬅️ Admin menyu", "callback_data": "a:menu"}])
    send(chat_id, format_bookings(rows, "📋 <b>Yozilganlar</b>"), keyboard=keyboard)


def admin_cancel_booking(chat_id: int, booking_id: int) -> None:
    row = get_booking(booking_id)
    if not row:
        send(chat_id, "Bu yozuv topilmadi.", keyboard=admin_menu())
        return
    delete_booking(booking_id)
    send(int(row["tg_id"]),
         f"⚠️ <b>{esc(row['label'])}</b> vaqtidagi yozuvingiz admin tomonidan bekor qilindi.")
    send(chat_id, f"❌ Bekor qilindi: #{row['id']} {esc(row['first_name'])} {esc(row['last_name'])}"
                  f" — {esc(row['label'])}\n(vaqt yana bo'shadi)", keyboard=admin_menu())


def daily_report() -> None:
    today = today_local()
    text = (
        format_bookings(bookings_for_day(today), f"📅 <b>Bugun ({today})</b>")
        + "\n\n━━━━━━━━━━\n\n"
        + format_bookings(upcoming_bookings()[:15], "⏭ <b>Kelgusi yozuvlar</b>")
    )
    slots = free_slots()
    places = sum(slot_places_left(s) for s in slots)
    text += f"\n\n🟢 Joyi bor vaqtlar: {len(slots)} ta ({places} ta bo'sh joy)"
    notify_admins(text)


# --------------------------------------------------------------------------
# Yangilanishlarni qayta ishlash
# --------------------------------------------------------------------------

HELP = ("Buyruqlar:\n"
        "/start — xona haqida ma'lumot va ro'yxatdan o'tish\n"
        "/vaqtlar — bo'sh vaqtlarni ko'rish\n"
        "/bekor — o'z yozuvimni ko'rish va bekor qilish\n"
        "/id — Telegram ID'mni bilish")


def handle_message(message: dict) -> None:
    user = message.get("from") or {}
    if not user or user.get("is_bot"):
        return
    chat_id = message["chat"]["id"]
    tg_id = user["id"]
    contact = message.get("contact") or {}
    text = (contact.get("phone_number") or message.get("text") or "").strip()
    admin = is_admin(tg_id)

    if text.startswith("/"):
        command = re.split(r"[\s@]", text)[0].lower()
        if command == "/start":
            clear_state(tg_id)
            show_intro(chat_id)
        elif command == "/vaqtlar":
            show_slots(chat_id)
        elif command in ("/bekor", "/mening"):
            show_my_bookings(chat_id, tg_id)
        elif command == "/id":
            send(chat_id, f"Sizning Telegram ID: <code>{tg_id}</code>")
        elif command == "/admin":
            clear_state(tg_id)
            if admin:
                send(chat_id, "🛠 <b>Admin panel</b>", keyboard=admin_menu())
            else:
                send(chat_id, "Bu bo'lim faqat admin uchun.")
        elif command in ("/help", "/yordam"):
            send(chat_id, HELP + ("\n/admin — admin panel" if admin else ""))
        else:
            send(chat_id, "Bunday buyruq yo'q.\n\n" + HELP)
        return

    state = get_state(tg_id)
    step = state["step"] if state else ""

    if step == "admin_slot" and admin:
        admin_save_slots(chat_id, tg_id, message.get("text") or "")
    elif step == "admin_text" and admin:
        set_setting("info_text", (message.get("text") or "").strip())
        clear_state(tg_id)
        send(chat_id, "✅ Matn yangilandi. /start bosib ko'ring.", keyboard=admin_menu())
    elif step in ("ism", "familiya", "telefon"):
        handle_step(chat_id, user, step, text)
    else:
        show_intro(chat_id)


def handle_callback(callback: dict) -> None:
    data = callback.get("data") or ""
    user = callback["from"]
    tg_id = user["id"]
    message = callback.get("message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    if not chat_id:
        answer(callback["id"])
        return

    if data == "reg":
        answer(callback["id"])
        show_slots(chat_id)
        return

    if data.startswith("slot:"):
        problem = pick_slot(chat_id, tg_id, int(data[5:]))
        answer(callback["id"], problem, bool(problem))
        if problem:
            show_slots(chat_id)
        return

    if data.startswith("cancel:"):
        row = get_booking(int(data[7:]))
        if not row or int(row["tg_id"]) != tg_id:
            answer(callback["id"], "Topilmadi")
            return
        delete_booking(int(row["id"]))
        answer(callback["id"], "Bekor qilindi")
        send(chat_id, f"❌ Yozuvingiz bekor qilindi: {esc(row['label'])}")
        notify_admins(f"❌ <b>Bekor qilindi</b>\n#{row['id']} {esc(row['first_name'])} "
                      f"{esc(row['last_name'])} — {esc(row['label'])}")
        return

    if data.startswith("a:"):
        answer(callback["id"])
        if not is_admin(tg_id):
            send(chat_id, "Bu bo'lim faqat admin uchun.")
            return
        action = data[2:]
        if action == "menu":
            send(chat_id, "🛠 <b>Admin panel</b>", keyboard=admin_menu())
        elif action == "add":
            set_state(tg_id, "admin_slot")
            send(chat_id,
                 "➕ <b>Vaqt qo'shish</b>\n\n"
                 "Vaqtlarni yozing. Har bir qator — bitta vaqt oralig'i:\n\n"
                 "<code>15.10 14:00-16:00\n15.10 16:00-18:00\n16.10 09:00-11:00</code>\n\n"
                 f"Har bir vaqtga <b>{SLOT_CAPACITY} ta joy</b> ajratiladi.\n"
                 "Boshqacha bo'lsa, oxiriga qo'shing:\n\n"
                 "<code>15.10 14:00-16:00 x5</code>\n\n"
                 "Yilni yozish shart emas. Bekor qilish uchun /admin bosing.")
        elif action == "slots":
            admin_show_slots(chat_id)
        elif action == "list":
            admin_show_bookings(chat_id)
        elif action == "text":
            set_state(tg_id, "admin_text")
            send(chat_id, "✏️ Hozirgi matn quyida. Yangisini to'liq yozib yuboring.\n"
                          "<b>qalin</b>, <i>qiya</i> uchun HTML teglari ishlaydi.\n"
                          "Bekor qilish uchun /admin bosing.")
            send(chat_id, get_setting("info_text", DEFAULT_INFO))
        elif action.startswith("del:"):
            admin_delete_slot(chat_id, int(action[4:]))
        elif action.startswith("unbook:"):
            admin_cancel_booking(chat_id, int(action[7:]))
        return

    answer(callback["id"])


# --------------------------------------------------------------------------
# Veb-server
# --------------------------------------------------------------------------

app = Flask(__name__)


@app.get("/")
def health():
    """UptimeRobot shu manzilni so'raydi — servis uxlab qolmaydi."""
    return f"combot tirik · {now_local()}", 200


@app.post(f"/webhook/{WEBHOOK_SECRET}")
def webhook():
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        return "forbidden", 403
    update = request.get_json(silent=True) or {}
    try:
        if "message" in update:
            handle_message(update["message"])
        elif "callback_query" in update:
            handle_callback(update["callback_query"])
    except Exception:  # noqa: BLE001 - Telegram qayta yubormasligi uchun 200 qaytaramiz
        log.exception("Update xatosi")
    return "ok", 200


@app.get(f"/setup/{WEBHOOK_SECRET}")
def setup():
    """Brauzerda bir marta ochiladi — webhookni Telegramga ro'yxatdan o'tkazadi."""
    base = request.url_root.rstrip("/").replace("http://", "https://")
    result = tg("setWebhook", {
        "url": f"{base}/webhook/{WEBHOOK_SECRET}",
        "secret_token": WEBHOOK_SECRET,
        "allowed_updates": ["message", "callback_query"],
        "drop_pending_updates": True,
    })
    return jsonify(result)


def report_loop() -> None:
    """Har kuni REPORT_HOUR da adminlarga ro'yxat yuboradi."""
    while True:
        try:
            stamp = now_local()
            if int(stamp[11:13]) == REPORT_HOUR and get_setting("last_report", "") != stamp[:10]:
                set_setting("last_report", stamp[:10])
                daily_report()
                log.info("Kunlik hisobot yuborildi")
        except Exception:  # noqa: BLE001 - sikl to'xtamasin
            log.exception("Kunlik hisobot xatosi")
        time.sleep(300)


def bootstrap() -> None:
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN o'rnatilmagan")
    if not ADMIN_IDS:
        log.warning("ADMIN_IDS bo'sh — admin paneliga hech kim kira olmaydi")
    init_db()
    threading.Thread(target=report_loop, daemon=True).start()


bootstrap()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
