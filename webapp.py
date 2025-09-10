#!/usr/bin/env python3
from http.server import BaseHTTPRequestHandler, HTTPServer
import os, sqlite3, urllib.parse, datetime, json
from jinja2 import Environment, FileSystemLoader

# =========================
# [setup] Konfiguration
# =========================
DB_PATH = os.environ.get("SCHOOL_DB_PATH", os.path.join(os.getcwd(), "school.db"))
LESSON_MINUTES = int(os.environ.get("LESSON_MINUTES", "45"))  # für Fehlstunden→Minuten

# Jinja2 Template-Setup
template_env = Environment(loader=FileSystemLoader(os.path.join(os.getcwd(), "templates")), autoescape=True)

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# =========================
# Helper / Utilities
# =========================

# Stundenplan-Raster
SCHEDULE_PATTERN = [
    {'period': 1, 'label': '1- 08:00-08:45'},
    {'period': 2, 'label': '2- 08:45-09:30'},
    {'break': 25, 'label': 'Pause 25min'},
    {'period': 3, 'label': '3- 09:55-10:40'},
    {'period': 4, 'label': '4- 10:40-11:25'},
    {'break': 20, 'label': 'Pause 20min'},
    {'period': 5, 'label': '5- 11:45-12:30'},
    {'period': 6, 'label': '6- 12:30-13:15'},
    {'break': 45, 'label': 'Pause 45min'},
    {'period': 8, 'label': '8- 14:00-14:45'},
    {'period': 9, 'label': '9- 14:45-15:30'},
]

def html_escape(text: str) -> str:
    from html import escape as _e
    if text is None:
        return ""
    return _e(str(text), quote=True).replace("'", "&#39;")

def _normalize_group_name(name: str) -> str:
    """
    Normalize a class or course name by uppercasing the letter portion.
    Example: '10f' -> '10F', '7sw' -> '7SW'.
    """
    name = (name or "").strip()
    if not name:
        return name
    import re
    m = re.match(r"^(\d+)([A-Za-z]+)$", name.strip())
    if m:
        grade, suffix = m.group(1), m.group(2)
        return grade + suffix.upper()
    return ''.join(ch.upper() if ch.isalpha() else ch for ch in name)

def get_all_classes(cur):
    return cur.execute("SELECT id, name FROM classes ORDER BY name ASC").fetchall()

def get_all_courses(cur):
    return cur.execute("SELECT id, name FROM courses ORDER BY name ASC").fetchall()

def get_all_teachers(cur):
    return cur.execute("SELECT id, short, name FROM teachers ORDER BY short ASC").fetchall()

def get_all_subjects(cur):
    return cur.execute("SELECT id, name, short FROM subjects ORDER BY name ASC").fetchall()

def get_or_create_class(cur, name: str):
    name = (name or "").strip()
    if not name:
        return None
    norm_name = _normalize_group_name(name)
    row = cur.execute("SELECT id FROM classes WHERE LOWER(name)=LOWER(?)", (norm_name,)).fetchone()
    if row:
        return row["id"]
    cur.execute("INSERT INTO classes(name) VALUES(?)", (norm_name,))
    return cur.lastrowid

def get_or_create_course(cur, name: str):
    name = (name or "").strip()
    if not name:
        return None
    norm_name = _normalize_group_name(name)
    row = cur.execute("SELECT id FROM courses WHERE LOWER(name)=LOWER(?)", (norm_name,)).fetchone()
    if row:
        return row["id"]
    cur.execute("INSERT INTO courses(name, class_id) VALUES(?, NULL)", (norm_name,))
    return cur.lastrowid

def create_teacher(cur, short: str, name: str):
    short = (short or "").strip()
    name  = (name or "").strip()
    if not short and not name:
        return None
    if not short and name:
        short = name.split()[0][:4].upper()
    if not name and short:
        name = short
    row = cur.execute("SELECT id FROM teachers WHERE short=?", (short,)).fetchone()
    if row:
        if name:
            cur.execute("UPDATE teachers SET name=COALESCE(NULLIF(name,''),?) WHERE id=?", (name, row["id"]))
        return row["id"]
    cur.execute("INSERT INTO teachers(short, name) VALUES(?,?)", (short, name))
    return cur.lastrowid

def get_or_create_subject(cur, name: str, short: str = None):
    name = (name or "").strip()
    if not name:
        return None
    row = cur.execute("SELECT id FROM subjects WHERE LOWER(name)=LOWER(?)", (name,)).fetchone()
    if row:
        return row["id"]
    short_norm = None
    if short:
        short_norm = short.strip().upper() or None
    if not short_norm:
        short_norm = name.strip().upper()[:8]
    cur.execute("INSERT INTO subjects(name, short) VALUES(?, ?)", (name, short_norm))
    return cur.lastrowid

def populate_default_timetable(cur):
    """
    Wöchentlicher Defaultplan (date=NULL). Siehe ursprüngliche Beispiel-Daten.
    """
    entries = [
        # Montag
        ("Montag", 1, "5f", "PFAG", "212", True),
        ("Montag", 3, "7sw", "PH", "136", False),
        ("Montag", 4, "7ch", "PH", "136", False),
        ("Montag", 5, "6b", "IF", "311", True),
        ("Montag", 8, "8fs", "PH", "239", False),
        # Dienstag
        ("Dienstag", 1, "10f", "AS", "con4", False),
        ("Dienstag", 2, "9if", "PH", "136", False),
        ("Dienstag", 3, "8if", "IF", "212", True),
        ("Dienstag", 5, "10f", "M", "con4", True),
        # Mittwoch
        ("Mittwoch", 1, "7bi", "PH", "239", False),
        ("Mittwoch", 2, "7if", "PH", "232", False),
        ("Mittwoch", 4, "8if", "IF", "212", False),
        # Donnerstag
        ("Donnerstag", 3, "10sw", "PH", "239", False),
        ("Donnerstag", 4, "10f", "AS", "con4", False),
        ("Donnerstag", 5, "10f", "M", "con4", True),
        ("Donnerstag", 8, "6e", "IF", "212", True),
        # Freitag
        ("Freitag", 1, "9tc", "PH", "239", False),
        ("Freitag", 3, "9if", "PH", "236", False),
        ("Freitag", 5, "7ch", "PH", "239", False),
        ("Freitag", 6, "7if", "PH", "232", False),
    ]

    subject_map = {
        'PFAG': ('PFAG', 'PFAG'),
        'AS': ('AS', 'AS'),
        'PH': ('Physik', 'PH'),
        'IF': ('IF', 'IF'),
        'M': ('M', 'M'),
    }
    import re
    for day, period, group_name, subj_token, room, is_double in entries:
        group_norm = _normalize_group_name(group_name)
        m = re.match(r"^(\d+)([A-Za-z]+)$", group_norm)
        class_id = None
        course_id = None
        if m:
            grade, suffix = m.group(1), m.group(2)
            if len(suffix) == 1:
                class_id = get_or_create_class(cur, group_norm)
            else:
                course_id = get_or_create_course(cur, group_norm)
                class_id = get_or_create_class(cur, group_norm)
        else:
            class_id = get_or_create_class(cur, group_norm)

        if subj_token in subject_map:
            subj_name, subj_short = subject_map[subj_token]
        else:
            subj_name, subj_short = subj_token, subj_token
        subj_id = get_or_create_subject(cur, subj_name, subj_short)

        cur.execute(
            "INSERT INTO timetable(class_id, course_id, period, is_double, slot, day, time_range, date, subject_id, room, status) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                class_id,
                course_id,
                period,
                1 if is_double else 0,
                period,
                day,
                None,
                None,
                subj_id,
                room,
                None,
            ),
        )

# =========================
# Schema Migration
# =========================
def ensure_schema_migrations():
    conn = get_db_connection()
    cur = conn.cursor()

    # ---- Basistabellen
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS teachers(
      id INTEGER PRIMARY KEY,
      short TEXT UNIQUE,
      name TEXT
    );
    CREATE TABLE IF NOT EXISTS classes(
      id INTEGER PRIMARY KEY,
      name TEXT UNIQUE NOT NULL,
      teacher_id INTEGER NULL REFERENCES teachers(id)
    );
    CREATE TABLE IF NOT EXISTS courses(
      id INTEGER PRIMARY KEY,
      name TEXT UNIQUE NOT NULL,
      class_id INTEGER NULL REFERENCES classes(id),
      leader_id INTEGER NULL REFERENCES teachers(id)
    );
    CREATE TABLE IF NOT EXISTS students(
      id INTEGER PRIMARY KEY,
      first_name TEXT NOT NULL,
      last_name  TEXT NOT NULL,
      class_id   INTEGER NOT NULL REFERENCES classes(id),
      course_id  INTEGER NULL REFERENCES courses(id)
    );
    CREATE TABLE IF NOT EXISTS attendance_records(
      id INTEGER PRIMARY KEY,
      student_id INTEGER NOT NULL REFERENCES students(id) ON DELETE CASCADE,
      date TEXT NOT NULL,
      status TEXT NOT NULL CHECK(status IN ('present','absent')),
      absent_minutes INTEGER NOT NULL DEFAULT 0,
      late_minutes   INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS grade_records(
      id INTEGER PRIMARY KEY,
      student_id INTEGER NOT NULL REFERENCES students(id) ON DELETE CASCADE,
      date TEXT NOT NULL,
      type TEXT NOT NULL CHECK(type IN ('performance','spontaneous')),
      subject TEXT,
      grade REAL
    );
    """)

    # Erweiterungen
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS subjects(
      id INTEGER PRIMARY KEY,
      name TEXT UNIQUE NOT NULL,
      short TEXT UNIQUE
    );
    CREATE TABLE IF NOT EXISTS class_subjects(
      class_id INTEGER NOT NULL REFERENCES classes(id) ON DELETE CASCADE,
      subject_id INTEGER NOT NULL REFERENCES subjects(id) ON DELETE CASCADE,
      PRIMARY KEY(class_id, subject_id)
    );
    CREATE TABLE IF NOT EXISTS course_subjects(
      course_id INTEGER NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
      subject_id INTEGER NOT NULL REFERENCES subjects(id) ON DELETE CASCADE,
      PRIMARY KEY(course_id, subject_id)
    );
    CREATE TABLE IF NOT EXISTS timetable(
      id INTEGER PRIMARY KEY,
      class_id INTEGER REFERENCES classes(id) ON DELETE CASCADE,
      period INTEGER NOT NULL,
      is_double INTEGER NOT NULL DEFAULT 0,
      slot INTEGER NOT NULL,
      day TEXT NOT NULL,
      time_range TEXT,
      date TEXT,
      status TEXT,
      subject_id INTEGER REFERENCES subjects(id),
      room TEXT,
      course_id INTEGER REFERENCES courses(id)
    );
    """)

    def col_exists(table, col):
        cur.execute(f"PRAGMA table_info({table})")
        return any(r["name"] == col for r in cur.fetchall())

    # ---- Notenschlüssel
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS grade_scales(
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      definition TEXT NOT NULL
    );
    """)

    # ---- Leistungsabfragen (zuerst anlegen, dann evtl. alter)
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS performance_queries(
        id INTEGER PRIMARY KEY,
        type TEXT NOT NULL,
        description TEXT,
        subject_id INTEGER REFERENCES subjects(id),
        class_id INTEGER REFERENCES classes(id),
        course_id INTEGER REFERENCES courses(id),
        date TEXT NOT NULL,
        grade_scale_id INTEGER REFERENCES grade_scales(id),
        max_op_points REAL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS performance_tasks(
        id INTEGER PRIMARY KEY,
        performance_id INTEGER NOT NULL REFERENCES performance_queries(id) ON DELETE CASCADE,
        number INTEGER NOT NULL,
        max_points REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS performance_results(
        id INTEGER PRIMARY KEY,
        performance_id INTEGER NOT NULL REFERENCES performance_queries(id) ON DELETE CASCADE,
        student_id INTEGER NOT NULL REFERENCES students(id) ON DELETE CASCADE,
        op_points REAL DEFAULT 0,
        zp_points REAL DEFAULT 0,
        grade_override REAL,
        comment TEXT,
        op_is_edited INTEGER DEFAULT 0,
        zp_is_edited INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS performance_task_results(
        id INTEGER PRIMARY KEY,
        performance_id INTEGER NOT NULL REFERENCES performance_queries(id) ON DELETE CASCADE,
        student_id INTEGER NOT NULL REFERENCES students(id) ON DELETE CASCADE,
        task_number INTEGER NOT NULL,
        points REAL NOT NULL,
        is_edited INTEGER DEFAULT 0
    );
    """)

    # ALTER TABLE-Befehle NACH dem Anlegen der Tabellen ausführen
    if not col_exists("attendance_records", "period"):
        cur.execute("ALTER TABLE attendance_records ADD COLUMN period INTEGER")
    if not col_exists("grade_records", "period"):
        cur.execute("ALTER TABLE grade_records ADD COLUMN period INTEGER")
    if not col_exists("grade_records", "comment"):
        cur.execute("ALTER TABLE grade_records ADD COLUMN comment TEXT")
    if not col_exists("performance_queries", "max_op_points"):
        cur.execute("ALTER TABLE performance_queries ADD COLUMN max_op_points REAL DEFAULT 0")
    if not col_exists("performance_results", "op_is_edited"):
        cur.execute("ALTER TABLE performance_results ADD COLUMN op_is_edited INTEGER DEFAULT 0")
    if not col_exists("performance_results", "zp_is_edited"):
        cur.execute("ALTER TABLE performance_results ADD COLUMN zp_is_edited INTEGER DEFAULT 0")
    if not col_exists("performance_task_results", "is_edited"):
        cur.execute("ALTER TABLE performance_task_results ADD COLUMN is_edited INTEGER DEFAULT 0")
    # ...entfernt, da diese SQL-Statements bereits korrekt oben stehen und hier Syntaxfehler verursachen...

    # subjects.short unique index (falls nötig)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_subjects_short ON subjects(short)")

    # ---- Default-Notenschlüssel nur anlegen, wenn Tabelle leer
    cur.execute("SELECT COUNT(*) FROM grade_scales")
    if (cur.fetchone()[0] or 0) == 0:
        # 0,5er-Schritte; Grenzen gemäß Wunsch: 86/72/58/44/20 (rechts-exklusiv),
        # feinere Staffel 1.0..6.0 in 0,5 Schritten, keine Überlappungen.
        default_def = "\n".join([
            "1.0;93.0;100.1",
            "1.5;86.0;93.0",
            "2.0;79.0;86.0",
            "2.5;72.0;79.0",
            "3.0;65.0;72.0",
            "3.5;58.0;65.0",
            "4.0;51.0;58.0",
            "4.5;44.0;51.0",
            "5.0;31.5;44.0",
            "5.5;19.0;31.5",
            "6.0;0.0;19.0",
        ])
        cur.execute("INSERT INTO grade_scales(name, definition) VALUES(?,?)",
                    ("Default (86/72/58/44/20, 0.5er)", default_def))

    # ---- Change Log
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS change_log(
      id INTEGER PRIMARY KEY,
      timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now')),
      action TEXT,
      table_name TEXT,
      record_id INTEGER,
      field_name TEXT,
      old_value TEXT,
      new_value TEXT,
      comment TEXT
    );
    """)

    # ---- Default-Fächer, falls leer
    cur.execute("SELECT COUNT(*) AS cnt FROM subjects")
    if (cur.fetchone()[0] or 0) == 0:
        default_subjects = [
            ("Deutsch", "DE"),
            ("Englisch", "EN"),
            ("Mathematik", "MA"),
            ("Physik", "PH"),
            ("Biologie", "BI"),
        ]
        for name, short in default_subjects:
            try:
                cur.execute("INSERT INTO subjects(name, short) VALUES(?, ?)", (name, short))
            except sqlite3.IntegrityError:
                pass

    # ---- Stundenplan-Defaults (nur wenn leer)
    cur.execute("SELECT COUNT(*) AS cnt FROM timetable")
    if cur.fetchone()[0] == 0:
        populate_default_timetable(cur)

    # ---- Default-Schüler (nur wenn leer)
    cur.execute("SELECT COUNT(*) AS cnt FROM students")
    if cur.fetchone()[0] == 0:
        populate_default_students(cur)

    conn.commit()
    conn.close()

def populate_default_students(cur):
    students = [
        ("John", "Smith", "5f"),
        ("Jane", "Doe", "5f"),
        ("Peter", "Jones", "6b"),
    ]
    for first_name, last_name, class_name in students:
        class_id = get_or_create_class(cur, class_name)
        if class_id:
            cur.execute(
                "INSERT INTO students(first_name, last_name, class_id) VALUES(?,?,?)",
                (first_name, last_name, class_id)
            )

# Schema sicherstellen (nachdem Helper definiert sind!)
ensure_schema_migrations()

# =========================
# HTTP Handler
# =========================
class SchoolHTTPRequestHandler(BaseHTTPRequestHandler):
    def _post_admin_timetable_delete(self):
        data = self._parse_post()
        tid = int(data.get('id', '0') or '0')
        if tid <= 0:
            return self._send_html("<h1>400</h1><p>Ungültige ID.</p>", status=400)
        conn = get_db_connection(); cur = conn.cursor()
        with conn:
            cur.execute("DELETE FROM timetable WHERE id=?", (tid,))
        conn.close()
        self._log_change('timetable', tid, 'delete', '', '', 'manual', None)
        self._redirect('/admin/timetable')

    NAV_BAR = """
    <nav>
        <a href="/">Home</a> |
        <a href="/classes">Klassen</a> |
        <a href="/courses">Kurse</a> |
        <a href="/students">Schüler</a> |
        <a href="/leistungsabfragen">Leistungsabfragen</a> |
        <a href="/grade_scales">Notenschlüssel</a> |
        <a href="/admin">Admin</a>
    </nav>
    <hr>
    """

    # ---------- HTTP helpers ----------
    def render(self, template_name, context={}):
        template = template_env.get_template(template_name)
        html = template.render(context)
        self._send_html(html)

    def _send_html(self, html: str, status: int = 200, headers: dict | None = None):
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        if headers:
            for k,v in headers.items():
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(html.encode("utf-8"))

    def _redirect(self, url: str):
        self.send_response(303)  # See Other
        self.send_header("Location", url)
        self.end_headers()

    def _send_json(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode("utf-8"))

    def _parse_json_post(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length == 0:
            return {}
        raw_data = self.rfile.read(length)
        try:
            return json.loads(raw_data)
        except json.JSONDecodeError:
            return {}

    def _parse_query(self):
        parsed = urllib.parse.urlparse(self.path)
        return parsed.path, urllib.parse.parse_qs(parsed.query)

    def _parse_post(self):
        ctype = self.headers.get("Content-Type","")
        length = int(self.headers.get("Content-Length","0") or "0")
        raw = self.rfile.read(length) if length>0 else b""
        if "application/x-www-form-urlencoded" in ctype or "multipart/form-data" in ctype:
            data = urllib.parse.parse_qs(raw.decode("utf-8"), keep_blank_values=True)
            return {k: v[0] for k,v in data.items()}
        return {}

    # ---------- GET ----------
    def do_GET(self):
        path, params = self._parse_query()
        try:
            if path == "/classes":
                self._handle_classes(params)
            elif path == "/courses":
                self._handle_courses(params)
            elif path == "/class":
                self._handle_class_detail(params)
            elif path == "/course":
                self._handle_course_detail(params)
            elif path == "/students":
                self._handle_students(params)
            elif path == "/student":
                self._handle_student_detail(params)
            elif path == "/admin":
                self._handle_admin(params)
            elif path == "/admin/subjects":
                self._handle_admin_subjects(params)
            elif path == "/admin/teachers":
                self._handle_admin_teachers(params)
            elif path == "/admin/classes":
                self._handle_admin_classes(params)
            elif path == "/admin/timetable":
                self._handle_admin_timetable(params)
            elif path == "/admin/timetable/edit":
                self._handle_admin_timetable_edit(params)
            elif path == "/admin/courses":
                self._handle_admin_courses(params)
            elif path == "/export_import":
                self._handle_export_import(params)
            elif path == "/performance":
                self._handle_performance_detail(params)
            elif path == "/leistung" or path == "/leistungsabfragen":
                self._handle_leistungsabfragen(params)
            elif path == "/performance/download":
                self._handle_performance_download(params)
            elif path == "/grade_scales":
                self._handle_grade_scales(params)
            elif path == "/admin/log":
                self._handle_admin_log(params)
            elif path == "/admin/attendance":
                self._handle_admin_attendance(params)
            elif path == "/capture_data":
                self._handle_capture_data(params)
            else:
                self._handle_home(params)
        except Exception as exc:
            # Fehler ins Log schreiben
            with open("server.log", "a", encoding="utf-8") as logf:
                import traceback
                logf.write(f"[{datetime.datetime.now()}] GET {self.path}\n")
                logf.write(traceback.format_exc())
                logf.write("\n")
            self._send_html(f"<h1>500</h1><pre>{html_escape(str(exc))}</pre>", status=500)

    # ---------- POST ----------
    def do_POST(self):
        path, _ = self._parse_query()
        try:
            if path == "/enroll/update":
                self._post_enroll_update()
            elif path == "/student/save":
                self._post_student_save()
            elif path == "/course/create":
                self._post_course_create()
            elif path == "/student/create":
                self._post_student_create()
            elif path == "/student/delete":
                self._post_student_delete()
            elif path == "/admin/subject/create":
                self._post_admin_subject_create()
            elif path == "/admin/subject/delete":
                self._post_admin_subject_delete()
            elif path == "/admin/teacher/create":
                self._post_admin_teacher_create()
            elif path == "/admin/teacher/delete":
                self._post_admin_teacher_delete()
            elif path == "/admin/timetable/create":
                self._post_admin_timetable_create()
            elif path == "/admin/timetable/update":
                self._post_admin_timetable_update()
            elif path == "/admin/timetable/delete":
                self._post_admin_timetable_delete()
            elif path == "/class/assign_teacher":
                self._post_class_assign_teacher()
            elif path == "/course/assign_leader":
                self._post_course_assign_leader()
            elif path == "/admin/course/create":
                self._post_admin_course_create()
            elif path == "/admin/course/delete":
                self._post_admin_course_delete()
            elif path == "/performance/create":
                self._post_performance_create()
            elif path == "/performance/import":
                self._post_performance_import()
            elif path == "/performance/delete":
                self._post_performance_delete()
            elif path == "/grade_scale/create":
                self._post_grade_scale_create()
            elif path == "/performance/assign_scale":
                self._post_assign_grade_scale()
            elif path == "/performance/update_override":
                self._post_update_grade_override()
            elif path == "/performance/update_student_scores":
                self._post_performance_update_student_scores()
            elif path == "/attendance/create":
                self._post_attendance_create()
            elif path == "/capture_data/save":
                self._post_capture_data_save()
            elif path == "/lesson/update_status":
                self._post_lesson_update_status()
            elif path == "/lesson/uncancel":
                self._post_lesson_uncancel()
            else:
                self._send_html("<h1>404</h1><p>Route nicht gefunden.</p>", status=404)
        except Exception as exc:
            # Fehler ins Log schreiben
            with open("server.log", "a", encoding="utf-8") as logf:
                import traceback
                logf.write(f"[{datetime.datetime.now()}] POST {self.path}\n")
                logf.write(traceback.format_exc())
                logf.write("\n")
            self._send_html(f"<h1>500</h1><pre>{html_escape(str(exc))}</pre>", status=500)
    # ---------- Views ----------
    def _handle_home(self, params):
        try:
            week_offset = int(params.get('week', ['0'])[0])
        except ValueError:
            week_offset = 0
        today = datetime.date.today()
        iso_year, iso_week, iso_weekday = today.isocalendar()
        monday = today - datetime.timedelta(days=iso_weekday - 1)
        start_monday = monday + datetime.timedelta(weeks=week_offset)
        iso_year2, iso_week2, _ = start_monday.isocalendar()
        kw = iso_week2
        if iso_week2 >= 35:
            school_start_year = iso_year2
        else:
            school_start_year = iso_year2 - 1
        try:
            school_start_monday = datetime.date.fromisocalendar(school_start_year, 35, 1)
        except Exception:
            school_start_monday = datetime.date(school_start_year, 9, 1)
        delta_days = (start_monday - school_start_monday).days
        weeks_since_start = delta_days // 7
        sw = (weeks_since_start % 6) + 1
        dates = [start_monday + datetime.timedelta(days=i) for i in range(5)]
        date_strs = [d.strftime('%Y-%m-%d') for d in dates]
        day_labels = ['Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag']

        conn = get_db_connection(); cur = conn.cursor()
        schedule_entries = {}
        for day_idx, date_str in enumerate(date_strs):
            day_name = day_labels[day_idx]
            prev_period = None
            period_list = [slot['period'] for slot in SCHEDULE_PATTERN if 'period' in slot]
            for slot in SCHEDULE_PATTERN:
                if 'period' not in slot:
                    continue
                period = slot['period']
                # 1. Suche nach Eintrag, der in dieser Periode startet
                row = cur.execute(
                    "SELECT t.id, t.class_id, c.name AS class_name, t.course_id, d.name AS course_name, "
                    "t.subject_id, s.short AS subject_short, s.name AS subject_name, t.room, t.is_double, t.status, t.day "
                    "FROM timetable t "
                    "LEFT JOIN classes c ON t.class_id=c.id "
                    "LEFT JOIN courses d ON t.course_id=d.id "
                    "LEFT JOIN subjects s ON t.subject_id=s.id "
                    "WHERE t.date=? AND t.period=?",
                    (date_str, period),
                ).fetchone()
                if not row:
                    row = cur.execute(
                        "SELECT t.id, t.class_id, c.name AS class_name, t.course_id, d.name AS course_name, "
                        "t.subject_id, s.short AS subject_short, s.name AS subject_name, t.room, t.is_double, t.status, t.day "
                        "FROM timetable t "
                        "LEFT JOIN classes c ON t.class_id=c.id "
                        "LEFT JOIN courses d ON t.course_id=d.id "
                        "LEFT JOIN subjects s ON t.subject_id=s.id "
                        "WHERE t.date IS NOT NULL AND t.date < ? AND t.day=? AND t.period=? "
                        "ORDER BY t.date DESC LIMIT 1",
                        (date_str, day_name, period),
                    ).fetchone()
                if not row:
                    row = cur.execute(
                        "SELECT t.id, t.class_id, c.name AS class_name, t.course_id, d.name AS course_name, "
                        "t.subject_id, s.short AS subject_short, s.name AS subject_name, t.room, t.is_double, t.status, t.day "
                        "FROM timetable t "
                        "LEFT JOIN classes c ON t.class_id=c.id "
                        "LEFT JOIN courses d ON t.course_id=d.id "
                        "LEFT JOIN subjects s ON t.subject_id=s.id "
                        "WHERE t.date IS NULL AND t.day=? AND t.period=?",
                        (day_name, period),
                    ).fetchone()
                # 2. Falls kein Eintrag, prüfe ob vorherige Periode eine Doppelstunde ist (und Tag passt)
                if not row and prev_period is not None:
                    prev_row = schedule_entries.get((day_idx, prev_period))
                    if prev_row and prev_row['is_double']:
                        # Tag muss passen!
                        if prev_row['day'] == day_name:
                            row = prev_row
                if row:
                    schedule_entries[(day_idx, period)] = row
                prev_period = period
        conn.close()

        # Prepare data for the template
        for key, entry in schedule_entries.items():
            # Convert the sqlite3.Row object to a mutable dictionary
            entry_dict = dict(entry)

            group_name = entry_dict['course_name'] or entry_dict['class_name']

            params = {
                'date': date_strs[key[0]],
                'period': key[1],
                'subject_id': entry_dict['subject_id'],
                'class_id': entry_dict['class_id'],
                'course_id': entry_dict['course_id'],
                'timetable_id': entry_dict['id'],
            }
            # Filter out None values
            params = {k: v for k, v in params.items() if v is not None}

            entry_dict['url'] = f"/capture_data?{urllib.parse.urlencode(params)}"

            subj = entry_dict['subject_short'] if entry_dict['subject_short'] else (entry_dict['subject_name'] if entry_dict['subject_name'] else '')
            room = entry_dict['room'] or ''
            parts = []
            if group_name: parts.append(group_name)
            if subj: parts.append(subj)
            if room: parts.append(room)
            entry_dict['label'] = ' - '.join(parts)

            # Replace the original Row object with the dictionary
            schedule_entries[key] = entry_dict

        current_day_idx = -1
        if week_offset == 0:
            current_day_idx = today.weekday()

        context = {
            "kw": kw,
            "sw": sw,
            "dates": dates,
            "day_labels": day_labels,
            "schedule_pattern": SCHEDULE_PATTERN,
            "schedule_entries": schedule_entries,
            "week_offset": week_offset,
            "current_day_idx": current_day_idx,
        }
        self.render("home.html", context)

    def _handle_capture_data(self, params):
        date = params.get('date', [None])[0]
        class_id = params.get('class_id', [None])[0]
        course_id = params.get('course_id', [None])[0]
        subject_id = params.get('subject_id', [None])[0]
        period = params.get('period', [None])[0]
        timetable_id = params.get('timetable_id', [None])[0]

        if not date or not subject_id or not period or not (class_id or course_id):
            return self._send_html("<h1>400</h1><p>Missing parameters: date, period, subject_id, and class_id/course_id are required.</p>", status=400)

        conn = get_db_connection(); cur = conn.cursor()

        timetable_entry = None
        if timetable_id:
            timetable_entry = cur.execute("SELECT * FROM timetable WHERE id = ?", (timetable_id,)).fetchone()

        group_name = ""
        students = []
        if course_id:
            course = cur.execute("SELECT name FROM courses WHERE id=?", (course_id,)).fetchone()
            if course:
                group_name = course['name']
            students = cur.execute("SELECT id, first_name, last_name FROM students WHERE course_id=? ORDER BY last_name, first_name", (course_id,)).fetchall()
        elif class_id:
            _class = cur.execute("SELECT name FROM classes WHERE id=?", (class_id,)).fetchone()
            if _class:
                group_name = _class['name']
            students = cur.execute("SELECT id, first_name, last_name FROM students WHERE class_id=? ORDER BY last_name, first_name", (class_id,)).fetchall()

        subject_row = cur.execute("SELECT name, short FROM subjects WHERE id=?", (subject_id,)).fetchone()
        subject_name = subject_row['name'] if subject_row else 'Unknown Subject'

        # Fetch existing data for this lesson
        existing_attendance_rows = cur.execute("SELECT student_id, status, absent_minutes, late_minutes FROM attendance_records WHERE date=? AND period=?", (date, period)).fetchall()
        existing_attendance = {}
        for r in existing_attendance_rows:
            d = dict(r)
            if (d['late_minutes'] or 0) > 0:
                d['status'] = 'late'
            existing_attendance[d['student_id']] = d
        
        existing_grades = {r['student_id']: r for r in cur.execute("SELECT student_id, grade, comment FROM grade_records WHERE date=? AND period=? AND subject=?", (date, period, subject_name)).fetchall()}

        conn.close()

        context = {
            "date": date,
            "class_id": class_id,
            "course_id": course_id,
            "timetable_id": timetable_id,
            "subject_id": subject_id,
            "period": period,
            "group_name": group_name,
            "students": students,
            "subject_name": subject_name,
            "existing_attendance": existing_attendance,
            "existing_grades": existing_grades,
            "range": range,
            "timetable_entry": timetable_entry,
        }
        self.render("capture_data.html", context)

    def _post_capture_data_save(self):
        data = self._parse_post()
        date = data.get('date')
        period = data.get('period')
        subject_id = data.get('subject_id')
        class_id = data.get('class_id')
        course_id = data.get('course_id')

        if not date or not subject_id or not period:
            return self._send_html("<h1>400</h1><p>Missing required form fields.</p>", status=400)

        conn = get_db_connection(); cur = conn.cursor()

        subject_row = cur.execute("SELECT name FROM subjects WHERE id=?", (subject_id,)).fetchone()
        subject_name = subject_row['name'] if subject_row else 'Unknown'

        student_ids = set()
        for key in data.keys():
            if key.startswith('attendance_') or key.startswith('grade_'):
                try:
                    sid = int(key.split('_')[-1])
                    student_ids.add(sid)
                except (ValueError, IndexError):
                    continue

        with conn:
            for sid in student_ids:
                # --- Attendance ---
                attendance_val = data.get(f'attendance_{sid}')

                # Check for existing record
                cur.execute("SELECT id FROM attendance_records WHERE student_id=? AND date=? AND period=?", (sid, date, period))
                existing_att_id = cur.fetchone()

                if attendance_val:
                    status = 'present'
                    late_minutes = 0
                    if attendance_val == 'absent':
                        status = 'absent'
                    elif attendance_val.startswith('late_'):
                        try:
                            late_minutes = int(attendance_val.split('_')[1])
                        except (ValueError, IndexError):
                            late_minutes = 0

                    absent_minutes = LESSON_MINUTES if status == 'absent' else 0

                    if existing_att_id:
                        cur.execute(
                            "UPDATE attendance_records SET status=?, absent_minutes=?, late_minutes=? WHERE id=?",
                            (status, absent_minutes, late_minutes, existing_att_id['id'])
                        )
                    else:
                        cur.execute(
                            "INSERT INTO attendance_records (student_id, date, period, status, absent_minutes, late_minutes) VALUES (?, ?, ?, ?, ?, ?)",
                            (sid, date, period, status, absent_minutes, late_minutes)
                        )

                # --- Grade and Comment ---
                grade_val = data.get(f'grade_{sid}')
                comment_val = data.get(f'comment_{sid}')

                cur.execute("SELECT id FROM grade_records WHERE student_id=? AND date=? AND period=? AND subject=?", (sid, date, period, subject_name))
                existing_grade_id = cur.fetchone()

                if grade_val or comment_val:
                    try:
                        grade = float(grade_val) if grade_val and grade_val.strip() else None
                        if existing_grade_id:
                            cur.execute(
                                "UPDATE grade_records SET grade=?, comment=? WHERE id=?",
                                (grade, comment_val or None, existing_grade_id['id'])
                            )
                        else:
                            cur.execute(
                                "INSERT INTO grade_records (student_id, date, period, type, subject, grade, comment) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (sid, date, period, 'spontaneous', subject_name, grade, comment_val or None)
                            )
                    except (ValueError, TypeError):
                        pass # Gracefully skip if grade is not a valid float
                elif existing_grade_id:
                    # If no grade/comment is submitted, but a record exists, delete it.
                    cur.execute("DELETE FROM grade_records WHERE id=?", (existing_grade_id['id'],))

        conn.close()
        self._redirect(f"/?msg=Daten+fuer+{date}+gespeichert")

    def _post_lesson_update_status(self):
        data = self._parse_post()
        date = data.get('date')
        period = data.get('period')
        timetable_id = data.get('timetable_id')
        status = data.get('status')

        if not date or not period or not timetable_id or not status:
            return self._send_html("<h1>400</h1><p>Missing required form fields.</p>", status=400)

        conn = get_db_connection()
        cur = conn.cursor()

        with conn:
            template_entry = cur.execute("SELECT * FROM timetable WHERE id=?", (timetable_id,)).fetchone()
            if not template_entry:
                conn.close()
                return self._send_html("<h1>404</h1><p>Original timetable entry not found.</p>", status=404)

            where_parts = ["date = ?", "period = ?"]
            params = [date, template_entry['period']]
            # Build query that handles NULLs correctly
            for col in ['subject_id', 'class_id', 'course_id']:
                if template_entry[col] is not None:
                    where_parts.append(f"{col} = ?")
                    params.append(template_entry[col])
                else:
                    where_parts.append(f"{col} IS NULL")

            query = "SELECT id FROM timetable WHERE " + " AND ".join(where_parts)
            existing_override = cur.execute(query, tuple(params)).fetchone()

            if existing_override:
                cur.execute("UPDATE timetable SET status=? WHERE id=?", (status, existing_override['id']))
            else:
                cur.execute(
                    """INSERT INTO timetable (class_id, course_id, period, is_double, slot, day, time_range, date, subject_id, room, status)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        template_entry['class_id'], template_entry['course_id'], template_entry['period'],
                        template_entry['is_double'], template_entry['slot'], template_entry['day'],
                        template_entry['time_range'], date, template_entry['subject_id'],
                        template_entry['room'], status
                    )
                )

        conn.close()
        self._redirect(f"/?msg=Stunde+am+{date}+aktualisiert")

    def _post_lesson_uncancel(self):
        data = self._parse_post()
        timetable_id = data.get('timetable_id')
        if not timetable_id:
            return self._send_html("<h1>400</h1><p>Missing timetable_id.</p>", status=400)

        conn = get_db_connection()
        with conn:
            cur = conn.cursor()
            # We only delete if it's a date-specific override. Don't delete templates.
            cur.execute("DELETE FROM timetable WHERE id = ? AND date IS NOT NULL", (timetable_id,))
        conn.close()
        self._redirect("/?msg=Ausfall+wurde+rueckgaengig+gemacht.")

    # ======= Klassen-Liste =======
    def _handle_classes(self, params):
        sort = (params.get("sort", ["name"])[0] or "name").lower()
        direction = (params.get("dir", ["asc"])[0] or "asc").lower()
        direction = "desc" if direction == "desc" else "asc"

        conn = get_db_connection(); cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM classes"); total_classes = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM courses"); total_courses = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM students"); total_students = cur.fetchone()[0]

        rows = cur.execute(
            "SELECT c.id, c.name, c.teacher_id, t.short AS teacher_short, t.name AS teacher_name "
            "FROM classes c LEFT JOIN teachers t ON c.teacher_id = t.id"
        ).fetchall()

        teachers_list = get_all_teachers(cur)

        data = []
        for r in rows:
            class_id = r["id"]
            student_count = cur.execute("SELECT COUNT(*) FROM students WHERE class_id=?", (class_id,)).fetchone()[0]
            cur.execute(
                "SELECT AVG(g.grade) FROM grade_records g "
                "JOIN students s ON g.student_id = s.id WHERE s.class_id=?", (class_id,)
            )
            avg_grade = cur.fetchone()[0] or None
            cur.execute(
                "SELECT COALESCE(SUM(a.absent_minutes),0) FROM attendance_records a JOIN students s ON a.student_id = s.id "
                "WHERE s.class_id=? AND a.status='absent'",
                (class_id,),
            )
            absent_minutes = cur.fetchone()[0] or 0
            cur.execute(
                "SELECT COALESCE(SUM(a.late_minutes),0) FROM attendance_records a "
                "JOIN students s ON a.student_id = s.id WHERE s.class_id=?",
                (class_id,),
            )
            total_late_minutes = cur.fetchone()[0] or 0

            fehlstunden = (absent_minutes or 0) / float(LESSON_MINUTES)
            teacher_label = (r["teacher_short"] or "") + ((" (" + r["teacher_name"] + ")") if r["teacher_name"] else "")
            data.append({
                "id": class_id,
                "name": r["name"],
                "teacher": teacher_label,
                "teacher_id": r["teacher_id"],
                "teacher_short": r["teacher_short"],
                "teacher_name": r["teacher_name"],
                "student_count": student_count,
                "avg_grade": avg_grade,
                "fehlstunden": fehlstunden,
                "verspaetung_min": total_late_minutes,
            })

        conn.close()

        key_map = {
            "name": lambda x: (x["name"] or "").lower(),
            "size": lambda x: x["student_count"],
            "avg":  lambda x: (x["avg_grade"] if x["avg_grade"] is not None else 9999.0),
            "fehl": lambda x: x["fehlstunden"],
            "late": lambda x: x["verspaetung_min"],
            "id":   lambda x: x["id"],
        }
        keyfunc = key_map.get(sort, key_map["name"])
        reverse = True if direction == "desc" else False
        data.sort(key=keyfunc, reverse=reverse)

        def get_sort_link(col):
            next_dir = "desc" if (sort == col and direction == "asc") else "asc"
            return f"/classes?sort={col}&dir={next_dir}"

        sort_links = {
            "id": get_sort_link("id"),
            "name": get_sort_link("name"),
            "size": get_sort_link("size"),
            "avg": get_sort_link("avg"),
            "fehl": get_sort_link("fehl"),
            "late": get_sort_link("late"),
        }

        context = {
            "total_classes": total_classes,
            "total_courses": total_courses,
            "total_students": total_students,
            "data": data,
            "teachers_list": teachers_list,
            "sort_links": sort_links,
        }
        self.render("classes.html", context)

    # ======= Kurse-Liste =======
    def _handle_courses(self, params):
        sort = (params.get("sort", ["name"])[0] or "name").lower()
        direction = (params.get("dir", ["asc"])[0] or "asc").lower()
        direction = "desc" if direction == "desc" else "asc"

        conn = get_db_connection(); cur = conn.cursor()

        rows = cur.execute(
            "SELECT d.id, d.name, d.leader_id, t.short AS leader_short, t.name AS leader_name, "
            "(SELECT COUNT(*) FROM students s WHERE s.course_id = d.id) AS student_count "
            "FROM courses d LEFT JOIN teachers t ON d.leader_id = t.id"
        ).fetchall()

        data = []
        for r in rows:
            course_id = r["id"]
            cur.execute(
                "SELECT AVG(g.grade) FROM grade_records g JOIN students s ON g.student_id=s.id WHERE s.course_id=?",
                (course_id,),
            )
            avg_grade = cur.fetchone()[0] or None
            cur.execute(
                "SELECT COALESCE(SUM(a.absent_minutes),0) FROM attendance_records a JOIN students s ON a.student_id = s.id "
                "WHERE s.course_id=? AND a.status='absent'",
                (course_id,),
            )
            absent_minutes = cur.fetchone()[0] or 0
            cur.execute(
                "SELECT COALESCE(SUM(a.late_minutes),0) FROM attendance_records a JOIN students s ON a.student_id = s.id "
                "WHERE s.course_id=?",
                (course_id,),
            )
            late_minutes = cur.fetchone()[0] or 0
            fehlstunden = (absent_minutes or 0) / float(LESSON_MINUTES)
            leader = (r["leader_short"] or "") + (
                (" (" + r["leader_name"] + ")") if r["leader_name"] else ""
            )
            data.append({
                "id": course_id,
                "name": r["name"],
                "leader": leader,
                "leader_id": r["leader_id"],
                "leader_short": r["leader_short"],
                "leader_name": r["leader_name"],
                "student_count": r["student_count"],
                "avg_grade": avg_grade,
                "fehlstunden": fehlstunden,
                "verspaetung_min": late_minutes,
            })
        conn.close()

        key_map = {
            "name": lambda x: (x["name"] or "").lower(),
            "size": lambda x: x["student_count"],
            "avg":  lambda x: (x["avg_grade"] if x["avg_grade"] is not None else 9999.0),
            "fehl": lambda x: x["fehlstunden"],
            "late": lambda x: x["verspaetung_min"],
            "id":   lambda x: x["id"],
            "leader": lambda x: (x["leader"] or "").lower(),
        }
        keyfunc = key_map.get(sort, key_map["name"])
        reverse = True if direction == "desc" else False
        data.sort(key=keyfunc, reverse=reverse)

        def get_sort_link(col):
            next_dir = "desc" if (sort == col and direction == "asc") else "asc"
            return f"/courses?sort={col}&dir={next_dir}"

        sort_links = {
            "id": get_sort_link("id"),
            "name": get_sort_link("name"),
            "size": get_sort_link("size"),
            "avg": get_sort_link("avg"),
            "fehl": get_sort_link("fehl"),
            "late": get_sort_link("late"),
            "leader": get_sort_link("leader"),
        }

        conn = get_db_connection(); cur = conn.cursor()
        teachers = get_all_teachers(cur)
        conn.close()

        context = {
            "data": data,
            "teachers": teachers,
            "sort_links": sort_links,
        }
        self.render("courses.html", context)

    # ======= Schüler-Liste =======
    def _handle_students(self, params):
        sort = (params.get("sort", ["class"])[0] or "class").lower()
        direction = (params.get("dir", ["asc"])[0] or "asc").lower()
        direction = "desc" if direction == "desc" else "asc"
        class_id = params.get("class_id", [""])[0]
        course_id = params.get("course_id", [""])[0]
        search_query = params.get("q", [""])[0].strip()

        conn = get_db_connection(); cur = conn.cursor()

        classes = get_all_classes(cur)
        courses = get_all_courses(cur)

        base = (
            "SELECT s.id, s.first_name, s.last_name, c.name AS class_name, d.name AS course_name, s.class_id, s.course_id "
            "FROM students s JOIN classes c ON s.class_id=c.id "
            "LEFT JOIN courses d ON s.course_id=d.id "
        )
        where, plist = [], []
        if class_id and class_id.isdigit():
            where.append("s.class_id=?"); plist.append(int(class_id))
        if course_id and course_id.isdigit():
            where.append("s.course_id=?"); plist.append(int(course_id))
        if search_query:
            where.append("(s.first_name LIKE ? OR s.last_name LIKE ?)")
            plist.extend([f"%{search_query}%", f"%{search_query}%"])
        if where: base += "WHERE " + " AND ".join(where) + " "

        order_sql = {
            "first": "ORDER BY s.first_name",
            "last":  "ORDER BY s.last_name",
            "class": "ORDER BY c.name",
            "course":"ORDER BY d.name",
            "id":    "ORDER BY s.id",
        }.get(sort, "ORDER BY c.name, s.last_name, s.first_name")
        order_sql += " DESC" if direction == "desc" else " ASC"

        rows = cur.execute(base + order_sql, plist).fetchall()

        total_classes = cur.execute("SELECT COUNT(*) FROM classes").fetchone()[0]
        total_courses = cur.execute("SELECT COUNT(*) FROM courses").fetchone()[0]
        total_students = cur.execute("SELECT COUNT(*) FROM students").fetchone()[0]

        conn.close()

        def get_sort_link(col):
            next_dir = "desc" if (sort == col and direction == "asc") else "asc"
            qs = []
            if class_id: qs.append(f"class_id={class_id}")
            if course_id: qs.append(f"course_id={course_id}")
            qs.append(f"sort={col}&dir={next_dir}")
            return f"/students?{'&'.join(qs)}"

        sort_links = {
            "id": get_sort_link("id"),
            "first": get_sort_link("first"),
            "last": get_sort_link("last"),
            "class": get_sort_link("class"),
            "course": get_sort_link("course"),
        }

        context = {
            "total_classes": total_classes,
            "total_courses": total_courses,
            "total_students": total_students,
            "classes": classes,
            "courses": courses,
            "rows": rows,
            "class_id": class_id,
            "course_id": course_id,
            "sort_links": sort_links,
            "search_query": search_query,
        }
        self.render("students.html", context)

    # ======= Klassen-Detail =======
    def _handle_class_detail(self, params):
        class_id = params.get("id", [None])[0]
        if not class_id:
            self._send_html("<html><body><h1>Fehler</h1><p>Keine Klassen-ID angegeben.</p><p><a href='/classes'>Zurück</a></p></body></html>"); return
        conn = get_db_connection(); cur = conn.cursor()
        cur.execute("SELECT id,name FROM classes WHERE id=?", (class_id,))
        row = cur.fetchone()
        if not row:
            conn.close(); self._send_html("<html><body><h1>Fehler</h1><p>Klasse nicht gefunden.</p><p><a href='/classes'>Zurück</a></p></body></html>"); return
        cname = row["name"]
        total_students = cur.execute("SELECT COUNT(*) FROM students WHERE class_id=?", (class_id,)).fetchone()[0]
        course_count_class = cur.execute("SELECT COUNT(DISTINCT course_id) FROM students WHERE class_id=? AND course_id IS NOT NULL", (class_id,)).fetchone()[0]
        rows = cur.execute(
            "SELECT s.id, s.first_name, s.last_name, c.name AS class_name, d.name AS course_name, s.course_id, s.class_id "
            "FROM students s JOIN classes c ON s.class_id=c.id "
            "LEFT JOIN courses d ON s.course_id=d.id WHERE c.id=? ORDER BY s.last_name, s.first_name",
            (class_id,)
        ).fetchall()
        classes = get_all_classes(cur)
        courses = get_all_courses(cur)
        conn.close()

        conn2 = get_db_connection(); cur2 = conn2.cursor()
        row_teacher = cur2.execute("SELECT teacher_id FROM classes WHERE id=?", (class_id,)).fetchone()
        teachers_for_select = get_all_teachers(cur2)
        conn2.close()
        current_teacher_id = row_teacher['teacher_id'] if row_teacher else None

        context = {
            "class_id": class_id,
            "cname": cname,
            "total_students": total_students,
            "course_count_class": course_count_class,
            "rows": rows,
            "classes": classes,
            "courses": courses,
            "current_teacher_id": current_teacher_id,
            "teachers_for_select": teachers_for_select,
        }
        self.render("class_detail.html", context)

    # ======= Kurs-Detail =======
    def _handle_course_detail(self, params):
        course_id = params.get("id", [None])[0]
        if not course_id:
            self._send_html("<html><body><h1>Fehler</h1><p>Keine Kurs-ID angegeben.</p><p><a href='/courses'>Zurück</a></p></body></html>"); return
        conn = get_db_connection(); cur = conn.cursor()
        cur.execute(
            "SELECT d.id, d.name AS course_name, c.name AS class_name, d.leader_id, "
            "t.short AS leader_short, t.name AS leader_name "
            "FROM courses d "
            "LEFT JOIN classes c ON d.class_id=c.id "
            "LEFT JOIN teachers t ON d.leader_id=t.id "
            "WHERE d.id=?",
            (course_id,)
        )
        row = cur.fetchone()
        if not row:
            conn.close(); self._send_html("<html><body><h1>Fehler</h1><p>Kurs nicht gefunden.</p><p><a href='/courses'>Zurück</a></p></body></html>"); return
        course_name = row["course_name"]
        class_name = row["class_name"] or ""
        leader = (row["leader_short"] or "") + ((" (" + row["leader_name"] + ")") if row["leader_name"] else "")
        total_students_in_course = cur.execute("SELECT COUNT(*) FROM students WHERE course_id=?", (course_id,)).fetchone()[0]
        rows = cur.execute(
            "SELECT s.id, s.first_name, s.last_name, c.name AS class_name, s.class_id, s.course_id "
            "FROM students s JOIN classes c ON s.class_id=c.id "
            "WHERE s.course_id=? ORDER BY s.last_name, s.first_name",
            (course_id,),
        ).fetchall()
        classes = get_all_classes(cur)
        courses = get_all_courses(cur)
        teachers = get_all_teachers(cur)
        conn.close()

        context = {
            "course_id": course_id,
            "course_name": course_name,
            "class_name": class_name,
            "total_students_in_course": total_students_in_course,
            "leader_id": row["leader_id"],
            "rows": rows,
            "classes": classes,
            "courses": courses,
            "teachers": teachers,
        }
        self.render("course_detail.html", context)

    def _handle_student_detail(self, params):
        student_id = params.get("id", [None])[0]
        filter_type = params.get("filter", [""])[0].strip()
        msg = html_escape("".join(params.get("msg", [""])))
        if not student_id:
            self._send_html("<html><body><h1>Fehler</h1><p>Keine Sch&uuml;ler-ID angegeben.</p><p><a href='/students'>Zurück</a></p></body></html>"); return

        conn = get_db_connection(); cur = conn.cursor()
        cur.execute(
            "SELECT s.id, s.first_name, s.last_name, c.name AS class_name, d.name AS course_name, s.class_id, s.course_id "
            "FROM students s JOIN classes c ON s.class_id=c.id "
            "LEFT JOIN courses d ON s.course_id=d.id WHERE s.id=?", (student_id,)
        )
        srow = cur.fetchone()
        if not srow:
            conn.close(); self._send_html("<html><body><h1>Fehler</h1><p>Sch&uuml;ler nicht gefunden.</p><p><a href='/students'>Zurück</a></p></body></html>"); return

        total_attendance = cur.execute("SELECT COUNT(*) FROM attendance_records WHERE student_id=?", (student_id,)).fetchone()[0]
        total_present    = cur.execute("SELECT COUNT(*) FROM attendance_records WHERE student_id=? AND status='present'", (student_id,)).fetchone()[0]
        total_absent     = cur.execute("SELECT COUNT(*) FROM attendance_records WHERE student_id=? AND status='absent'", (student_id,)).fetchone()[0]
        total_absent_minutes = cur.execute("SELECT COALESCE(SUM(absent_minutes),0) FROM attendance_records WHERE student_id=? AND status='absent'", (student_id,)).fetchone()[0]
        total_late_minutes = cur.execute("SELECT COALESCE(SUM(late_minutes),0) FROM attendance_records WHERE student_id=?", (student_id,)).fetchone()[0]

        total_grades = cur.execute("SELECT COUNT(*) FROM grade_records WHERE student_id=?", (student_id,)).fetchone()[0]
        perf_count, perf_avg = cur.execute("SELECT COUNT(*), COALESCE(AVG(grade),0) FROM grade_records WHERE student_id=? AND type='performance'", (student_id,)).fetchone()
        spont_count, spont_avg = cur.execute("SELECT COUNT(*), COALESCE(AVG(grade),0) FROM grade_records WHERE student_id=? AND type='spontaneous'", (student_id,)).fetchone()
        perf_avg_display = f"{perf_avg:.2f}" if perf_count else "-"
        spont_avg_display = f"{spont_avg:.2f}" if spont_count else "-"

        classes = get_all_classes(cur)
        courses = get_all_courses(cur)

        recs = []
        if filter_type in ("", "attendance", "present", "absent"):
            q = "SELECT date, period, 'attendance' AS kind, status, absent_minutes, late_minutes, NULL AS type, NULL AS grade, NULL AS subject, NULL as comment FROM attendance_records WHERE student_id=?"
            plist = [student_id]
            if filter_type == "present": q += " AND status='present'"
            if filter_type == "absent":  q += " AND status='absent'"
            recs.extend(cur.execute(q, plist).fetchall())
        if filter_type in ("", "grades", "performance", "spontaneous"):
            q = "SELECT date, period, 'grade' AS kind, NULL AS status, NULL AS absent_minutes, NULL AS late_minutes, type, grade, subject, comment FROM grade_records WHERE student_id=?"
            plist = [student_id]
            if filter_type == "performance": q += " AND type='performance'"
            if filter_type == "spontaneous": q += " AND type='spontaneous'"
            recs.extend(cur.execute(q, plist).fetchall())
        recs.sort(key=lambda r: r["date"], reverse=True)
        conn.close()

        fehlstunden = (total_absent_minutes or 0) / float(LESSON_MINUTES)
        today_str = datetime.date.today().isoformat()

        context = {
            "student_id": student_id,
            "srow": srow,
            "msg": msg,
            "total_attendance": total_attendance,
            "total_present": total_present,
            "total_absent": total_absent,
            "total_absent_minutes": total_absent_minutes,
            "total_late_minutes": total_late_minutes,
            "fehlstunden": fehlstunden,
            "total_grades": total_grades,
            "perf_count": perf_count,
            "perf_avg": perf_avg,
            "spont_count": spont_count,
            "spont_avg": spont_avg,
            "classes": classes,
            "courses": courses,
            "recs": recs,
            "filter_type": filter_type,
            "today_str": today_str,
        }
        self.render("student_detail.html", context)

    # ---------- Admin-Bereich ----------
    def _handle_admin(self, params):
        self.render("admin.html")

    # ----- Subjects Admin -----
    def _handle_admin_subjects(self, params):
        conn = get_db_connection(); cur = conn.cursor()
        subjects = cur.execute("SELECT id, name, short FROM subjects ORDER BY name").fetchall()
        conn.close()
        context = {
            "subjects": subjects,
        }
        self.render("admin_subjects.html", context)

    def _post_admin_subject_create(self):
        data = self._parse_post()
        name = (data.get('name') or '').strip()
        short = (data.get('short') or '').strip()
        if not name or not short:
            return self._send_html("<h1>400</h1><p>Name und Kürzel erforderlich.</p>", status=400)
        conn = get_db_connection(); cur = conn.cursor()
        with conn:
            cur.execute("INSERT OR IGNORE INTO subjects(name, short) VALUES(?, ?)", (name, short))
        conn.close()
        self._log_change('subjects', None, 'create', '', f'{name}/{short}', 'manual', None)
        self._redirect('/admin/subjects')

    def _post_admin_subject_delete(self):
        data = self._parse_post()
        sid = int(data.get('id','0') or '0')
        if sid<=0:
            return self._send_html("<h1>400</h1><p>Ungültige ID.</p>", status=400)
        conn = get_db_connection(); cur = conn.cursor()
        with conn:
            cur.execute("DELETE FROM subjects WHERE id=?", (sid,))
        conn.close()
        self._log_change('subjects', sid, 'delete', '', '', 'manual', None)
        self._redirect('/admin/subjects')

    # ----- Teachers Admin -----
    def _handle_admin_teachers(self, params):
        conn = get_db_connection(); cur = conn.cursor()
        teachers = cur.execute("SELECT id, short, name FROM teachers ORDER BY short").fetchall()
        conn.close()
        context = {
            "teachers": teachers,
        }
        self.render("admin_teachers.html", context)

    def _handle_admin_classes(self, params):
        conn = get_db_connection(); cur = conn.cursor()
        classes = cur.execute(
            "SELECT c.id, c.name, c.teacher_id, t.short AS teacher_short, t.name AS teacher_name, "
            "(SELECT COUNT(*) FROM students s WHERE s.class_id = c.id) AS student_count "
            "FROM classes c LEFT JOIN teachers t ON c.teacher_id = t.id ORDER BY c.name"
        ).fetchall()
        teachers = get_all_teachers(cur)
        conn.close()
        context = {
            "classes": classes,
            "teachers": teachers,
        }
        self.render("admin_classes.html", context)

    def _post_admin_teacher_create(self):
        data = self._parse_post()
        short = (data.get('short') or '').strip()
        name  = (data.get('name') or '').strip()
        if not short or not name:
            return self._send_html("<h1>400</h1><p>Kürzel und Name erforderlich.</p>", status=400)
        conn = get_db_connection(); cur = conn.cursor()
        with conn:
            cur.execute("INSERT OR IGNORE INTO teachers(short, name) VALUES(?, ?)", (short, name))
        conn.close()
        self._log_change('teachers', None, 'create', '', f'{short}/{name}', 'manual', None)
        self._redirect('/admin/teachers')

    def _post_admin_teacher_delete(self):
        data = self._parse_post()
        tid = int(data.get('id','0') or '0')
        if tid<=0:
            return self._send_html("<h1>400</h1><p>Ungültige ID.</p>", status=400)
        conn = get_db_connection(); cur = conn.cursor()
        with conn:
            cur.execute("DELETE FROM teachers WHERE id=?", (tid,))
        conn.close()
        self._log_change('teachers', tid, 'delete', '', '', 'manual', None)
        self._redirect('/admin/teachers')

    # ----- Courses Admin -----
    def _handle_admin_courses(self, params):
        conn = get_db_connection(); cur = conn.cursor()
        courses_rows = cur.execute(
            "SELECT d.id, d.name, d.leader_id, t.short AS leader_short, t.name AS leader_name, "
            "(SELECT COUNT(*) FROM students s WHERE s.course_id = d.id) AS student_count "
            "FROM courses d LEFT JOIN teachers t ON d.leader_id = t.id ORDER BY d.name"
        ).fetchall()

        courses_data = []
        for course in courses_rows:
            course_dict = dict(course)
            class_list_rows = cur.execute(
                "SELECT DISTINCT c.name FROM classes c JOIN students s ON c.id = s.class_id WHERE s.course_id = ? ORDER BY c.name",
                (course['id'],)
            ).fetchall()
            course_dict['classes_in_course'] = ", ".join([r['name'] for r in class_list_rows])
            courses_data.append(course_dict)

        teachers = get_all_teachers(cur)
        conn.close()
        context = {
            "courses": courses_data,
            "teachers": teachers,
        }
        self.render("admin_courses.html", context)

    def _handle_admin_timetable(self, params):
        conn = get_db_connection(); cur = conn.cursor()
        
        # Fetch all timetable entries
        entries = cur.execute("""
            SELECT t.*, c.name as class_name, co.name as course_name, s.name as subject_name
            FROM timetable t
            LEFT JOIN classes c on t.class_id = c.id
            LEFT JOIN courses co on t.course_id = co.id
            LEFT JOIN subjects s on t.subject_id = s.id
            ORDER BY t.day, t.period
        """).fetchall()

        classes = get_all_classes(cur)
        courses = get_all_courses(cur)
        subjects = get_all_subjects(cur)
        
        conn.close()

        context = {
            "entries": entries,
            "classes": classes,
            "courses": courses,
            "subjects": subjects,
            "days": ['Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag'],
            "periods": [item['period'] for item in SCHEDULE_PATTERN if 'period' in item]
        }
        self.render("admin_timetable.html", context)

    def _handle_admin_timetable_edit(self, params):
        entry_id = params.get('id', [None])[0]
        if not entry_id:
            return self._send_html("<h1>400</h1><p>Missing entry ID.</p>", status=400)

        conn = get_db_connection(); cur = conn.cursor()

        entry = cur.execute("SELECT * FROM timetable WHERE id = ?", (entry_id,)).fetchone()
        if not entry:
            conn.close()
            return self._send_html("<h1>404</h1><p>Entry not found.</p>", status=404)

        classes = get_all_classes(cur)
        courses = get_all_courses(cur)
        subjects = get_all_subjects(cur)

        conn.close()

        context = {
            "entry": entry,
            "classes": classes,
            "courses": courses,
            "subjects": subjects,
            "days": ['Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag'],
            "periods": [item['period'] for item in SCHEDULE_PATTERN if 'period' in item]
        }
        self.render("admin_timetable_edit.html", context)

    def _check_timetable_conflict(self, cur, day, period, class_id, course_id, exclude_id=None):
        """Checks for a conflict in the timetable. Returns True if a conflict exists."""
        # A conflict exists if a class is double-booked, or a course is double-booked.
        if not class_id and not course_id:
            return False

        query_parts = []
        params = [day, period]

        # Build query for class or course. A slot can be for a class OR a course.
        # If a class is given, check for conflicts for that class.
        # If a course is given, check for conflicts for that course.
        # If both are given (e.g. a course within a class), check both.
        conflict_clauses = []
        if class_id:
            conflict_clauses.append("class_id = ?")
            params.append(class_id)
        if course_id:
            conflict_clauses.append("course_id = ?")
            params.append(course_id)

        base_query = f"SELECT id FROM timetable WHERE day = ? AND period = ? AND ({' OR '.join(conflict_clauses)})"

        if exclude_id:
            base_query += " AND id != ?"
            params.append(exclude_id)

        conflict = cur.execute(base_query, tuple(params)).fetchone()
        return conflict is not None

    def _post_admin_timetable_create(self):
        data = self._parse_post()
        day = data.get('day')
        period = data.get('period')
        subject_id = data.get('subject_id')
        class_id = data.get('class_id') or None
        course_id = data.get('course_id') or None
        room = data.get('room')
        is_double = 'is_double' in data

        if not (day and period and subject_id and (class_id or course_id)):
             return self._send_html("<h1>400</h1><p>Missing required fields: Day, Period, Subject and Class/Course.</p>", status=400)

        conn = get_db_connection(); cur = conn.cursor()

        if self._check_timetable_conflict(cur, day, period, class_id, course_id):
            conn.close()
            return self._send_html("<h1>409 Conflict</h1><p>A lesson for this class/course already exists at this time.</p><p><a href='/admin/timetable'>Back</a></p>", status=409)

        with conn:
            cur.execute(
                """INSERT INTO timetable (day, period, subject_id, class_id, course_id, room, is_double, slot, date)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)""",
                (day, period, subject_id, class_id, course_id, room, 1 if is_double else 0, period)
            )
        conn.close()
        self._redirect('/admin/timetable')

    def _post_admin_timetable_update(self):
        data = self._parse_post()
        entry_id = data.get('id')
        day = data.get('day')
        period = data.get('period')
        subject_id = data.get('subject_id')
        class_id = data.get('class_id') or None
        course_id = data.get('course_id') or None
        room = data.get('room')
        is_double = 'is_double' in data

        if not (entry_id and day and period and subject_id and (class_id or course_id)):
             return self._send_html("<h1>400</h1><p>Missing required fields.</p>", status=400)

        conn = get_db_connection(); cur = conn.cursor()

        if self._check_timetable_conflict(cur, day, period, class_id, course_id, exclude_id=entry_id):
            conn.close()
            return self._send_html("<h1>409 Conflict</h1><p>A lesson for this class/course already exists at this time.</p><p><a href='/admin/timetable'>Back</a></p>", status=409)

        with conn:
            cur.execute(
                """UPDATE timetable
                   SET day=?, period=?, subject_id=?, class_id=?, course_id=?, room=?, is_double=?, slot=?
                   WHERE id=?""",
                (day, period, subject_id, class_id, course_id, room, 1 if is_double else 0, period, entry_id)
            )
        conn.close()
        self._redirect('/admin/timetable')

    def _post_admin_course_create(self):
        data = self._parse_post()
        course_name = (data.get('course_name') or '').strip()
        if not course_name:
            return self._send_html("<h1>400</h1><p>Kursname erforderlich.</p>", status=400)
        leader_id_raw = data.get('leader_id', '0') or '0'
        leader_id = int(leader_id_raw) if leader_id_raw.isdigit() else 0
        new_teacher_short = (data.get('new_teacher_short') or '').strip()
        new_teacher_name = (data.get('new_teacher_name') or '').strip()
        conn = get_db_connection(); cur = conn.cursor()
        with conn:
            if new_teacher_short or new_teacher_name:
                leader_id = create_teacher(cur, new_teacher_short, new_teacher_name)
            norm_name = _normalize_group_name(course_name)
            row = cur.execute("SELECT id FROM courses WHERE LOWER(name)=LOWER(?)", (norm_name,)).fetchone()
            if row:
                if leader_id > 0:
                    cur.execute("UPDATE courses SET leader_id=? WHERE id=?", (leader_id, row['id']))
            else:
                cur.execute("INSERT INTO courses(name, leader_id) VALUES(?, ?)", (norm_name, leader_id if leader_id>0 else None))
        conn.close()
        self._log_change('courses', None, 'create', '', course_name, 'manual', None)
        self._redirect('/admin/courses')

    def _post_admin_course_delete(self):
        data = self._parse_post()
        try:
            cid = int(data.get('id','0') or '0')
        except ValueError:
            cid = 0
        if cid <= 0:
            return self._send_html("<h1>400</h1><p>Ungültige ID.</p>", status=400)
        conn = get_db_connection(); cur = conn.cursor()
        with conn:
            cur.execute("DELETE FROM courses WHERE id=?", (cid,))
        conn.close()
        self._log_change('courses', cid, 'delete', '', '', 'manual', None)
        self._redirect('/admin/courses')

    # ---------- Export/Import Leistungsabfragen ----------
    def _handle_export_import(self, params):
        conn = get_db_connection(); cur = conn.cursor()
        perf_rows = cur.execute(
            "SELECT p.id, p.type, p.description, p.date, p.class_id, p.course_id, c.name AS class_name, d.name AS course_name, s.name AS subject_name, s.short AS subject_short "
            "FROM performance_queries p "
            "LEFT JOIN classes c ON p.class_id=c.id "
            "LEFT JOIN courses d ON p.course_id=d.id "
            "LEFT JOIN subjects s ON p.subject_id=s.id "
            "ORDER BY p.date DESC, p.id DESC"
        ).fetchall()
        classes = get_all_classes(cur)
        courses = get_all_courses(cur)
        subjects = get_all_subjects(cur)
        conn.close()
        context = {
            "perf_rows": perf_rows,
            "classes": classes,
            "courses": courses,
            "subjects": subjects,
        }
        self.render("export_import.html", context)

    def _post_performance_create(self):
        data = self._parse_post()
        type_ = (data.get('type') or '').strip()
        description = (data.get('description') or '').strip()
        date = (data.get('date') or '').strip()
        subject_id_raw = data.get('subject_id', '0') or '0'
        class_id_raw = data.get('class_id', '0') or '0'
        course_id_raw = data.get('course_id', '0') or '0'
        try: subject_id = int(subject_id_raw)
        except ValueError: subject_id = 0
        try: class_id = int(class_id_raw)
        except ValueError: class_id = 0
        try: course_id = int(course_id_raw)
        except ValueError: course_id = 0
        max_op_points_raw = data.get('max_op_points', '0') or '0'
        try: max_op_points = float(max_op_points_raw)
        except ValueError: max_op_points = 0.0
        task_count_raw = data.get('task_count', '0') or '0'
        try:
            task_count = int(task_count_raw);
            if task_count < 0: task_count = 0
        except ValueError:
            task_count = 0
        max_points_str = (data.get('max_points') or '').strip()
        max_points_list = []
        if max_points_str:
            for part in max_points_str.split(','):
                part = part.strip()
                if not part: continue
                try:
                    max_points_list.append(float(part))
                except ValueError:
                    pass
        while len(max_points_list) < task_count:
            max_points_list.append(0.0)

        conn = get_db_connection(); cur = conn.cursor()
        with conn:
            cur.execute(
                "INSERT INTO performance_queries(type, description, subject_id, class_id, course_id, date, max_op_points) VALUES(?,?,?,?,?,?,?)",
                (type_ or 'Andere', description or None, subject_id if subject_id>0 else None, class_id if class_id>0 else None, course_id if course_id>0 else None, date, max_op_points)
            )
            perf_id = cur.lastrowid
            for i in range(1, task_count+1):
                maxp = max_points_list[i-1] if i-1 < len(max_points_list) else 0.0
                cur.execute(
                    "INSERT INTO performance_tasks(performance_id, number, max_points) VALUES(?,?,?)",
                    (perf_id, i, maxp)
                )

        # Log create
        group_txt = ""
        if class_id>0: group_txt = f"Klasse {class_id}"
        elif course_id>0: group_txt = f"Kurs {course_id}"
        self._log_change('performance_queries', perf_id, 'create', '', '', 'manual', f"{type_} {description}".strip() or None)

        # CSV Vorlage
        cur2 = conn.cursor()
        if class_id > 0:
            stu_rows = cur2.execute("SELECT id, last_name, first_name FROM students WHERE class_id=? ORDER BY last_name, first_name", (class_id,)).fetchall()
        elif course_id > 0:
            stu_rows = cur2.execute("SELECT id, last_name, first_name FROM students WHERE course_id=? ORDER BY last_name, first_name", (course_id,)).fetchall()
        else:
            stu_rows = []
        header = ["StudentID","Nachname","Vorname"]
        for i in range(1, task_count+1):
            header.append(f"Aufgabe{i}")
        header.extend(["OP","ZP"])
        lines = [";".join(header)]
        for s in stu_rows:
            row = [str(s['id']), s['last_name'], s['first_name']]
            row.extend(['' for _ in range(task_count)])
            row.extend(['',''])
            lines.append(";".join(row))
        csv_content = "\n".join(lines)
        conn.close()
        filename = f"leistungsabfrage_{perf_id}.csv"
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", f"attachment; filename={filename}")
        self.end_headers()
        self.wfile.write(csv_content.encode('utf-8'))

    def _post_performance_import(self):
        data = self._parse_post()
        perf_id_raw = data.get('performance_id', '0') or '0'
        csv_data = data.get('csv_data','') or ''
        try:
            perf_id = int(perf_id_raw)
        except ValueError:
            return self._send_html("<h1>400</h1><p>Ungültige Abfrage-ID.</p>", status=400)
        if not csv_data.strip():
            return self._send_html("<h1>400</h1><p>Keine CSV-Daten übermittelt.</p>", status=400)
        lines = [l.strip() for l in csv_data.strip().splitlines() if l.strip()]
        if not lines:
            return self._send_html("<h1>400</h1><p>CSV-Daten konnten nicht gelesen werden.</p>", status=400)

        header = [h.strip() for h in lines[0].split(';')]
        try:
            task_numbers = [h for h in header if h.startswith('Aufgabe')]
            task_count = len(task_numbers)
        except Exception:
            task_count = 0

        conn = get_db_connection(); cur = conn.cursor()
        with conn:
            cur.execute("DELETE FROM performance_task_results WHERE performance_id=?", (perf_id,))
            cur.execute("DELETE FROM performance_results WHERE performance_id=?", (perf_id,))
        imported_rows = 0
        for line in lines[1:]:
            parts = [p.strip() for p in line.split(';')]
            if len(parts) < 3:
                continue
            try:
                student_id = int(parts[0])
            except ValueError:
                continue
            values = parts[3:3+task_count]
            op = 0.0; zp = 0.0
            if len(parts) >= 3+task_count+1:
                op_str = parts[3+task_count]
                try: op = float(op_str) if op_str else 0.0
                except ValueError: op = 0.0
            if len(parts) >= 3+task_count+2:
                zp_str = parts[3+task_count+1]
                try: zp = float(zp_str) if zp_str else 0.0
                except ValueError: zp = 0.0
            with conn:
                cur.execute(
                    "INSERT INTO performance_results(performance_id, student_id, op_points, zp_points) VALUES(?,?,?,?)",
                    (perf_id, student_id, op, zp)
                )
                for i, val in enumerate(values, start=1):
                    try:
                        pts = float(val) if val else 0.0
                    except ValueError:
                        pts = 0.0
                    cur.execute(
                        "INSERT INTO performance_task_results(performance_id, student_id, task_number, points) VALUES(?,?,?,?)",
                        (perf_id, student_id, i, pts)
                    )
            imported_rows += 1
        conn.close()

        # Log Import
        connm = get_db_connection(); curm = connm.cursor()
        meta = curm.execute(
            "SELECT p.type, p.description, c.name AS class_name, d.name AS course_name, s.short AS subj_short, s.name AS subj_name "
            "FROM performance_queries p "
            "LEFT JOIN classes c ON p.class_id=c.id "
            "LEFT JOIN courses d ON p.course_id=d.id "
            "LEFT JOIN subjects s ON p.subject_id=s.id "
            "WHERE p.id=?", (perf_id,)
        ).fetchone()
        connm.close()
        group = (meta['class_name'] or meta['course_name'] or '') if meta else ''
        subj  = (meta['subj_short'] or meta['subj_name'] or '') if meta else ''
        comment = f"import; {meta['type'] if meta else ''} {group} {subj}".strip()
        self._log_change('performance_results', perf_id, 'import', '', str(imported_rows), 'import', comment)

        self._redirect(f"/performance?id={perf_id}")

    def _handle_performance_detail(self, params):
        perf_id = params.get('id', [None])[0]
        if not perf_id:
            return self._send_html("<h1>400</h1><p>Keine Abfrage-ID.</p>", status=400)
        try:
            pid = int(perf_id)
        except ValueError:
            return self._send_html("<h1>400</h1><p>Ungültige Abfrage-ID.</p>", status=400)
        conn = get_db_connection(); cur = conn.cursor()
        row = cur.execute(
            "SELECT p.id, p.type, p.description, p.date, p.class_id, p.course_id, p.grade_scale_id, p.max_op_points, c.name AS class_name, d.name AS course_name, "
            "s.name AS subject_name, s.short AS subject_short "
            "FROM performance_queries p "
            "LEFT JOIN classes c ON p.class_id=c.id "
            "LEFT JOIN courses d ON p.course_id=d.id "
            "LEFT JOIN subjects s ON p.subject_id=s.id "
            "WHERE p.id=?",
            (pid,)
        ).fetchone()
        if not row:
            conn.close()
            return self._send_html("<h1>404</h1><p>Leistungsabfrage nicht gefunden.</p>", status=404)
        tasks = cur.execute(
            "SELECT number, max_points FROM performance_tasks WHERE performance_id=? ORDER BY number",
            (pid,)
    ).fetchall()
        if not row:
            conn.close()
            return self._send_html("<h1>404</h1><p>Leistungsabfrage nicht gefunden.</p>", status=404)

        tasks = cur.execute(
            "SELECT number, max_points FROM performance_tasks WHERE performance_id=? ORDER BY number",
            (pid,)
        ).fetchall()
        task_count = len(tasks)
        if row['class_id']:
            students = cur.execute(
                "SELECT id, last_name, first_name FROM students WHERE class_id=? ORDER BY last_name, first_name",
                (row['class_id'],)
            ).fetchall()
        elif row['course_id']:
            students = cur.execute("SELECT id, last_name, first_name FROM students WHERE course_id=? ORDER BY last_name, first_name",
                (row['course_id'],)
            ).fetchall()
        else:
            students = []

        results = {}
        for s in students:
            results[s['id']] = {'op': 0.0, 'zp': 0.0, 'tasks': {}}

        for r2 in cur.execute("SELECT student_id, op_points, zp_points, grade_override, comment, op_is_edited, zp_is_edited FROM performance_results WHERE performance_id=?", (pid,)).fetchall():
            if r2['student_id'] in results:
                results[r2['student_id']]['op'] = r2['op_points'] or 0.0
                results[r2['student_id']]['zp'] = r2['zp_points'] or 0.0
                results[r2['student_id']]['override'] = r2['grade_override']
                results[r2['student_id']]['comment'] = r2['comment'] or ''
                results[r2['student_id']]['op_is_edited'] = r2['op_is_edited']
                results[r2['student_id']]['zp_is_edited'] = r2['zp_is_edited']

        for r3 in cur.execute("SELECT student_id, task_number, points, is_edited FROM performance_task_results WHERE performance_id=?", (pid,)).fetchall():
            if r3['student_id'] in results:
                results[r3['student_id']]['tasks'][r3['task_number']] = r3['points'] or 0.0

        grade_scales = cur.execute("SELECT id, name FROM grade_scales ORDER BY id").fetchall()
        curr_scale_row = None
        if row['grade_scale_id']:
            curr_scale_row = cur.execute("SELECT id, name, definition FROM grade_scales WHERE id=?", (row['grade_scale_id'],)).fetchone()

        scale_def = []
        if curr_scale_row:
            for ln in (curr_scale_row['definition'] or '').splitlines():
                parts = [p.strip() for p in ln.split(';')]
                if len(parts) == 3:
                    grade = parts[0]
                    try:
                        minp = float(parts[1]); maxp = float(parts[2])
                    except ValueError:
                        continue
                    scale_def.append((grade, minp, maxp))

        total_max = sum([t['max_points'] for t in tasks]) if task_count > 0 else 0.0
        student_totals = []
        task_avg = {i:0.0 for i in range(1, task_count+1)}
        if students:
            for i in range(1, task_count+1):
                total_i = 0.0; count_i = 0
                for s in students:
                    pts = results[s['id']]['tasks'].get(i)
                    if pts is not None:
                        total_i += pts
                        count_i += 1
                if count_i > 0:
                    task_avg[i] = total_i / count_i
            for s in students:
                task_sum = sum([v for v in results[s['id']]['tasks'].values()])
                tot = task_sum + results[s['id']]['op'] + results[s['id']]['zp']
                student_totals.append(tot)

        avg_points = sum(student_totals)/len(student_totals) if student_totals else 0.0
        best_points = max(student_totals) if student_totals else 0.0
        worst_points = min(student_totals) if student_totals else 0.0
        most_avg_task = None; most_avg_val = -1
        least_avg_task = None; least_avg_val = 1e9
        if task_count > 0:
            for i,v in task_avg.items():
                if v > most_avg_val: most_avg_val = v; most_avg_task = i
                if v < least_avg_val: least_avg_val = v; least_avg_task = i

        context = {
            'pid': pid,
            'row': row,
            'tasks': tasks,
            'task_count': task_count,
            'students': students,
            'results': results,
            'grade_scales': grade_scales,
            'curr_scale_row': curr_scale_row,
            'scale_def': scale_def,
            'total_max': total_max,
            'avg_points': avg_points,
            'best_points': best_points,
            'worst_points': worst_points,
            'most_avg_task': most_avg_task,
            'most_avg_val': most_avg_val,
            'least_avg_task': least_avg_task,
            'least_avg_val': least_avg_val,
        }
        conn.close()
        self.render("performance_detail.html", context)
        class_id = params.get('class_id', [''])[0]
        course_id = params.get('course_id', [''])[0]
        type_filter = params.get('type', [''])[0]
        conn = get_db_connection(); cur = conn.cursor()
        classes = get_all_classes(cur)
        courses = get_all_courses(cur)
        base = (
            "SELECT p.id, p.date, p.type, p.description, p.class_id, p.course_id, "
            "c.name AS class_name, d.name AS course_name, s.name AS subject_name, s.short AS subject_short, p.grade_scale_id "
            "FROM performance_queries p "
            "LEFT JOIN classes c ON p.class_id=c.id "
            "LEFT JOIN courses d ON p.course_id=d.id "
            "LEFT JOIN subjects s ON p.subject_id=s.id "
        )
        where = []; plist = []
        if class_id and class_id.isdigit():
            where.append("p.class_id=?"); plist.append(int(class_id))
        if course_id and course_id.isdigit():
            where.append("p.course_id=?"); plist.append(int(course_id))
        if type_filter:
            where.append("p.type LIKE ?"); plist.append(type_filter)
        if where:
            base += "WHERE " + " AND ".join(where) + " "
        base += "ORDER BY p.date DESC, p.id DESC"
        rows = cur.execute(base, plist).fetchall()
        conn.close()

        # Augment rows with average grade
        conn_for_avg = get_db_connection()
        cur_for_avg = conn_for_avg.cursor()

        augmented_rows = []
        for row in rows:
            row_dict = dict(row)
            cur_for_avg.execute(
                "SELECT AVG(grade_override) FROM performance_results WHERE performance_id = ? AND grade_override IS NOT NULL",
                (row['id'],)
            )
            avg_grade = cur_for_avg.fetchone()[0]
            row_dict['avg_grade'] = avg_grade
            augmented_rows.append(row_dict)

        conn_for_avg.close()

        context = {
            "rows": augmented_rows,
            "classes": classes,
            "courses": courses,
            "class_id": class_id,
            "course_id": course_id,
            "type_filter": type_filter,
        }
        self.render("leistungsabfragen.html", context)

    def _handle_performance_download(self, params):
        perf_id = params.get('id', [None])[0]
        if not perf_id:
            return self._send_html("<h1>400</h1><p>Keine Abfrage-ID.</p>", status=400)
        try:
            pid = int(perf_id)
        except ValueError:
            return self._send_html("<h1>400</h1><p>Ungültige ID.</p>", status=400)
        conn = get_db_connection(); cur = conn.cursor()
        row = cur.execute("SELECT id, class_id, course_id FROM performance_queries WHERE id=?", (pid,)).fetchone()
        if not row:
            conn.close()
            return self._send_html("<h1>404</h1><p>Abfrage nicht gefunden.</p>", status=404)
        tasks = cur.execute("SELECT number FROM performance_tasks WHERE performance_id=? ORDER BY number", (pid,)).fetchall()
        task_count = len(tasks)
        if row['class_id']:
            stu_rows = cur.execute("SELECT id, last_name, first_name FROM students WHERE class_id=? ORDER BY last_name, first_name", (row['class_id'],)).fetchall()
        elif row['course_id']:
            stu_rows = cur.execute("SELECT id, last_name, first_name FROM students WHERE course_id=? ORDER BY last_name, first_name", (row['course_id'],)).fetchall()
        else:
            stu_rows = []
        conn.close()
        header = ["StudentID","Nachname","Vorname"]
        for i in range(1, task_count+1):
            header.append(f"Aufgabe{i}")
        header.extend(["OP","ZP"])
        lines = [";".join(header)]
        for s in stu_rows:
            row_items = [str(s['id']), s['last_name'], s['first_name']]
            row_items.extend(['' for _ in range(task_count)])
            row_items.extend(['',''])
            lines.append(";".join(row_items))
        csv_content = "\n".join(lines)
        filename = f"leistungsabfrage_{pid}.csv"
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", f"attachment; filename={filename}")
        self.end_headers()
        self.wfile.write(csv_content.encode('utf-8'))

    # ======= Grade Scales (Notenschlüssel) =======
    def _handle_grade_scales(self, params):
        conn = get_db_connection(); cur = conn.cursor()
        scales = cur.execute("SELECT id, name, definition FROM grade_scales ORDER BY id").fetchall()
        conn.close()
        context = {
            "scales": scales,
        }
        self.render("grade_scales.html", context)

    def _post_grade_scale_create(self):
        data = self._parse_post()
        name = (data.get('name') or '').strip()
        definition = (data.get('definition') or '').strip()
        if not name or not definition:
            return self._send_html("<h1>400</h1><p>Name und Definition erforderlich.</p>", status=400)
        conn = get_db_connection(); cur = conn.cursor()
        with conn:
            cur.execute("INSERT INTO grade_scales(name, definition) VALUES(?, ?)", (name, definition))
        conn.close()
        self._log_change('grade_scales', None, 'create', '', name, 'manual', None)
        self._redirect('/grade_scales')

    def _post_assign_grade_scale(self):
        data = self._parse_post()
        perf_id_raw = data.get('performance_id', '0') or '0'
        scale_id_raw = data.get('scale_id', '0') or '0'
        try:
            perf_id = int(perf_id_raw)
            scale_id = int(scale_id_raw)
        except ValueError:
            return self._send_html("<h1>400</h1><p>Ungültige IDs.</p>", status=400)
        conn = get_db_connection(); cur = conn.cursor()
        old_row = cur.execute("SELECT grade_scale_id FROM performance_queries WHERE id=?", (perf_id,)).fetchone()
        old_val = old_row['grade_scale_id'] if old_row else None
        with conn:
            cur.execute("UPDATE performance_queries SET grade_scale_id=? WHERE id=?",
                        (scale_id if scale_id>0 else None, perf_id))
        conn.close()
        self._log_change('performance_queries', perf_id, 'grade_scale_id',
                         str(old_val), str(scale_id if scale_id>0 else None), 'manual', None)
        self._redirect(f"/performance?id={perf_id}")

    def _post_update_grade_override(self):
        data = self._parse_post()
        try:
            perf_id = int(data.get('performance_id','0') or '0')
            student_id = int(data.get('student_id','0') or '0')
        except ValueError:
            return self._send_html("<h1>400</h1><p>Ungültige IDs.</p>", status=400)
        override_raw = data.get('override','')
        comment = (data.get('comment') or '').strip()
        try:
            override_val = float(override_raw) if override_raw else None
        except ValueError:
            override_val = None
        conn = get_db_connection(); cur = conn.cursor()
        old_row = cur.execute("SELECT grade_override, comment FROM performance_results WHERE performance_id=? AND student_id=?", (perf_id, student_id)).fetchone()
        with conn:
            cur.execute("UPDATE performance_results SET grade_override=?, comment=? WHERE performance_id=? AND student_id=?",
                        (override_val, comment or None, perf_id, student_id))
        conn.close()
        old_override = old_row['grade_override'] if old_row else None
        old_comment = old_row['comment'] if old_row else None
        if override_val != old_override or comment != (old_comment or ''):
            self._log_change('performance_results', None, 'grade_override', str(old_override), str(override_val), 'manual', comment)
        self._redirect(f"/performance?id={perf_id}")

    def _calculate_student_performance_grade(self, cur, performance_id, student_id):
        """Calculates the grade for a single student in a single performance assessment."""

        performance = cur.execute("SELECT grade_scale_id FROM performance_queries WHERE id=?", (performance_id,)).fetchone()
        if not performance or not performance['grade_scale_id']:
            return None

        tasks = cur.execute("SELECT number, max_points FROM performance_tasks WHERE performance_id=? ORDER BY number", (performance_id,)).fetchall()
        total_max = sum(t['max_points'] for t in tasks)
        if total_max == 0:
            return None # Avoid division by zero if there are no tasks with points

        scale_row = cur.execute("SELECT definition FROM grade_scales WHERE id=?", (performance['grade_scale_id'],)).fetchone()
        if not scale_row:
            return None

        scale_def = []
        for ln in (scale_row['definition'] or '').splitlines():
            parts = [p.strip() for p in ln.split(';')]
            if len(parts) == 3:
                try:
                    scale_def.append((parts[0], float(parts[1]), float(parts[2])))
                except ValueError:
                    continue

        if not scale_def:
            return None

        results = cur.execute("SELECT op_points, zp_points, grade_override FROM performance_results WHERE performance_id=? AND student_id=?", (performance_id, student_id)).fetchone()
        task_results = cur.execute("SELECT points FROM performance_task_results WHERE performance_id=? AND student_id=?", (performance_id, student_id)).fetchall()

        op = results['op_points'] if results else 0
        zp = results['zp_points'] if results else 0
        override = results['grade_override'] if results else None

        task_sum = sum(r['points'] for r in task_results)

        tot = task_sum + op + zp
        tot_rounded = round(tot * 2.0) / 2.0
        pct = (tot_rounded / total_max * 100.0)

        grade = ''
        for g, mi, ma in scale_def:
            if pct >= mi and pct < ma:
                grade = g
                break

        final_grade = override if override is not None else grade

        return {
            "total_points": tot_rounded,
            "percentage": pct,
            "grade": final_grade
        }

    def _post_performance_update_student_scores(self):
        data = self._parse_json_post()
        performance_id = data.get('performance_id')
        student_id = data.get('student_id')
        scores = data.get('scores', {})

        if not all([performance_id, student_id, scores]):
            return self._send_json({'status': 'error', 'message': 'Missing data.'}, status=400)

        conn = get_db_connection()
        cur = conn.cursor()

        with conn:
            # Update op_points and zp_points
            if 'op_points' in scores:
                cur.execute("UPDATE performance_results SET op_points = ?, op_is_edited = 1 WHERE performance_id = ? AND student_id = ?", (scores['op_points'], performance_id, student_id))
            if 'zp_points' in scores:
                cur.execute("UPDATE performance_results SET zp_points = ?, zp_is_edited = 1 WHERE performance_id = ? AND student_id = ?", (scores['zp_points'], performance_id, student_id))

            # Update task results
            for key, value in scores.items():
                if key.startswith('task_'):
                    try:
                        task_number = int(key.split('_')[1])
                        points = float(value)
                    except (ValueError, IndexError):
                        continue

                    cur.execute("SELECT id FROM performance_task_results WHERE performance_id=? AND student_id=? AND task_number=?", (performance_id, student_id, task_number))
                    existing = cur.fetchone()
                    if existing:
                        cur.execute("UPDATE performance_task_results SET points=?, is_edited=1 WHERE id=?", (points, existing['id']))
                    else:
                        cur.execute("INSERT INTO performance_task_results (performance_id, student_id, task_number, points, is_edited) VALUES (?, ?, ?, ?, 1)", (performance_id, student_id, task_number, points))

            # After updating, recalculate the grade to return to the client
            updated_data = self._calculate_student_performance_grade(cur, performance_id, student_id)

        conn.close()
        return self._send_json({'status': 'ok', 'updated_data': updated_data})

    # ----- Attendance handling -----
    def _post_attendance_create(self):
        data = self._parse_post()
        try:
            student_id = int(data.get('student_id', '0') or '0')
        except ValueError:
            return self._send_html("<h1>400</h1><p>Ungültige Sch&uuml;ler-ID.</p>", status=400)
        date = (data.get('date') or '').strip()
        status = (data.get('status') or 'present').strip().lower()
        # Fehlstunden (Unterrichtsstunden) → Minuten
        abs_units_raw = data.get('absent_units', '')
        try:
            abs_units = int(abs_units_raw) if abs_units_raw != '' else 0
        except ValueError:
            abs_units = 0
        try:
            absent_minutes_input = int(data.get('absent_minutes','0') or '0')
        except ValueError:
            absent_minutes_input = 0
        absent_minutes = max(absent_minutes_input, abs_units * LESSON_MINUTES)

        try:
            late_minutes = int(data.get('late_minutes','0') or '0')
        except ValueError:
            late_minutes = 0
        if absent_minutes < 0: absent_minutes = 0
        if late_minutes < 0: late_minutes = 0

        db_status = 'present' if status == 'late' else status
        db_abs = absent_minutes if status == 'absent' else 0
        db_late = late_minutes if status == 'late' else 0

        conn = get_db_connection(); cur = conn.cursor()
        try:
            with conn:
                cur.execute("INSERT INTO attendance_records(student_id, date, status, absent_minutes, late_minutes) VALUES(?,?,?,?,?)", (student_id, date, db_status, db_abs, db_late))
                rec_id = cur.lastrowid
        except Exception as e:
            conn.close()
            return self._send_html(f"<h1>500</h1><p>Fehler beim Speichern: {html_escape(e)}</p>", status=500)
        conn.close()
        self._log_change('attendance_records', rec_id, 'create', '', f"{status};abs={db_abs};late={db_late}", 'manual', None)
        self._redirect(f"/student?id={student_id}&msg=Anwesenheit+eingetragen")

    def _handle_admin_attendance(self, params):
        class_id = params.get('class_id', [''])[0]
        course_id = params.get('course_id', [''])[0]
        conn = get_db_connection(); cur = conn.cursor()
        classes = get_all_classes(cur)
        courses = get_all_courses(cur)
        query = ("SELECT a.id, a.date, a.status, a.absent_minutes, a.late_minutes, "
                 "s.id as student_id, s.last_name, s.first_name, c.name AS class_name, d.name AS course_name "
                 "FROM attendance_records a "
                 "JOIN students s ON a.student_id = s.id "
                 "LEFT JOIN classes c ON s.class_id = c.id "
                 "LEFT JOIN courses d ON s.course_id = d.id ")
        where = []; plist = []
        if class_id and class_id.isdigit():
            where.append("s.class_id = ?"); plist.append(int(class_id))
        if course_id and course_id.isdigit():
            where.append("s.course_id = ?"); plist.append(int(course_id))
        if where:
            query += "WHERE " + " AND ".join(where) + " "
        query += "ORDER BY a.date DESC, s.last_name, s.first_name"
        rows = cur.execute(query, plist).fetchall()
        conn.close()
        context = {
            "classes": classes,
            "courses": courses,
            "class_id": class_id,
            "course_id": course_id,
            "rows": rows,
        }
        self.render("admin_attendance.html", context)

    def _handle_admin_log(self, params):
        action_filter = params.get('action', [''])[0]
        conn = get_db_connection()
        cur = conn.cursor()
        base = "SELECT id, timestamp, action, table_name, record_id, field_name, old_value, new_value, comment FROM change_log"
        plist = []
        if action_filter:
            base += " WHERE action=?"
            plist.append(action_filter)
        base += " ORDER BY id DESC LIMIT 200"
        rows = cur.execute(base, tuple(plist)).fetchall()
        conn.close()

        context = {
            "logs": rows,
            "action_filter": action_filter,
            "actions": ['import', 'manual']
        }
        self.render("admin_log.html", context)

    # Logging helper
    def _log_change(self, table_name: str, record_id: int | None, field_name: str, old_value: str, new_value: str, action: str, comment: str | None):
        conn = get_db_connection(); cur = conn.cursor()
        with conn:
            cur.execute(
                "INSERT INTO change_log(action, table_name, record_id, field_name, old_value, new_value, comment) VALUES(?,?,?,?,?,?,?)",
                (action, table_name, record_id, field_name, old_value, new_value, comment)
            )
        conn.close()

    # ----- Assignments & CRUD -----
    def _post_class_assign_teacher(self):
        data = self._parse_post()
        cid = int(data.get('class_id','0') or '0')
        tid_raw = data.get('teacher_id','0') or '0'
        tid = int(tid_raw) if tid_raw.isdigit() else 0
        if cid<=0:
            return self._send_html("<h1>400</h1><p>Ungültige Daten.</p>", status=400)
        conn = get_db_connection(); cur = conn.cursor()
        with conn:
            if tid<=0:
                cur.execute("UPDATE classes SET teacher_id=NULL WHERE id=?", (cid,))
            else:
                row = cur.execute("SELECT id FROM teachers WHERE id=?", (tid,)).fetchone()
                if not row:
                    conn.close(); return self._send_html("<h1>400</h1><p>Lehrer existiert nicht.</p>", status=400)
                cur.execute("UPDATE classes SET teacher_id=? WHERE id=?", (tid, cid))
        conn.close()
        self._log_change('classes', cid, 'teacher_id', '', str(tid if tid>0 else None), 'manual', None)
        self._redirect('/classes')

    def _post_performance_delete(self):
        data = self._parse_post()
        try:
            pid = int(data.get('id','0') or '0')
        except ValueError:
            return self._send_html("<h1>400</h1><p>Ung&uuml;ltige ID.</p>", status=400)
        if pid <= 0:
            return self._send_html("<h1>400</h1><p>Ung&uuml;ltige ID.</p>", status=400)
        conn = get_db_connection(); cur = conn.cursor()
        with conn:
            cur.execute("DELETE FROM performance_queries WHERE id=?", (pid,))
        conn.close()
        self._log_change('performance_queries', pid, 'delete', '', '', 'manual', 'LA gelöscht')
        self._redirect('/leistungsabfragen')

    def _post_course_assign_leader(self):
        data = self._parse_post()
        course_id = int(data.get('course_id','0') or '0')
        leader_raw = data.get('leader_id','0') or '0'
        leader_id = int(leader_raw) if leader_raw.isdigit() else 0
        if course_id<=0:
            return self._send_html("<h1>400</h1><p>Ungültige Daten.</p>", status=400)
        conn = get_db_connection(); cur = conn.cursor()
        with conn:
            if leader_id<=0:
                cur.execute("UPDATE courses SET leader_id=NULL WHERE id=?", (course_id,))
            else:
                row = cur.execute("SELECT id FROM teachers WHERE id=?", (leader_id,)).fetchone()
                if not row:
                    conn.close(); return self._send_html("<h1>400</h1><p>Lehrer existiert nicht.</p>", status=400)
                cur.execute("UPDATE courses SET leader_id=? WHERE id=?", (leader_id, course_id))
        conn.close()
        self._log_change('courses', course_id, 'leader_id', '', str(leader_id if leader_id>0 else None), 'manual', None)
        self._redirect(f'/course?id={course_id}')

    def _post_enroll_update(self):
        data = self._parse_post()
        student_id = int(data.get("student_id","0") or "0")
        class_id = int(data.get("class_id","0") or "0")
        course_id_raw = data.get("course_id","0") or "0"
        course_id = int(course_id_raw) if course_id_raw.isdigit() else 0
        next_url = data.get("next") or "/"
        if student_id <= 0 or class_id <= 0:
            return self._send_html("<h1>400</h1><p>Ung&uuml;ltige Daten.</p>", status=400)

        conn = get_db_connection(); cur = conn.cursor()
        with conn:
            row = cur.execute("SELECT id FROM classes WHERE id=?", (class_id,)).fetchone()
            if not row:
                conn.close()
                return self._send_html("<h1>400</h1><p>Klasse existiert nicht.</p>", status=400)
            if course_id == 0:
                cur.execute("UPDATE students SET class_id=?, course_id=NULL WHERE id=?", (class_id, student_id))
            else:
                row = cur.execute("SELECT id FROM courses WHERE id=?", (course_id,)).fetchone()
                if not row:
                    conn.close()
                    return self._send_html("<h1>400</h1><p>Kurs existiert nicht.</p>", status=400)
                cur.execute("UPDATE students SET class_id=?, course_id=? WHERE id=?", (class_id, course_id, student_id))
        conn.close()
        self._log_change('students', student_id, 'enroll', '', f'class={class_id};course={course_id or None}', 'manual', None)
        self._redirect(next_url)

    def _post_student_save(self):
        data = self._parse_post()
        sid = int(data.get("id","0") or "0")
        first = (data.get("first_name") or "").strip()
        last  = (data.get("last_name") or "").strip()
        class_id_raw = data.get("class_id","0") or "0"
        course_id_raw = data.get("course_id","0") or "0"
        class_id = int(class_id_raw) if class_id_raw.isdigit() else 0
        course_id = int(course_id_raw) if course_id_raw.isdigit() else 0
        class_new = (data.get("class_name_new") or "").strip()
        course_new = (data.get("course_name_new") or "").strip()

        if sid <= 0 or not first or not last:
            return self._send_html("<h1>400</h1><p>Ung&uuml;ltige Eingaben.</p>", status=400)

        conn = get_db_connection(); cur = conn.cursor()
        old_row = cur.execute("SELECT first_name,last_name,class_id,course_id FROM students WHERE id=?", (sid,)).fetchone()
        with conn:
            if class_new:
                class_id = get_or_create_class(cur, class_new)
            if course_new:
                course_id = get_or_create_course(cur, course_new)
            if class_id <= 0:
                conn.close()
                return self._send_html("<h1>400</h1><p>Klasse fehlt/ung&uuml;ltig.</p>", status=400)
            if course_id == 0:
                cur.execute("UPDATE students SET first_name=?, last_name=?, class_id=?, course_id=NULL WHERE id=?",
                            (first, last, class_id, sid))
            else:
                cur.execute("UPDATE students SET first_name=?, last_name=?, class_id=?, course_id=? WHERE id=?",
                            (first, last, class_id, course_id, sid))
        conn.close()
        self._log_change('students', sid, 'update',
                         f"{old_row['first_name']} {old_row['last_name']},c={old_row['class_id']},k={old_row['course_id']}",
                         f"{first} {last},c={class_id},k={course_id or None}", 'manual', None)
        self._redirect(f"/student?id={sid}&msg=Gespeichert")

    def _post_course_create(self):
        data = self._parse_post()
        name = (data.get("course_name") or "").strip()
        leader_id_raw = data.get("leader_id","0") or "0"
        leader_id = int(leader_id_raw) if leader_id_raw.isdigit() else 0
        new_short = (data.get("new_teacher_short") or "").strip()
        new_name  = (data.get("new_teacher_name") or "").strip()

        if not name:
            return self._send_html("<h1>400</h1><p>Kursname fehlt.</p>", status=400)

        conn = get_db_connection(); cur = conn.cursor()
        with conn:
            if new_short or new_name:
                tid = create_teacher(cur, new_short, new_name)
                if tid: leader_id = tid
            row = cur.execute("SELECT id FROM courses WHERE name=?", (name,)).fetchone()
            if row:
                if leader_id > 0:
                    cur.execute("UPDATE courses SET leader_id=? WHERE id=?", (leader_id, row["id"]))
            else:
                cur.execute("INSERT INTO courses(name, class_id, leader_id) VALUES(?, NULL, ?)",
                            (name, leader_id if leader_id>0 else None))
        conn.close()
        self._log_change('courses', None, 'create', '', name, 'manual', None)
        self._redirect("/courses")

    def _post_student_create(self):
        data = self._parse_post()
        first = (data.get("first_name") or "").strip()
        last  = (data.get("last_name") or "").strip()
        class_id_raw = data.get("class_id","0") or "0"
        course_id_raw = data.get("course_id","0") or "0"
        class_id = int(class_id_raw) if class_id_raw.isdigit() else 0
        course_id = int(course_id_raw) if course_id_raw.isdigit() else 0
        if not (first and last and class_id>0):
            return self._send_html("<h1>400</h1><p>Pflichtfelder fehlen.</p>", status=400)
        conn = get_db_connection(); cur = conn.cursor()
        with conn:
            if course_id == 0:
                cur.execute("INSERT INTO students(first_name, last_name, class_id, course_id) VALUES(?,?,?,NULL)",
                            (first, last, class_id))
            else:
                cur.execute("INSERT INTO students(first_name, last_name, class_id, course_id) VALUES(?,?,?,?)",
                            (first, last, class_id, course_id))
        conn.close()
        self._log_change('students', None, 'create', '', f"{first} {last}", 'manual', None)
        self._redirect("/students")

    def _post_student_delete(self):
        data = self._parse_post()
        sid = int(data.get("id","0") or "0")
        if sid <= 0:
            return self._send_html("<h1>400</h1><p>ID fehlt.</p>", status=400)
        conn = get_db_connection(); cur = conn.cursor()
        with conn:
            cur.execute("DELETE FROM students WHERE id=?", (sid,))
        conn.close()
        self._log_change('students', sid, 'delete', '', '', 'manual', None)
        self._redirect("/students")

# =========================
# HTTP Server
# =========================
def run(server_class=HTTPServer, handler_class=SchoolHTTPRequestHandler, port=8000):
    server_address = ("", port)
    httpd = server_class(server_address, handler_class)
    print(f"Serving HTTP on port {port} (database: {DB_PATH}) ...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped")
    finally:
        httpd.server_close()

if __name__ == "__main__":
    run()