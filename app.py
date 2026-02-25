import os
import math
import uuid
import random
import sqlite3
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, session, g, abort

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(APP_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "annotations.db")

SEGMENT_SECONDS = 60  # 1-minute segments

# --- Configure your video pool here ---
# Put MP4s in static/videos/
VIDEO_POOL = [
    {"video_id": "sample_001", "path": "videos/sample.mp4"},
    # {"video_id": "dlg002", "path": "videos/dlg002.mp4"},
]

# Emotions: edit to match your self-report instrument
EMOTIONS = [
    "Anger",
    "Sadness",
    "Fear/Anxiety",
    "Joy/Happiness",
    "Disgust",
    "Surprise",
    "Compassion/Empathy",
    "Neutral",
]

# Post-dialog SVI single-item facets (edit labels as desired)
SVI_FACETS = [
    ("svi_deal", "How satisfied was the person with the deal/outcome they got?"),
    ("svi_relationship", "How satisfied was the person with the relationship with the other party?"),
    ("svi_process", "How satisfied was the person with the fairness of the process?"),
    ("svi_self", "How satisfied was the person with how they represented themselves?"),
]

REGIONS = [
    "United States",
    "Canada",
    "UK/Ireland",
    "Europe (other)",
    "Latin America",
    "East Asia",
    "South Asia",
    "Southeast Asia",
    "Middle East",
    "Africa",
    "Oceania",
    "Other / Prefer not to say",
]

def create_app():
    app = Flask(__name__)
    app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev_secret_change_me")

    os.makedirs(DATA_DIR, exist_ok=True)
    init_db()

    @app.before_request
    def load_db():
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row

    @app.teardown_request
    def close_db(exc):
        db = getattr(g, "db", None)
        if db is not None:
            db.close()

    @app.get("/")
    def index():
        # New participant link
        return render_template("index.html")

    @app.post("/start")
    def start():
        # Create a participant session
        participant_id = str(uuid.uuid4())
        assignment = random.choice(VIDEO_POOL)

        # Randomly choose target side: left/right
        target_side = random.choice(["left", "right"])

        # Create a run_id so multiple runs per person is possible later
        run_id = str(uuid.uuid4())

        session.clear()
        session["participant_id"] = participant_id
        session["run_id"] = run_id
        session["video_id"] = assignment["video_id"]
        session["video_path"] = assignment["path"]
        session["target_side"] = target_side

        # These will be set once browser reads video metadata
        session["duration_sec"] = None
        session["n_segments"] = None
        session["segment_idx"] = 0  # 0-based

        # Record run row
        g.db.execute(
            """
            INSERT INTO runs(run_id, participant_id, video_id, target_side, created_at_utc)
            VALUES(?,?,?,?,?)
            """,
            (run_id, participant_id, assignment["video_id"], target_side, datetime.utcnow().isoformat()),
        )
        g.db.commit()

        return redirect(url_for("demographics"))

    @app.get("/demographics")
    def demographics():
        ensure_session()
        return render_template("demographics.html", regions=REGIONS)

    @app.post("/demographics")
    def demographics_post():
        ensure_session()
        run_id = session["run_id"]

        payload = {
            "age": request.form.get("age", "").strip(),
            "gender": request.form.get("gender", "").strip(),
            "grew_up_region": request.form.get("grew_up_region", "").strip(),
            "grew_up_detail": request.form.get("grew_up_detail", "").strip(),
            "native_language": request.form.get("native_language", "").strip(),
        }

        g.db.execute(
            "UPDATE runs SET demographics_json=? WHERE run_id=?",
            (json_dumps(payload), run_id),
        )
        g.db.commit()

        return redirect(url_for("task"))

    @app.get("/task")
    def task():
        ensure_session()
        if session.get("n_segments") is None:
            # Need video metadata first; JS will call /init_video
            pass

        segment_idx = int(session.get("segment_idx", 0))
        n_segments = session.get("n_segments")
        if n_segments is not None and segment_idx >= int(n_segments):
            return redirect(url_for("post_dialog"))

        return render_template(
            "task.html",
            video_path=url_for("static", filename=session["video_path"]),
            target_side=session["target_side"],
            segment_idx=segment_idx,
            n_segments=n_segments,
            segment_seconds=SEGMENT_SECONDS,
            emotions=EMOTIONS,
            run_id=session["run_id"],
            video_id=session["video_id"],
        )

    @app.post("/init_video")
    def init_video():
        """
        Called once from the browser after it loads the video metadata and knows duration.
        """
        ensure_session()
        duration = request.form.get("duration_sec", type=float)
        if duration is None or duration <= 0:
            abort(400, "Invalid duration")

        n_segments = int(math.ceil(duration / SEGMENT_SECONDS))

        session["duration_sec"] = float(duration)
        session["n_segments"] = int(n_segments)

        # Save to DB
        g.db.execute(
            "UPDATE runs SET duration_sec=?, n_segments=? WHERE run_id=?",
            (float(duration), int(n_segments), session["run_id"]),
        )
        g.db.commit()

        return ("OK", 200)

    @app.post("/submit_segment")
    def submit_segment():
        ensure_session()
        run_id = session["run_id"]
        segment_idx = int(request.form.get("segment_idx", -1))
        if segment_idx < 0:
            abort(400, "Missing segment_idx")

        # Collect emotion ratings 1..7
        ratings = {}
        for e in EMOTIONS:
            key = f"emo_{slug(e)}"
            ratings[e] = request.form.get(key, "").strip()

        open_text = request.form.get("open_text", "").strip()

        # Basic validation: require all ratings
        if any(ratings[e] == "" for e in EMOTIONS):
            # Re-render with an error
            return render_template(
                "task.html",
                video_path=url_for("static", filename=session["video_path"]),
                target_side=session["target_side"],
                segment_idx=segment_idx,
                n_segments=session.get("n_segments"),
                segment_seconds=SEGMENT_SECONDS,
                emotions=EMOTIONS,
                run_id=session["run_id"],
                video_id=session["video_id"],
                error="Please answer all emotion ratings before continuing.",
            )

        g.db.execute(
            """
            INSERT INTO segment_annotations(
                run_id, segment_idx, ratings_json, open_text, created_at_utc
            ) VALUES (?,?,?,?,?)
            """,
            (run_id, segment_idx, json_dumps(ratings), open_text, datetime.utcnow().isoformat()),
        )
        g.db.commit()

        # Advance to next segment
        session["segment_idx"] = segment_idx + 1
        return redirect(url_for("task"))

    @app.get("/post_dialog")
    def post_dialog():
        ensure_session()
        return render_template(
            "post.html",
            emotions=EMOTIONS,
            regions=REGIONS,
            svi_facets=SVI_FACETS,
            target_side=session["target_side"],
        )

    @app.post("/post_dialog")
    def post_dialog_post():
        ensure_session()
        run_id = session["run_id"]

        # Overall emotion ratings
        overall = {}
        for e in EMOTIONS:
            key = f"overall_{slug(e)}"
            overall[e] = request.form.get(key, "").strip()

        if any(overall[e] == "" for e in EMOTIONS):
            return render_template(
                "post.html",
                emotions=EMOTIONS,
                regions=REGIONS,
                svi_facets=SVI_FACETS,
                target_side=session["target_side"],
                error="Please answer all overall emotion ratings.",
            )

        origin_guess = {
            "origin_region": request.form.get("origin_region", "").strip(),
            "origin_detail": request.form.get("origin_detail", "").strip(),
        }

        svi = {}
        for key, _label in SVI_FACETS:
            svi[key] = request.form.get(key, "").strip()

        if any(svi[k] == "" for k, _ in SVI_FACETS):
            return render_template(
                "post.html",
                emotions=EMOTIONS,
                regions=REGIONS,
                svi_facets=SVI_FACETS,
                target_side=session["target_side"],
                error="Please answer all SVI questions.",
            )

        payload = {
            "overall_emotions": overall,
            "origin_guess": origin_guess,
            "svi": svi,
        }

        completion_code = make_completion_code()

        g.db.execute(
            "UPDATE runs SET post_json=?, completion_code=?, finished_at_utc=? WHERE run_id=?",
            (json_dumps(payload), completion_code, datetime.utcnow().isoformat(), run_id),
        )
        g.db.commit()

        return redirect(url_for("done"))

    @app.get("/done")
    def done():
        ensure_session()
        run_id = session["run_id"]
        row = g.db.execute("SELECT completion_code FROM runs WHERE run_id=?", (run_id,)).fetchone()
        code = row["completion_code"] if row else None
        return render_template("done.html", code=code)

    @app.get("/admin/exports.csv")
    def export_csv():
        """
        Minimal export endpoint (no auth). Add auth before real deployment.
        """
        rows = g.db.execute(
            """
            SELECT r.*, sa.segment_idx, sa.ratings_json, sa.open_text
            FROM runs r
            LEFT JOIN segment_annotations sa ON sa.run_id = r.run_id
            ORDER BY r.created_at_utc, sa.segment_idx
            """
        ).fetchall()

        # Build a quick CSV response
        import csv
        from io import StringIO
        from flask import Response

        out = StringIO()
        w = csv.writer(out)
        w.writerow([
            "run_id","participant_id","video_id","target_side","created_at_utc",
            "duration_sec","n_segments","demographics_json",
            "segment_idx","ratings_json","open_text",
            "post_json","completion_code","finished_at_utc"
        ])
        for r in rows:
            w.writerow([
                r["run_id"], r["participant_id"], r["video_id"], r["target_side"], r["created_at_utc"],
                r["duration_sec"], r["n_segments"], r["demographics_json"],
                r["segment_idx"], r["ratings_json"], r["open_text"],
                r["post_json"], r["completion_code"], r["finished_at_utc"]
            ])
        return Response(out.getvalue(), mimetype="text/csv")

    return app


# ----------------- Helpers -----------------

def ensure_session():
    if "run_id" not in session:
        abort(403, "No active session. Please start from the home page.")

def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            participant_id TEXT,
            video_id TEXT,
            target_side TEXT,
            created_at_utc TEXT,
            duration_sec REAL,
            n_segments INTEGER,
            demographics_json TEXT,
            post_json TEXT,
            completion_code TEXT,
            finished_at_utc TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS segment_annotations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            segment_idx INTEGER,
            ratings_json TEXT,
            open_text TEXT,
            created_at_utc TEXT,
            FOREIGN KEY(run_id) REFERENCES runs(run_id)
        )
        """
    )

    conn.commit()
    conn.close()

def slug(s: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in s).strip("_")

def make_completion_code() -> str:
    # Short human-typable code
    return "A-" + uuid.uuid4().hex[:10].upper()

def json_dumps(obj) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False)

if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=5000, debug=True)