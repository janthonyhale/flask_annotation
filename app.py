import os
import math
import uuid
import random
import sqlite3
import hashlib
from datetime import datetime
from urllib.parse import urlparse, parse_qs, unquote

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from flask import Flask, render_template, request, redirect, url_for, session, g, abort

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(APP_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "annotations.db")

SEGMENT_SECONDS = 60  # each snippet is 60 seconds long
SEGMENT_OVERLAP_SECONDS = 2  # consecutive snippets overlap by 2 seconds
SEGMENT_STRIDE_SECONDS = SEGMENT_SECONDS - SEGMENT_OVERLAP_SECONDS

if SEGMENT_STRIDE_SECONDS <= 0:
    raise ValueError("SEGMENT_OVERLAP_SECONDS must be smaller than SEGMENT_SECONDS")

# --- Configure your video pool here ---
# Add one entry per line in data/video_sources.txt.
# Entry formats:
#   - Full URL: https://...
#   - S3 object key or filename: folder/my_video.mp4
#   - S3 URI: s3://bucket-name/folder/my_video.mp4
#   - Explicit ID + source: my_video,https://... or my_video,s3://bucket/key.mp4
#
# For S3 entries, we generate presigned URLs at request time.
S3_BUCKET_NAME = os.environ.get("VIDEO_S3_BUCKET", "kodis-video")
S3_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-west-2"
S3_PRESIGN_EXPIRES = int(os.environ.get("VIDEO_URL_EXPIRES_SEC", "7200"))
VIDEO_LIST_PATH = os.path.join(DATA_DIR, "video_sources.txt")

# Legacy local static fallback (if no configured source list exists).
# Supported extensions: .mp4, .webm, .mov, .m4v
VIDEO_EXTENSIONS = {".mp4", ".webm", ".mov", ".m4v"}

# Cache S3 metadata duration lookups per (bucket, key) for this process.
S3_DURATION_CACHE: dict[tuple[str, str], float | None] = {}
S3_DURATION_METADATA_KEYS_SEC = (
    "duration_sec",
    "duration_seconds",
)
S3_DURATION_METADATA_KEYS_MIN = (
    "duration_min",
    "duration_mins",
    "duration_minutes",
)

# Optional target length matching tolerance for ?t=<minutes>
TARGET_DURATION_TOLERANCE_SEC = 5 * 60


def load_video_pool_from_static() -> list[dict]:
    videos_dir = os.path.join(APP_DIR, "static", "videos")
    if not os.path.isdir(videos_dir):
        return []

    pool = []
    for name in sorted(os.listdir(videos_dir)):
        path = os.path.join(videos_dir, name)
        if not os.path.isfile(path):
            continue
        ext = os.path.splitext(name)[1].lower()
        if ext not in VIDEO_EXTENSIONS:
            continue

        video_id = os.path.splitext(name)[0]
        pool.append({"video_id": video_id, "path": f"videos/{name}"})
    return pool


def parse_video_source_line(raw_line: str, line_number: int) -> dict | None:
    line = raw_line.strip()
    if not line or line.startswith("#"):
        return None

    video_id = None
    source = line

    if "," in line:
        left, right = line.split(",", 1)
        if left.strip() and right.strip():
            video_id = left.strip()
            source = right.strip()

    parsed_entry = None
    filename_source = source

    if source.startswith(("http://", "https://")):
        parsed_entry = {"url": source}
    elif source.startswith("s3://"):
        parsed = urlparse(source)
        bucket = parsed.netloc.strip()
        object_key = parsed.path.lstrip("/")
        if not bucket or not object_key:
            return None
        parsed_entry = {"s3_bucket": bucket, "s3_key": object_key}
        filename_source = object_key
    else:
        object_key = source.lstrip("/")
        if not object_key:
            return None
        parsed_entry = {"s3_bucket": S3_BUCKET_NAME, "s3_key": object_key}
        filename_source = object_key

    if not video_id:
        if filename_source.startswith(("http://", "https://")):
            filename = os.path.basename(urlparse(filename_source).path).strip()
        else:
            filename = os.path.basename(filename_source).strip()
        video_id = os.path.splitext(filename)[0] if filename else ""

    if not video_id:
        short_hash = hashlib.sha1(f"{line_number}:{line}".encode("utf-8")).hexdigest()[:8]
        video_id = f"video_{short_hash}"

    parsed_entry["video_id"] = video_id
    return parsed_entry


def load_video_pool_from_config() -> list[dict]:
    if not os.path.isfile(VIDEO_LIST_PATH):
        return []

    pool = []
    seen_ids = set()
    with open(VIDEO_LIST_PATH, "r", encoding="utf-8") as f:
        for idx, raw_line in enumerate(f, start=1):
            parsed = parse_video_source_line(raw_line, idx)
            if not parsed:
                continue
            original_id = parsed["video_id"]
            dedup_id = original_id
            n = 2
            while dedup_id in seen_ids:
                dedup_id = f"{original_id}_{n}"
                n += 1
            parsed["video_id"] = dedup_id
            seen_ids.add(dedup_id)
            pool.append(parsed)
    return pool


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
    "Afghanistan",
    "Albania",
    "Algeria",
    "Andorra",
    "Angola",
    "Antigua and Barbuda",
    "Argentina",
    "Armenia",
    "Australia",
    "Austria",
    "Azerbaijan",
    "Bahamas",
    "Bahrain",
    "Bangladesh",
    "Barbados",
    "Belarus",
    "Belgium",
    "Belize",
    "Benin",
    "Bhutan",
    "Bolivia",
    "Bosnia and Herzegovina",
    "Botswana",
    "Brazil",
    "Brunei",
    "Bulgaria",
    "Burkina Faso",
    "Burundi",
    "Cabo Verde",
    "Cambodia",
    "Cameroon",
    "Canada",
    "Central African Republic",
    "Chad",
    "Chile",
    "China",
    "Colombia",
    "Comoros",
    "Congo",
    "Costa Rica",
    "Cote d'Ivoire",
    "Croatia",
    "Cuba",
    "Cyprus",
    "Czechia",
    "Democratic Republic of the Congo",
    "Denmark",
    "Djibouti",
    "Dominica",
    "Dominican Republic",
    "Ecuador",
    "Egypt",
    "El Salvador",
    "Equatorial Guinea",
    "Eritrea",
    "Estonia",
    "Eswatini",
    "Ethiopia",
    "Fiji",
    "Finland",
    "France",
    "Gabon",
    "Gambia",
    "Georgia",
    "Germany",
    "Ghana",
    "Greece",
    "Grenada",
    "Guatemala",
    "Guinea",
    "Guinea-Bissau",
    "Guyana",
    "Haiti",
    "Honduras",
    "Hungary",
    "Iceland",
    "India",
    "Indonesia",
    "Iran",
    "Iraq",
    "Ireland",
    "Israel",
    "Italy",
    "Jamaica",
    "Japan",
    "Jordan",
    "Kazakhstan",
    "Kenya",
    "Kiribati",
    "Kuwait",
    "Kyrgyzstan",
    "Laos",
    "Latvia",
    "Lebanon",
    "Lesotho",
    "Liberia",
    "Libya",
    "Liechtenstein",
    "Lithuania",
    "Luxembourg",
    "Madagascar",
    "Malawi",
    "Malaysia",
    "Maldives",
    "Mali",
    "Malta",
    "Marshall Islands",
    "Mauritania",
    "Mauritius",
    "Mexico",
    "Micronesia",
    "Moldova",
    "Monaco",
    "Mongolia",
    "Montenegro",
    "Morocco",
    "Mozambique",
    "Myanmar",
    "Namibia",
    "Nauru",
    "Nepal",
    "Netherlands",
    "New Zealand",
    "Nicaragua",
    "Niger",
    "Nigeria",
    "North Korea",
    "North Macedonia",
    "Norway",
    "Oman",
    "Pakistan",
    "Palau",
    "Panama",
    "Papua New Guinea",
    "Paraguay",
    "Peru",
    "Philippines",
    "Poland",
    "Portugal",
    "Qatar",
    "Romania",
    "Russia",
    "Rwanda",
    "Saint Kitts and Nevis",
    "Saint Lucia",
    "Saint Vincent and the Grenadines",
    "Samoa",
    "San Marino",
    "Sao Tome and Principe",
    "Saudi Arabia",
    "Senegal",
    "Serbia",
    "Seychelles",
    "Sierra Leone",
    "Singapore",
    "Slovakia",
    "Slovenia",
    "Solomon Islands",
    "Somalia",
    "South Africa",
    "South Korea",
    "South Sudan",
    "Spain",
    "Sri Lanka",
    "Sudan",
    "Suriname",
    "Sweden",
    "Switzerland",
    "Syria",
    "Tajikistan",
    "Tanzania",
    "Thailand",
    "Timor-Leste",
    "Togo",
    "Tonga",
    "Trinidad and Tobago",
    "Tunisia",
    "Turkey",
    "Turkmenistan",
    "Tuvalu",
    "Uganda",
    "Ukraine",
    "United Arab Emirates",
    "United Kingdom",
    "United States",
    "Uruguay",
    "Uzbekistan",
    "Vanuatu",
    "Vatican City",
    "Venezuela",
    "Vietnam",
    "Yemen",
    "Zambia",
    "Zimbabwe",
    "Prefer not to say",
]



GENDERS = [
    "Female",
    "Male",
    "Non-binary",
    "Another identity",
    "Prefer not to say",
]

US_STATES = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware",
    "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky",
    "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
    "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York",
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island",
    "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
    "West Virginia", "Wisconsin", "Wyoming", "District of Columbia", "Not in the United States",
]


CHINA_PROVINCES = [
    "Anhui", "Beijing", "Chongqing", "Fujian", "Gansu", "Guangdong", "Guangxi", "Guizhou", "Hainan", "Hebei", "Heilongjiang", "Henan", "Hong Kong", "Hubei", "Hunan", "Inner Mongolia", "Jiangsu", "Jiangxi", "Jilin", "Liaoning", "Macau", "Ningxia", "Qinghai", "Shaanxi", "Shandong", "Shanghai", "Shanxi", "Sichuan", "Tianjin", "Tibet", "Xinjiang", "Yunnan", "Zhejiang",
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
        prefill_id = request.args.get("id", "").strip()
        t_value = request.args.get("t", "").strip()
        return render_template("index.html", prefill_id=prefill_id, t_value=t_value)

    @app.post("/start")
    def start():
        participant_id = request.form.get("participant_id", "").strip()
        if not participant_id:
            abort(400, "Unique ID is required")

        t_minutes = parse_target_minutes(request.form.get("t", "").strip())
        video_pool = load_video_pool_from_config()
        if not video_pool:
            video_pool = load_video_pool_from_static()
        if not video_pool:
            return render_template(
                "index.html",
                error=(
                    "No videos found. Add entries to data/video_sources.txt "
                    "(one S3 key or URL per line), or add local files to static/videos."
                ),
                prefill_id=participant_id,
                t_value=request.form.get("t", "").strip(),
            )

        assignment, target_side = choose_video_assignment(g.db, participant_id, video_pool, t_minutes)
        run_id = str(uuid.uuid4())

        session.clear()
        session["participant_id"] = participant_id
        session["run_id"] = run_id
        session["video_id"] = assignment["video_id"]
        session["video_source_url"] = assignment.get("url")
        session["video_s3_bucket"] = assignment.get("s3_bucket")
        session["video_s3_key"] = assignment.get("s3_key")
        session["video_path"] = assignment.get("path")
        session["target_side"] = target_side
        session["duration_sec"] = None
        session["n_segments"] = None
        session["segment_idx"] = 0
        session["target_minutes"] = t_minutes

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
        return render_template("demographics.html", regions=REGIONS, genders=GENDERS, us_states=US_STATES, china_provinces=CHINA_PROVINCES)

    @app.post("/demographics")
    def demographics_post():
        ensure_session()
        run_id = session["run_id"]

        age_raw = request.form.get("age", "").strip()
        try:
            age = int(age_raw)
        except ValueError:
            return render_template(
                "demographics.html",
                regions=REGIONS,
                genders=GENDERS,
                us_states=US_STATES,
                china_provinces=CHINA_PROVINCES,
                error="Please enter age as a number.",
            )

        if age < 18 or age > 120:
            return render_template(
                "demographics.html",
                regions=REGIONS,
                genders=GENDERS,
                us_states=US_STATES,
                china_provinces=CHINA_PROVINCES,
                error="Please enter an age between 18 and 120.",
            )

        grew_up_state = request.form.get("grew_up_state", "").strip()
        grew_up_province = request.form.get("grew_up_province", "").strip()
        grew_up_region = request.form.get("grew_up_region", "").strip()
        grew_up_detail = grew_up_state if grew_up_region == "United States" else (grew_up_province if grew_up_region == "China" else "")

        if grew_up_region == "United States" and not grew_up_state:
            return render_template(
                "demographics.html",
                regions=REGIONS,
                genders=GENDERS,
                us_states=US_STATES,
                china_provinces=CHINA_PROVINCES,
                error="Please select a U.S. state.",
            )

        if grew_up_region == "China" and not grew_up_province:
            return render_template(
                "demographics.html",
                regions=REGIONS,
                genders=GENDERS,
                us_states=US_STATES,
                china_provinces=CHINA_PROVINCES,
                error="Please select a Chinese province.",
            )
        payload = {
            "age": age,
            "gender": request.form.get("gender", "").strip(),
            "grew_up_region": grew_up_region,
            "grew_up_state": grew_up_state,
            "grew_up_province": grew_up_province,
            # Backward-compatible alias for previous exports/consumers
            "grew_up_detail": grew_up_detail,
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
        segment_idx = int(session.get("segment_idx", 0))
        duration_sec = float(session["duration_sec"]) if session.get("duration_sec") is not None else None
        n_segments = session.get("n_segments")
        if not n_segments and duration_sec is not None:
            n_segments = compute_n_segments(duration_sec)
        if n_segments is not None and segment_idx >= int(n_segments):
            return redirect(url_for("post_dialog"))

        segment_start_sec, segment_end_sec = get_segment_bounds(segment_idx, duration_sec)

        video_url = resolve_video_source(
            {
                "url": session.get("video_source_url"),
                "s3_bucket": session.get("video_s3_bucket"),
                "s3_key": session.get("video_s3_key"),
                "path": session.get("video_path"),
            }
        )

        return render_template(
            "task.html",
            video_path=video_url,
            video_mime=guess_video_mime(video_url) if video_url else "video/mp4",
            target_side=session["target_side"],
            segment_idx=segment_idx,
            n_segments=n_segments,
            segment_seconds=SEGMENT_SECONDS,
            segment_overlap_seconds=SEGMENT_OVERLAP_SECONDS,
            segment_stride_seconds=SEGMENT_STRIDE_SECONDS,
            segment_start_sec=segment_start_sec,
            segment_end_sec=segment_end_sec,
            emotions=EMOTIONS,
            run_id=session["run_id"],
            video_id=session["video_id"],
        )

    @app.post("/init_video")
    def init_video():
        ensure_session()
        duration = request.form.get("duration_sec", type=float)
        if duration is None or duration <= 0:
            abort(400, "Invalid duration")

        n_segments = compute_n_segments(float(duration))
        session["duration_sec"] = float(duration)
        session["n_segments"] = int(n_segments)

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

        current_segment_idx = int(session.get("segment_idx", 0))
        if segment_idx != current_segment_idx:
            segment_idx = current_segment_idx

        n_segments = session.get("n_segments")
        if n_segments is not None and segment_idx >= int(n_segments):
            return redirect(url_for("post_dialog"))

        ratings = {}
        for e in EMOTIONS:
            key = f"emo_{slug(e)}"
            ratings[e] = request.form.get(key, "").strip()

        open_text = request.form.get("open_text", "").strip()


        exp_items = [
            ("anger", "I showed anger at partner"),
            ("compassion", "I showed compassion for partner"),
            ("joy", "I showed joy"),
            ("fear_anxiety", "I showed fear/anxiety"),
            ("sadness", "I showed sadness"),
            ("hide_feelings", "I tried to hide my feelings"),
            ("different_than_felt", "I showed emotion different than I felt"),
        ]
        exp_ratings = {}
        for slug_key, _label in exp_items:
            form_key = f"exp_{slug_key}"
            exp_ratings[slug_key] = request.form.get(form_key, "").strip()

        felt_primary = request.form.get("felt_primary", "").strip()


        moved_exp_ok = all(request.form.get(f"touch_exp_{k}") == "1" for k, _ in exp_items)
        if any(exp_ratings[k] == "" for k, _ in exp_items) or not moved_exp_ok or felt_primary == "":
            video_url = resolve_video_source(
                {
                    "url": session.get("video_source_url"),
                    "s3_bucket": session.get("video_s3_bucket"),
                    "s3_key": session.get("video_s3_key"),
                    "path": session.get("video_path"),
                }
            )
            duration_sec = float(session["duration_sec"]) if session.get("duration_sec") is not None else None
            segment_start_sec, segment_end_sec = get_segment_bounds(segment_idx, duration_sec)
            return render_template(
                "task.html",
                video_path=video_url,
                video_mime=guess_video_mime(video_url) if video_url else "video/mp4",
                target_side=session["target_side"],
                segment_idx=segment_idx,
                n_segments=session.get("n_segments"),
                segment_seconds=SEGMENT_SECONDS,
                segment_overlap_seconds=SEGMENT_OVERLAP_SECONDS,
                segment_stride_seconds=SEGMENT_STRIDE_SECONDS,
                segment_start_sec=segment_start_sec,
                segment_end_sec=segment_end_sec,
                emotions=EMOTIONS,
                run_id=session["run_id"],
                video_id=session["video_id"],
                error="Please answer all segment questions, move every slider at least once, and provide your primary felt emotion(s) before continuing.",
            )

        g.db.execute(
            """
            INSERT INTO segment_annotations(
                run_id, segment_idx, ratings_json, open_text, created_at_utc
            ) VALUES (?,?,?,?,?)
            """,
            (run_id, segment_idx, json_dumps({"target_emotions": ratings, "expressed_items": exp_ratings, "felt_primary": felt_primary, "notes": open_text}), open_text, datetime.utcnow().isoformat()),
        )
        g.db.commit()

        next_segment_idx = segment_idx + 1
        session["segment_idx"] = next_segment_idx

        if n_segments is not None and next_segment_idx >= int(n_segments):
            return redirect(url_for("post_dialog"))

        return redirect(url_for("task"))

    @app.get("/post_dialog")
    def post_dialog():
        ensure_session()
        return render_template(
            "post.html",
            emotions=EMOTIONS,
            regions=REGIONS,
            us_states=US_STATES,
            china_provinces=CHINA_PROVINCES,
            svi_facets=SVI_FACETS,
            target_side=session["target_side"],
        )

    @app.post("/post_dialog")
    def post_dialog_post():
        ensure_session()
        run_id = session["run_id"]

        overall_items = [
            ("anger", "I showed anger at partner"),
            ("compassion", "I showed compassion for partner"),
            ("joy", "I showed joy"),
            ("fear_anxiety", "I showed fear/anxiety"),
            ("sadness", "I showed sadness"),
            ("hide_feelings", "I tried to hide my feelings"),
            ("different_than_felt", "I showed emotion different than I felt"),
        ]
        overall = {}
        for key, _label in overall_items:
            overall[key] = request.form.get(f"self_overall_{key}", "").strip()

        felt_primary_overall = request.form.get("felt_primary_overall", "").strip()
        moved_overall_ok = all(request.form.get(f"touch_self_overall_{key}") == "1" for key, _ in overall_items)

        svi = {}
        for key, _label in SVI_FACETS:
            svi[key] = request.form.get(key, "").strip()

        moved_svi_ok = all(request.form.get(f"touch_{key}") == "1" for key, _ in SVI_FACETS)

        origin_region = request.form.get("origin_region", "").strip()
        origin_state = request.form.get("origin_state", "").strip()
        origin_province = request.form.get("origin_province", "").strip()

        if origin_region == "United States" and not origin_state:
            return render_template(
                "post.html",
                emotions=EMOTIONS,
                regions=REGIONS,
                us_states=US_STATES,
                china_provinces=CHINA_PROVINCES,
                svi_facets=SVI_FACETS,
                target_side=session["target_side"],
                error="Please select a U.S. state.",
            )

        if origin_region == "China" and not origin_province:
            return render_template(
                "post.html",
                emotions=EMOTIONS,
                regions=REGIONS,
                us_states=US_STATES,
                china_provinces=CHINA_PROVINCES,
                svi_facets=SVI_FACETS,
                target_side=session["target_side"],
                error="Please select a Chinese province.",
            )

        origin_detail = origin_state if origin_region == "United States" else (origin_province if origin_region == "China" else "")
        origin_guess = {
            "origin_region": origin_region,
            "origin_state": origin_state,
            "origin_province": origin_province,
            # Backward-compatible alias for previous exports/consumers
            "origin_detail": origin_detail,
        }

        if (
            any(overall[k] == "" for k, _ in overall_items)
            or felt_primary_overall == ""
            or not moved_overall_ok
            or any(svi[k] == "" for k, _ in SVI_FACETS)
            or not moved_svi_ok
        ):
            return render_template(
                "post.html",
                emotions=EMOTIONS,
                regions=REGIONS,
                us_states=US_STATES,
                china_provinces=CHINA_PROVINCES,
                svi_facets=SVI_FACETS,
                target_side=session["target_side"],
                error="Please answer all final questions and interact with every slider at least once.",
            )

        payload = {
            "overall_emotions": overall,
            "felt_primary_overall": felt_primary_overall,
            "origin_guess": origin_guess,
            "svi": svi,
        }

        completion_code = make_completion_code(session["participant_id"])

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
        rows = g.db.execute(
            """
            SELECT r.*, sa.segment_idx, sa.ratings_json, sa.open_text
            FROM runs r
            LEFT JOIN segment_annotations sa ON sa.run_id = r.run_id
            ORDER BY r.created_at_utc, sa.segment_idx
            """
        ).fetchall()

        import csv
        from io import StringIO
        from flask import Response

        out = StringIO()
        w = csv.writer(out)
        w.writerow([
            "run_id", "participant_id", "video_id", "target_side", "created_at_utc",
            "duration_sec", "n_segments", "demographics_json",
            "segment_idx", "ratings_json", "open_text",
            "post_json", "completion_code", "finished_at_utc"
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


def get_s3_client():
    return boto3.client("s3", region_name=S3_REGION)


def create_presigned_s3_url(bucket_name: str, object_key: str, expires_in: int = S3_PRESIGN_EXPIRES) -> str:
    try:
        return get_s3_client().generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": bucket_name, "Key": object_key},
            ExpiresIn=expires_in,
            HttpMethod="GET",
        )
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(
            f"Could not create presigned URL for s3://{bucket_name}/{object_key}: {e}"
        )


def resolve_video_source(assignment: dict) -> str:
    if assignment.get("url"):
        return normalize_google_drive_url(assignment["url"])
    if assignment.get("s3_key"):
        bucket_name = assignment.get("s3_bucket") or S3_BUCKET_NAME
        return create_presigned_s3_url(bucket_name, assignment["s3_key"])
    if assignment.get("path"):
        return url_for("static", filename=assignment["path"])
    abort(500, "Invalid VIDEO_POOL entry: expected 'url', 's3_key', or 'path'.")



def parse_s3_duration_metadata(metadata: dict) -> float | None:
    if not metadata:
        return None

    for key in S3_DURATION_METADATA_KEYS_SEC:
        value = metadata.get(key)
        if value is None:
            continue
        try:
            duration_sec = float(str(value).strip())
        except (TypeError, ValueError):
            continue
        if duration_sec > 0:
            return duration_sec

    for key in S3_DURATION_METADATA_KEYS_MIN:
        value = metadata.get(key)
        if value is None:
            continue
        try:
            duration_min = float(str(value).strip())
        except (TypeError, ValueError):
            continue
        if duration_min > 0:
            return duration_min * 60.0

    return None


def get_s3_duration_seconds(bucket_name: str, object_key: str) -> float | None:
    cache_key = (bucket_name, object_key)
    if cache_key in S3_DURATION_CACHE:
        return S3_DURATION_CACHE[cache_key]

    try:
        response = get_s3_client().head_object(Bucket=bucket_name, Key=object_key)
    except (BotoCoreError, ClientError):
        S3_DURATION_CACHE[cache_key] = None
        return None

    duration_sec = parse_s3_duration_metadata(response.get("Metadata", {}))
    S3_DURATION_CACHE[cache_key] = duration_sec
    return duration_sec

def normalize_google_drive_url(url: str) -> str:
    """
    Convert common Google Drive sharing URLs into a more video-player-friendly direct URL.
    We preserve resource keys when present, since some files require them.
    """
    parsed = urlparse(url)
    if "drive.google.com" not in parsed.netloc:
        return url

    query = parse_qs(parsed.query)

    # /file/d/<id>/view?resourcekey=...
    path_parts = [p for p in parsed.path.split("/") if p]
    file_id = None
    if "file" in path_parts and "d" in path_parts:
        d_idx = path_parts.index("d")
        if d_idx + 1 < len(path_parts):
            file_id = unquote(path_parts[d_idx + 1])

    # /open?id=<id> or /uc?id=<id>
    if file_id is None:
        query_id = query.get("id", [])
        if query_id:
            file_id = query_id[0]

    if not file_id:
        return url

    base = f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
    resource_key = query.get("resourcekey", [])
    if resource_key:
        base += f"&resourcekey={resource_key[0]}"

    return base


# ----------------- Helpers -----------------

def ensure_session():
    if "run_id" not in session:
        abort(403, "No active session. Please start from the home page.")


def parse_target_minutes(raw: str):
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    if value <= 0:
        return None
    return value


def choose_video_assignment(db, participant_id: str, video_pool: list[dict], target_minutes=None) -> tuple[dict, str]:
    pair_counts = {
        (row["video_id"], row["target_side"]): int(row["cnt"])
        for row in db.execute(
            "SELECT video_id, target_side, COUNT(*) AS cnt FROM runs GROUP BY video_id, target_side"
        ).fetchall()
    }

    seen_pairs = {
        (row["video_id"], row["target_side"])
        for row in db.execute(
            "SELECT video_id, target_side FROM runs WHERE participant_id=?",
            (participant_id,),
        ).fetchall()
    }

    candidate_videos = list(video_pool)
    if target_minutes is not None:
        target_seconds = target_minutes * 60.0
        filtered = []
        for video in candidate_videos:
            duration_sec = None
            if video.get("s3_key"):
                bucket_name = video.get("s3_bucket") or S3_BUCKET_NAME
                duration_sec = get_s3_duration_seconds(bucket_name, video["s3_key"])
            if duration_sec is None:
                continue
            if abs(duration_sec - target_seconds) <= TARGET_DURATION_TOLERANCE_SEC:
                filtered.append(video)
        if filtered:
            candidate_videos = filtered

    candidate_pairs = []
    for video in candidate_videos:
        for side in ("left", "right"):
            pair_key = (video["video_id"], side)
            if pair_key in seen_pairs:
                continue
            candidate_pairs.append((video, side))

    if not candidate_pairs:
        abort(400, "No eligible video/side assignment remains for this participant.")

    min_count = min(pair_counts.get((video["video_id"], side), 0) for video, side in candidate_pairs)
    least_annotated_pairs = [
        (video, side)
        for video, side in candidate_pairs
        if pair_counts.get((video["video_id"], side), 0) == min_count
    ]
    return random.choice(least_annotated_pairs)


def compute_n_segments(duration_sec: float) -> int:
    if duration_sec <= 0:
        return 0
    if duration_sec <= SEGMENT_SECONDS:
        return 1
    return int(math.ceil((duration_sec - SEGMENT_SECONDS) / SEGMENT_STRIDE_SECONDS)) + 1


def get_segment_bounds(segment_idx: int, duration_sec: float | None) -> tuple[float, float | None]:
    start_sec = float(segment_idx * SEGMENT_STRIDE_SECONDS)
    end_sec = start_sec + SEGMENT_SECONDS
    if duration_sec is not None:
        end_sec = min(end_sec, float(duration_sec))
    return start_sec, end_sec


def guess_video_mime(video_path: str) -> str:
    parsed = urlparse(video_path)
    ext = os.path.splitext(parsed.path)[1].lower()
    return {
        ".mp4": "video/mp4",
        ".webm": "video/webm",
        ".mov": "video/quicktime",
        ".m4v": "video/mp4",
    }.get(ext, "video/mp4")


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


def hash_participant_id(participant_id: str) -> str:
    return hashlib.sha256(participant_id.encode("utf-8")).hexdigest()[:8].upper()


def make_completion_code(participant_id: str) -> str:
    hashed = hash_participant_id(participant_id)
    return f"A-{hashed}-{uuid.uuid4().hex[:6].upper()}"


def json_dumps(obj) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False)


if __name__ == "__main__":
    app = create_app()
    # threaded=True helps local dev handle multiple participants concurrently.
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True)