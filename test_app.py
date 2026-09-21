import os
import sqlite3
import tempfile
import unittest
from unittest import mock

import app as annotation_app


class AnnotationFlowTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "annotations.db")
        self.patches = [
            mock.patch.object(annotation_app, "DATA_DIR", self.temp_dir.name),
            mock.patch.object(annotation_app, "DB_PATH", self.db_path),
        ]
        for patch in self.patches:
            patch.start()
        self.app = annotation_app.create_app()
        self.app.config.update(TESTING=True, SECRET_KEY="test-secret")
        self.client = self.app.test_client()
        self.run_id = "run-1"
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                """
                INSERT INTO runs(run_id, participant_id, video_id, target_side,
                                 media_mode, created_at_utc)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (self.run_id, "participant-1", "video-1", "left", "video", "now"),
            )
        self._set_session()

    def tearDown(self):
        for patch in reversed(self.patches):
            patch.stop()
        self.temp_dir.cleanup()

    def _set_session(self, **updates):
        values = {
            "participant_id": "participant-1",
            "run_id": self.run_id,
            "video_id": "video-1",
            "video_source_url": "https://example.com/video.mp4",
            "target_side": "left",
            "audio_only": False,
            "video_only": False,
            "segment_idx": 0,
            "duration_sec": None,
            "n_segments": None,
            "lang": "en",
        }
        values.update(updates)
        with self.client.session_transaction() as session:
            session.clear()
            session.update(values)

    @staticmethod
    def _valid_segment_payload(**updates):
        keys = (
            "anger", "compassion", "joy", "fear_anxiety", "sadness",
            "hide_feelings", "different_than_felt",
        )
        payload = {"segment_idx": "0", "media_duration_sec": "30", "felt_primary": "calm"}
        for key in keys:
            payload[f"exp_{key}"] = "4"
            payload[f"touch_exp_{key}"] = "1"
        payload["dispute_outcome_prediction"] = "4"
        payload["touch_dispute_outcome_prediction"] = "1"
        payload.update(updates)
        return payload

    def test_audio_source_can_use_composite_as_timing_master(self):
        source_path = os.path.join(self.temp_dir.name, "audio_sources.txt")
        with open(source_path, "w", encoding="utf-8") as source_file:
            source_file.write(
                "session-2_group-298,https://example.com/buyer.wav,"
                "https://example.com/seller.wav,560,0,"
                "s3://kodis-video/composite-298.mp4\n"
            )

        with mock.patch.object(annotation_app, "AUDIO_LIST_PATH", source_path):
            pool = annotation_app.load_audio_pool_from_config()

        self.assertEqual(len(pool), 1)
        self.assertEqual(pool[0]["audio_master_s3_bucket"], "kodis-video")
        self.assertEqual(pool[0]["audio_master_s3_key"], "composite-298.mp4")

    def test_audio_task_renders_composite_master_and_two_analysis_tracks(self):
        self._set_session(
            audio_only=True,
            audio_channel_1_url="https://example.com/buyer.wav",
            audio_channel_2_url="https://example.com/seller.wav",
            audio_master_s3_bucket="kodis-video",
            audio_master_s3_key="composite-298.mp4",
        )
        with mock.patch.object(
            annotation_app,
            "resolve_video_source",
            return_value="https://example.com/composite.mp4",
        ):
            response = self.client.get("/task")

        self.assertEqual(response.status_code, 200)
        page = response.get_data(as_text=True)
        self.assertIn('id="vid"', page)
        self.assertIn('src="https://example.com/composite.mp4"', page)
        self.assertIn('id="audioChannel1"', page)
        self.assertIn('id="audioChannel2"', page)

    def test_submission_initializes_duration_when_background_request_failed(self):
        response = self.client.post("/submit_segment", data=self._valid_segment_payload())
        self.assertEqual(response.status_code, 302)
        self.assertIn("/post_dialog", response.location)
        with sqlite3.connect(self.db_path) as db:
            run = db.execute(
                "SELECT duration_sec, n_segments FROM runs WHERE run_id=?", (self.run_id,)
            ).fetchone()
            count = db.execute(
                "SELECT COUNT(*) FROM segment_annotations WHERE run_id=?", (self.run_id,)
            ).fetchone()[0]
        self.assertEqual(run, (30.0, 1))
        self.assertEqual(count, 1)

    def test_repeated_submission_is_rejected_without_copying_answers(self):
        payload = self._valid_segment_payload()
        self.assertEqual(self.client.post("/submit_segment", data=payload).status_code, 302)
        self.assertEqual(self.client.post("/submit_segment", data=payload).status_code, 409)
        with sqlite3.connect(self.db_path) as db:
            count = db.execute(
                "SELECT COUNT(*) FROM segment_annotations WHERE run_id=?", (self.run_id,)
            ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_database_constraint_returns_conflict_for_duplicate_segment(self):
        self._set_session(duration_sec=30.0, n_segments=1)
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                """
                INSERT INTO segment_annotations(
                    run_id, segment_idx, ratings_json, open_text, created_at_utc
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (self.run_id, 0, "{}", "", "now"),
            )
        response = self.client.post("/submit_segment", data=self._valid_segment_payload())
        self.assertEqual(response.status_code, 409)

    def test_out_of_range_rating_is_not_saved(self):
        response = self.client.post(
            "/submit_segment",
            data=self._valid_segment_payload(exp_anger="99"),
        )
        self.assertEqual(response.status_code, 200)
        with sqlite3.connect(self.db_path) as db:
            count = db.execute("SELECT COUNT(*) FROM segment_annotations").fetchone()[0]
        self.assertEqual(count, 0)

    def test_final_questionnaire_requires_all_segments(self):
        self._set_session(duration_sec=120.0, n_segments=3)
        response = self.client.get("/post_dialog")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/task", response.location)

    def test_non_finite_duration_is_rejected(self):
        response = self.client.post("/init_video", data={"duration_sec": "nan"})
        self.assertEqual(response.status_code, 400)

    def test_export_requires_configured_bearer_token(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ADMIN_EXPORT_TOKEN", None)
            self.assertEqual(self.client.get("/admin/exports.csv").status_code, 503)

        with mock.patch.dict(os.environ, {"ADMIN_EXPORT_TOKEN": "export-secret"}):
            self.assertEqual(self.client.get("/admin/exports.csv").status_code, 401)
            response = self.client.get(
                "/admin/exports.csv",
                headers={"Authorization": "Bearer export-secret"},
            )
            self.assertEqual(response.status_code, 200)
            self.assertIn("participant_id", response.get_data(as_text=True))


if __name__ == "__main__":
    unittest.main()
