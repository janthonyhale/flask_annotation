# Flask Conversation Annotator

A Flask application for segment-by-segment emotion annotation of two-person
conversations. A task can present the original video, silent video, or a
two-channel audio visualization.

## Setup

From the repository root:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Start the development server:

```bash
.venv/bin/python app.py
```

The application is available at <http://localhost:8000/>.

The default Flask secret is intended only for local development. Set a unique
secret before deploying:

```bash
FLASK_SECRET_KEY="replace-with-a-random-secret" \
  .venv/bin/python app.py
```

## Testing

### Automated tests

Run the regression suite from the repository root:

```bash
.venv/bin/python -m unittest -v
```

The tests use a temporary SQLite database and do not modify
`data/annotations.db`. They currently check that:

- a segment submission can recover when the background media-duration request
  fails;
- repeated or stale segment submissions do not create mislabeled responses;
- ratings outside the permitted range are rejected;
- participants cannot open the final questionnaire before finishing every
  segment;
- invalid media durations are rejected; and
- CSV exports require the configured administrator token.

A successful run ends with output similar to:

```text
Ran 9 tests in ...

OK
```

### Browser smoke test

Manual tests write records to `data/annotations.db`. Back up that file first if
it already contains data. Use a clearly recognizable participant ID such as
`local-test-2026-09-20-01`, and use a new ID for each repeated test.

Start the server with development-only secrets:

```bash
FLASK_SECRET_KEY="local-test-secret" \
ADMIN_EXPORT_TOKEN="local-export-token" \
FLASK_DEBUG=true \
  .venv/bin/python app.py
```

Then perform this basic video-mode test:

1. Open <http://localhost:8000/> and enter a unique test participant ID.
2. Confirm that the consent page appears, followed by the demographics page.
3. Complete all required demographic fields. If you select the United States
   or China, confirm that the corresponding state or province field appears.
4. On the task page, confirm that the highlighted side matches the instruction
   and that the displayed segment count becomes a number after media metadata
   loads.
5. Press **Play**. Confirm that playback begins at the segment start and stops
   automatically at the displayed end time.
6. Confirm that **Continue** remains disabled until the entire segment has
   played, every slider has been touched, and the primary-emotion field is not
   empty.
7. Complete each segment. Later segments should begin 58 seconds after the
   preceding segment, producing a two-second overlap.
8. Confirm that the final questionnaire appears only after the final segment.
9. Complete every final slider and required field. Confirm that the completion
   page displays a nonempty code.

Repeat the relevant playback checks in the other modes:

| Mode | Test URL | Expected behavior |
| --- | --- | --- |
| Silent video | <http://localhost:8000/?video_only=true> | Video plays with audio forced off. |
| Paired audio | <http://localhost:8000/?audio_only=true> | Two speaker spheres appear and both audio channels follow their configured offsets. |
| Chinese audio | <http://localhost:8000/?audio_only=true&lang=cn> | Instructions and questionnaires appear in Chinese. |

For a quick playback test, temporarily configure a short media file of no more
than 60 seconds. A local video may be placed in `static/videos/`, but local
videos are used only when `data/video_sources.txt` has no active entries.
`static/videos/` and database files are ignored by Git.

### Verify the saved export

With the server running and `ADMIN_EXPORT_TOKEN` set, download the CSV:

```bash
curl --fail \
  -H "Authorization: Bearer local-export-token" \
  -o /tmp/annotation-export.csv \
  http://localhost:8000/admin/exports.csv
```

Open the CSV and find the test participant ID. Verify that:

- the run has the expected `video_id`, `target_side`, and `media_mode`;
- segment indexes start at `0` and continue through `n_segments - 1`;
- there is exactly one row for each `run_id` and `segment_idx` combination;
- `ratings_json` contains the submitted slider values, outcome prediction, and
  primary-emotion response; and
- a completed run has `post_json`, `completion_code`, and `finished_at_utc`.

Requests without the export token return `401`. If the server was started
without `ADMIN_EXPORT_TOKEN`, the export endpoint returns `503`.

### Common test problems

- **No videos found:** add an active source to `data/video_sources.txt`, or
  comment out all active entries and place a supported file in
  `static/videos/`.
- **An S3 video does not load:** check the AWS credentials, bucket name, region,
  object key, and permission to call `GetObject` and `HeadObject`.
- **Remote audio loads but does not play or animate:** confirm that its host
  permits cross-origin browser requests with an appropriate CORS policy.
- **Continue stays disabled:** let the segment reach its automatic stopping
  point, touch every slider at least once, and enter a primary emotion.
- **No assignment remains:** use a new participant ID. A participant is not
  assigned the same conversation and target-side pair twice.
- **Port 8000 is already occupied:** start the application with another port,
  such as `FLASK_PORT=8001 .venv/bin/python app.py`.

## Presentation modes

Presentation mode is selected per participant using the initial URL. The mode
is stored in that browser's session for the remainder of the task.

| Mode | URL |
| --- | --- |
| Video with audio (default) | `http://localhost:8000/` |
| Silent video | `http://localhost:8000/?video_only=true` |
| Paired audio visualization | `http://localhost:8000/?audio_only=true` |

If both mode parameters are true, audio-only mode takes precedence. Accepted
true values are `1`, `true`, `yes`, and `on`, case-insensitively.

Add `lang=cn` for the Chinese interface:

```text
http://localhost:8000/?audio_only=true&lang=cn
```

Optional initial parameters include:

- `id`: prefill the participant ID.
- `t`: request a target conversation length in minutes.

For example:

```text
http://localhost:8000/?audio_only=true&lang=cn&id=test-001
```

Use a new participant ID when repeating tests. The assignment system avoids
giving the same participant the same conversation/target-side pair twice.

## Video sources

Configure videos in
[`data/video_sources.txt`](data/video_sources.txt).
Each active line may contain:

```text
https://example.com/conversation.mp4
s3://bucket-name/path/conversation.mp4
path/in/default-bucket/conversation.mp4
conversation_id,https://example.com/conversation.mp4
```

For S3 object keys without an explicit bucket, the default bucket is
`kodis-video`. Override it with `VIDEO_S3_BUCKET`. Other relevant environment
variables are `AWS_REGION`, `AWS_DEFAULT_REGION`, and
`VIDEO_URL_EXPIRES_SEC`.

## Paired audio sources

Configure audio conversations in
[`data/audio_sources.txt`](data/audio_sources.txt).
Each active row may contain five comma-separated values for the legacy paired-WAV
player, or a sixth composite source for exact correspondence with video segments:

```text
conversation_id,buyer_url,seller_url,buyer_offset_ms,seller_offset_ms[,composite_source]
```

- The buyer is represented by the left sphere.
- The seller is represented by the right sphere.
- Both offsets are non-negative milliseconds on one shared conversation
  timeline.
- Use `0` for a channel that begins at the start of the conversation.
- A positive offset delays only that channel.
- When `composite_source` is present, its audio is what participants hear and
  its clock defines the segment boundaries. The buyer and seller WAVs are muted
  and used only for the two speaker visualizations. The source accepts the same
  URL, S3 URI, or S3 object-key forms as `video_sources.txt`.

Example where the buyer begins 2.56 seconds after the seller:

```text
session_1,https://example.com/buyer.wav,https://example.com/seller.wav,2560,0
```

Example using a composite recording as the exact timing master:

```text
session_1,https://example.com/buyer.wav,https://example.com/seller.wav,2560,0,s3://kodis-video/session_1_composite.mp4
```

Example where the seller begins 6.38 seconds after the buyer:

```text
session_2,https://example.com/buyer.wav,https://example.com/seller.wav,0,6380
```

Audio playback covers the union of both channels. The earlier channel plays
alone until the later channel begins, and a channel that extends beyond the
other continues playing to its own end. Segmentation uses this shared timeline.

Remote audio hosts must permit browser access to the files, including an
appropriate CORS policy.

## Disabling a source

Blank lines and lines whose first non-whitespace character is `#` are ignored
in both source files:

```text
# conversation_id,https://example.com/buyer.wav,https://example.com/seller.wav,0,500
```

Place `#` at the beginning of the line; inline comments are not supported. The
assignment system chooses among the remaining active sources. If no applicable
sources remain, the start page displays an error.

## Segments and responses

Segments are 60 seconds long with a two-second overlap, so their start times
advance in 58-second steps. Internally they are indexed from `0` through
`n_segments - 1`; participants see Segment 1 through Segment `n_segments`.

Each segment requires participants to:

- Watch or listen to the entire segment in order.
- Interact with every seven-point slider, even when leaving it at the displayed
  default of 4.
- Enter the primary emotion or emotions the highlighted person likely felt.
- Predict whether the dispute will end in impasse or success on a seven-point
  scale (`1 = Impasse`, `7 = Success`).

The Chinese free-response interface asks participants to answer in Chinese.
After the last segment, the final questionnaire refers only to the conversation
that was just annotated.

## Data

Annotations are stored in:

```text
data/annotations.db
```

The administrator CSV export is available at:

```text
http://localhost:8000/admin/exports.csv
```

Set an export token before starting the application:

```bash
ADMIN_EXPORT_TOKEN="replace-with-a-random-secret" .venv/bin/python app.py
```

Send that token as a bearer token when downloading the export:

```bash
curl -H "Authorization: Bearer replace-with-a-random-secret" \
  http://localhost:8000/admin/exports.csv
```

The endpoint remains disabled when `ADMIN_EXPORT_TOKEN` is not set.

The export's `media_mode` column identifies how each annotation run was
presented:

- `video`: video with audio (the default mode)
- `audio`: paired audio visualization
- `video_only`: silent video

Runs created before the `media_mode` field was added have a blank value because
their presentation mode was not stored.

Back up the SQLite database before replacing or removing it. Separate browsers
have separate task sessions, but multiple tabs in the same browser share one
session and should not be used to annotate concurrently.
