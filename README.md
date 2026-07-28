# Flask Conversation Annotator

A Flask application for segment-by-segment emotion annotation of two-person
conversations. A task can present the original video, silent video, or a
two-channel audio visualization.

## Setup

From the repository root:

```bash
python3 -m venv .venv
.venv/bin/pip install -r flask_annotation/requirements.txt
```

Start the development server:

```bash
.venv/bin/python flask_annotation/app.py
```

The application is available at <http://localhost:8000/>.

The default Flask secret is intended only for local development. Set a unique
secret before deploying:

```bash
FLASK_SECRET_KEY="replace-with-a-random-secret" \
  .venv/bin/python flask_annotation/app.py
```

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
[`flask_annotation/data/video_sources.txt`](flask_annotation/data/video_sources.txt).
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
[`flask_annotation/data/audio_sources.txt`](flask_annotation/data/audio_sources.txt).
Every active row must contain five comma-separated values:

```text
conversation_id,buyer_url,seller_url,buyer_offset_ms,seller_offset_ms
```

- The buyer is represented by the left sphere.
- The seller is represented by the right sphere.
- Both offsets are non-negative milliseconds on one shared conversation
  timeline.
- Use `0` for a channel that begins at the start of the conversation.
- A positive offset delays only that channel.

Example where the buyer begins 2.56 seconds after the seller:

```text
session_1,https://example.com/buyer.wav,https://example.com/seller.wav,2560,0
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
flask_annotation/data/annotations.db
```

The administrator CSV export is available at:

```text
http://localhost:8000/admin/exports.csv
```

Back up the SQLite database before replacing or removing it. Separate browsers
have separate task sessions, but multiple tabs in the same browser share one
session and should not be used to annotate concurrently.

