#!/usr/bin/env python3
"""
Batch add/update duration_sec metadata on S3 video objects.

Requirements:
  pip install boto3
  ffprobe installed and on PATH

Examples:
  python add_duration_metadata.py --bucket kodis-video
  python add_duration_metadata.py --bucket kodis-video --prefix videos/
  python add_duration_metadata.py --bucket kodis-video --force
  python add_duration_metadata.py --bucket kodis-video --dry-run
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Iterable

import boto3
from botocore.exceptions import ClientError

VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".webm", ".mkv", ".avi"}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True, help="S3 bucket name")
    p.add_argument("--prefix", default="", help="Optional prefix to limit objects")
    p.add_argument("--region", default=None, help="AWS region, optional")
    p.add_argument("--force", action="store_true", help="Overwrite existing duration_sec metadata")
    p.add_argument("--dry-run", action="store_true", help="Show what would change without writing")
    p.add_argument(
        "--download",
        action="store_true",
        help="Download each object to a temp file before probing. Safer if ffprobe cannot read presigned URLs in your environment.",
    )
    return p.parse_args()


def require_ffprobe() -> None:
    if shutil.which("ffprobe") is None:
        raise SystemExit("ffprobe not found on PATH. Install ffmpeg/ffprobe first.")


def iter_objects(s3, bucket: str, prefix: str) -> Iterable[dict]:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            if Path(key).suffix.lower() not in VIDEO_EXTS:
                continue
            yield obj


def probe_duration_seconds_from_url(url: str) -> float:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_entries",
        "format=duration",
        url,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    payload = json.loads(result.stdout)
    duration = payload.get("format", {}).get("duration")
    if duration is None:
        raise RuntimeError("ffprobe returned no duration")
    return float(duration)


def probe_duration_seconds_from_file(path: str) -> float:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_entries",
        "format=duration",
        path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    payload = json.loads(result.stdout)
    duration = payload.get("format", {}).get("duration")
    if duration is None:
        raise RuntimeError("ffprobe returned no duration")
    return float(duration)


def format_duration_sec(value: float) -> str:
    return f"{value:.3f}".rstrip("0").rstrip(".")


def main() -> int:
    args = parse_args()
    require_ffprobe()

    session = boto3.session.Session(region_name=args.region)
    s3 = session.client("s3")

    updated = 0
    skipped = 0
    failed = 0

    for obj in iter_objects(s3, args.bucket, args.prefix):
        key = obj["Key"]
        size = obj["Size"]

        try:
            head = s3.head_object(Bucket=args.bucket, Key=key)
            metadata = dict(head.get("Metadata", {}))

            if "duration_sec" in metadata and not args.force:
                print(f"SKIP  {key} (already has duration_sec={metadata['duration_sec']})")
                skipped += 1
                continue

            if size > 5 * 1024**3:
                print(f"SKIP  {key} (>5 GB; this script uses single-call CopyObject for metadata update)")
                skipped += 1
                continue

            if args.download:
                with tempfile.TemporaryDirectory() as tmpdir:
                    local_path = str(Path(tmpdir) / Path(key).name)
                    s3.download_file(args.bucket, key, local_path)
                    duration_sec = probe_duration_seconds_from_file(local_path)
            else:
                presigned = s3.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": args.bucket, "Key": key},
                    ExpiresIn=3600,
                    HttpMethod="GET",
                )
                duration_sec = probe_duration_seconds_from_url(presigned)

            new_metadata = metadata.copy()
            new_metadata["duration_sec"] = format_duration_sec(duration_sec)

            if args.dry_run:
                print(f"DRY   {key} -> duration_sec={new_metadata['duration_sec']}")
                continue

            copy_source = {"Bucket": args.bucket, "Key": key}

            # Preserve common object properties when replacing metadata.
            extra = {}
            if head.get("ContentType"):
                extra["ContentType"] = head["ContentType"]
            if head.get("CacheControl"):
                extra["CacheControl"] = head["CacheControl"]
            if head.get("ContentDisposition"):
                extra["ContentDisposition"] = head["ContentDisposition"]
            if head.get("ContentEncoding"):
                extra["ContentEncoding"] = head["ContentEncoding"]
            if head.get("ContentLanguage"):
                extra["ContentLanguage"] = head["ContentLanguage"]
            if head.get("WebsiteRedirectLocation"):
                extra["WebsiteRedirectLocation"] = head["WebsiteRedirectLocation"]

            s3.copy_object(
                Bucket=args.bucket,
                Key=key,
                CopySource=copy_source,
                Metadata=new_metadata,
                MetadataDirective="REPLACE",
                TaggingDirective="COPY",
                **extra,
            )

            print(f"OK    {key} -> duration_sec={new_metadata['duration_sec']}")
            updated += 1

        except subprocess.CalledProcessError as e:
            print(f"FAIL  {key} ffprobe error: {e.stderr.strip()}", file=sys.stderr)
            failed += 1
        except ClientError as e:
            print(f"FAIL  {key} AWS error: {e}", file=sys.stderr)
            failed += 1
        except Exception as e:
            print(f"FAIL  {key} {e}", file=sys.stderr)
            failed += 1

    print("\nSummary")
    print(f"  Updated: {updated}")
    print(f"  Skipped: {skipped}")
    print(f"  Failed:  {failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())