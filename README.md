# s3-easy-pitr

Simple Point-In-Time Recovery (PITR) tool for S3-compatible object stores.

Features
- Works with S3-compatible endpoints (custom endpoint + path-style support)
- Performs recovery in-place (copies latest object versions within the bucket)
- Parallel processing
- Cobra + Viper for CLI and env var configuration
- Zap for structured logging
- Progress bar + live per-worker current-file display

Basic usage

Configure via flags or environment variables (prefix S3_PITR_):

- endpoint: S3 endpoint URL (required)
- access-key, secret-key: credentials (required)
- bucket: bucket name (required)
- prefix: filter objects by prefix (optional)
- parallel: number of parallel workers (default 8)
- target-time (-t): RFC3339 timestamp for point-in-time recovery (required)

New features and tunables

- In-place PITR using S3 object versioning: the tool lists object versions and restores the version that was active at the given target time. Versioning must be enabled on the bucket.
- Single-shot CopyObject is used for small objects. For large objects the tool will switch to multipart copy using UploadPartCopy.
- Retry & timeout controls and multipart tuning are exposed as CLI flags and env vars (see below).

Flags (short form shown where available)

- --endpoint, -e            S3 endpoint URL (env: S3_PITR_ENDPOINT)
- --access-key, -a         Access key (env: S3_PITR_ACCESS_KEY)
- --secret-key, -s         Secret key (env: S3_PITR_SECRET_KEY)
- --bucket, -b             Bucket name (env: S3_PITR_BUCKET)
- --prefix, -p             Object prefix filter (env: S3_PITR_PREFIX)
- --target-time, -t        Target RFC3339 time for PITR (env: S3_PITR_TARGET_TIME)
- --parallel, -n           Number of parallel workers (env: S3_PITR_PARALLEL)
- --log-level              Log level (env: S3_PITR_LOG_LEVEL)

Copy / multipart tuning flags

- --copy-retries           Number of retries for copy operations (env: S3_PITR_COPY_RETRIES). Default: 5
- --copy-timeout           Per-copy timeout (env: S3_PITR_COPY_TIMEOUT). Example: 30s, 2m. Default: 2m
- --copy-part-size         Multipart copy part size (env: S3_PITR_COPY_PART_SIZE). Example: 100MB, 200MB. Default: 200MB
- --multipart-threshold    Use multipart copy for objects larger than this size (env: S3_PITR_MULTIPART_THRESHOLD). Default: 1GB

Examples

Using environment variables (recommended for credentials):

```bash
export S3_PITR_ENDPOINT=https://s3.example.com
export S3_PITR_ACCESS_KEY=AKIA...
export S3_PITR_SECRET_KEY=...
export S3_PITR_BUCKET=my-bucket
export S3_PITR_TARGET_TIME=2025-11-05T10:00:00Z
./s3-easy-pitr run --parallel 8
```

Override defaults for flaky networks / large objects:

```bash
S3_PITR_COPY_RETRIES=8 S3_PITR_COPY_TIMEOUT=3m S3_PITR_COPY_PART_SIZE=200MB ./s3-easy-pitr run --prefix docker/registry
```

Notes and recommendations

- Ensure S3 versioning is enabled on the bucket. The tool relies on ListObjectVersions and versionIds to restore historical states.
- If you have a very flaky endpoint, increase `--copy-retries` and/or reduce `--parallel` to lower concurrent pressure.
- Multipart copy parts are copied sequentially by default; you can adjust part size to trade between number of requests and per-request duration. Smaller parts reduce per-request time but increase total number of parts.
- For extremely large objects you may consider implementing parallel UploadPartCopy (not enabled by default in this release) or using the provider's native replication/restore features if available.

Troubleshooting

- If you see IMDS / EC2 metadata errors when using custom endpoints, ensure you provided explicit credentials (env or flags). This tool disables IMDS use when static credentials are provided.
- If you see occasional CopyObject timeouts, try increasing `--copy-retries` and `--copy-timeout`, or lower `--parallel`.

Contact / Contributing

Open issues or PRs in the repository. Small fixes, tests and documentation improvements are welcome.
