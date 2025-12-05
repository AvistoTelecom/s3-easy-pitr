# s3-easy-pitr - Easy Point-In-Time Recovery (PITR) for S3-compatible object stores

## What is s3-easy-pitr?

s3-easy-pitr is a very easy and user-friendly tool for Point-In-Time Recovery (PITR) for S3-compatible object stores.

## Installation

TODO

## Features

- Works with S3-compatible endpoints (custom endpoint + path-style support)
- Performs recovery in-place (copies latest object versions within the bucket)
- Parallel processing
- Progress bar
- Easy to test with built-in test commands

## Notes and recommendations

- Ensure S3 versioning is enabled on the bucket. The tool relies on ListObjectVersions and versionIds to restore historical states.
- If you have a very flaky endpoint, increase `--copy-retries` and/or reduce `--parallel` to lower concurrent pressure.
- Multipart copy parts are copied sequentially by default; you can adjust part size to trade between number of requests and per-request duration. Smaller parts reduce per-request time but increase total number of parts.

## Troubleshooting

- If you see IMDS / EC2 metadata errors when using custom endpoints, ensure you provided explicit credentials (env or flags). This tool disables IMDS use when static credentials are provided.
- If you see occasional CopyObject timeouts, try increasing `--copy-retries` and `--copy-timeout`, or lower `--parallel`.

## Community

If you find a bug or have a question or a feature request, head to the GitHub issues. To contribute to the code, open an issue first and read the contribution documentation.
