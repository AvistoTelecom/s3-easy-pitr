# s3-easy-pitr - Easy Point-In-Time Recovery (PITR) for S3-compatible object stores

## What is s3-easy-pitr?

s3-easy-pitr is a very easy and user-friendly tool for Point-In-Time Recovery (PITR) for S3-compatible object stores.

## Features

- Works with S3-compatible endpoints (custom endpoint + path-style support)
- Safe to use/no data loss (copies wanted object versions on top of the object within the bucket)
- Parallel processing
- Progress bar and nice UX

## Installation

For Unix/Mac users, run:

```sh
curl -fsSL https://github.com/AvistoTelecom/s3-easy-pitr/raw/main/install.sh | bash
```

For Windows/Powershell users, run:
```powershell
iwr https://github.com/AvistoTelecom/s3-easy-pitr/raw/main/install.ps1 -useb | iex
```

If you want to install a specific version (after 0.1.2), use:
```sh
# Bash
curl -fsSL https://github.com/AvistoTelecom/s3-easy-pitr/raw/main/install.sh | bash -s -- -v={version}
# Powershell
$v="{version}";iwr https://github.com/AvistoTelecom/s3-easy-pitr/raw/main/install.ps1 -useb | iex
```

## Usage

```sh
S3_PITR_ENDPOINT=https://s3.example.com S3_PITR_ACCESS_KEY=access-key S3_PITR_SECRET_KEY=secret-key S3_PITR_BUCKET=my-bucket S3_PITR_TARGET_TIME=2025-12-05T18:04:00+01:00 s3-easy-pitr run
```

## S3 compatibility

| Service            | Tested |
|:-------------------|:-------|
| AWS S3             | ❓      |
| MinIO              | ✅      |
| OVH Object Storage | ✅      |


## Notes and recommendations

- Ensure S3 versioning is enabled on the bucket. The tool relies on ListObjectVersions and versionIds to restore historical states.
- If you have a very flaky endpoint, increase `--copy-retries` and/or reduce `--parallel` to lower concurrent pressure.
- Multipart copy parts are copied sequentially by default; you can adjust part size to trade between number of requests and per-request duration. Smaller parts reduce per-request time but increase total number of parts.

## Troubleshooting

- If you see IMDS / EC2 metadata errors when using custom endpoints, ensure you provided explicit credentials (env or flags). This tool disables IMDS use when static credentials are provided.
- If you see occasional CopyObject timeouts, try increasing `--copy-retries` and `--copy-timeout`, or lower `--parallel`.

## Community

If you find a bug or have a question or a feature request, head to the GitHub issues. To contribute to the code, open an issue first and read the contribution documentation.

## Development

This project uses some optional tools:

- [Devbox](https://www.jetify.com/docs/devbox/) creates isolated shells for development. Simply run `devbox shell` after installing `devbox`.
  Devbox installs every binary you need (golang, nodejs) at the same version for everyone working on the project.
- [direnv](https://direnv.net/) loads dynamically content of `.envrc` (in order to start Devbox automatically when you open the project).

If you wish a simpler experience, install the right version of Golang (see `devbox.json`) and that's it.

For convenience, a `minio` can be easily started using docker and the compose file located under `dev` folder. You will find a `.env.dev` with basic credentials to connect to the minio for easy development.

## Acknowledgements

s3-easy-pitr was inspired by [angeloc/s3-pit-restore](https://github.com/angeloc/s3-pit-restore) and [bugfender/s3-version-restore](https://github.com/bugfender/s3-version-restore).

Install scripts were forked from [https://github.com/release-lab/install](https://github.com/release-lab/install).
