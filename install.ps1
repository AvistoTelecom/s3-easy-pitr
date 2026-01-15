#!/usr/bin/env pwsh
# Copied from https://github.com/release-lab/install/raw/refs/heads/v1/install.ps1
# Modified: user bin dir + temp extract for multi-file GoReleaser archives


# inherit from https://deno.land/x/install@v0.1.4/install.ps1
# Copyright 2018 the Deno authors. All rights reserved. MIT license.

$ErrorActionPreference = 'Stop'

$inputVersion = if ($version) { "${version}" } else { "${v}" }
$inputExe = if ($exe) { "${exe}" } else { "${e}" }

$githubUrl = if ($github) { "${github}" } elseif ($g) { "${g}" } else { "https://github.com" }

$owner = "AvistoTelecom"
$repoName = "s3-easy-pitr"
$exeName = "${inputExe}"

if ($exeName -eq "") {
  $exeName = "${repoName}"
}

if ($inputVersion) {
  $version = "${inputVersion}"
}

if ([Environment]::Is64BitProcess) {
  $arch = "amd64"
} else {
  $arch = "386"
}

$BinDir = "$Home\bin"
$Target = "windows_${arch}"
$DownloadedTarGz = "$env:TEMP\${exeName}_${Target}.tar.gz"
$TempDir = [System.IO.Path]::GetTempPath()

# GitHub requires TLS 1.2
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$ResourceUri = if (!$version) {
  "${githubUrl}/${owner}/${repoName}/releases/latest/download/${exeName}_${Target}.tar.gz"
} else {
  "${githubUrl}/${owner}/${repoName}/releases/download/${version}/${exeName}_${Target}.tar.gz"
}

if (!(Test-Path $BinDir)) {
  New-Item $BinDir -ItemType Directory | Out-Null
}

Write-Output "[1/3] Download ${ResourceUri}"
Remove-Item $DownloadedTarGz -ErrorAction SilentlyContinue
Invoke-WebRequest $ResourceUri -OutFile $DownloadedTarGz -UseBasicParsing -ErrorAction Stop

Write-Output "[2/3] Install ${exeName} to ${BinDir}"
# Extract to temp dir first, move only binary
$ExtractDir = New-TemporaryFile | % { $_.Delete(); New-Item -ItemType Directory -Path $_.FullName }
if (Get-Command tar -ErrorAction SilentlyContinue) {
  tar -xzf $DownloadedTarGz -C $ExtractDir.FullName
} else {
  # Fallback to 7Zip module
  if (-not (Get-Command Expand-7Zip -ErrorAction SilentlyContinue)) {
    Install-Package -Scope CurrentUser -Force 7Zip4PowerShell > $null
  }
  Expand-7Zip $DownloadedTarGz $ExtractDir.FullName
}
# Move only the .exe binary
$BinaryPath = Join-Path $ExtractDir.FullName "${exeName}.exe"
if (Test-Path $BinaryPath) {
  Move-Item $BinaryPath $BinDir -Force
} else {
  throw "Binary ${exeName}.exe not found in archive"
}
Remove-Item $DownloadedTarGz -Force
Remove-Item $ExtractDir.FullName -Recurse -Force

$downloadedExe = "$BinDir\${exeName}.exe"
# Make executable (not strictly needed on Windows)
# icacls "$downloadedExe" /grant "${env:USERNAME}:RX"  # Optional

Write-Output "[3/3] Set environment variables"
Write-Output "${exeName} was installed successfully to $downloadedExe"

$User = [EnvironmentVariableTarget]::User
$Path = [Environment]::GetEnvironmentVariable('Path', $User)
if (!(";$Path;".ToLower() -like "*;$BinDir;*".ToLower())) {
  [Environment]::SetEnvironmentVariable('Path', "$Path;$BinDir", $User)
  $Env:Path += ";$BinDir"
  Write-Output "PATH updated. Restart PowerShell or run: `$Env:PATH += ';$BinDir'"
}

if (Get-Command $exeName -ErrorAction SilentlyContinue) {
  Write-Output "Run '${exeName} --help' to get started"
} else {
  Write-Output "Restart PowerShell and run '${exeName} --help' to get started"
}
