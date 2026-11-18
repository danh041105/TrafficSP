#Requires -Version 7.0
<# Merge the checked-in Trino connection into the persistent CloudBeaver workspace.
   Run from PowerShell on the host. Existing connections and accounts are preserved.
   The server is restarted briefly to reload its datasource configuration.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$datasourcePath = '/opt/cloudbeaver/workspace/GlobalConfiguration/.dbeaver/data-sources.json'
$templatePath = Join-Path $repoRoot 'conf/cloudbeaver/data-sources.json'
$tempPath = Join-Path ([System.IO.Path]::GetTempPath()) ('traffic-cloudbeaver-' + [guid]::NewGuid() + '.json')
$serverStopped = $false

function Invoke-Compose {
    param([string[]]$Arguments)
    $result = & docker compose --project-directory $repoRoot @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Compose failed while configuring CloudBeaver (exit $LASTEXITCODE)."
    }
    return $result
}

try {
    $currentJson = Invoke-Compose -Arguments @('exec', '-T', 'cloudbeaver', 'cat', $datasourcePath)
    $current = ($currentJson -join "`n") | ConvertFrom-Json -AsHashtable
    $template = Get-Content -LiteralPath $templatePath -Raw | ConvertFrom-Json -AsHashtable
    if (-not $current.ContainsKey('connections')) { $current['connections'] = @{} }
    foreach ($id in $template.connections.Keys) {
        $current.connections[$id] = $template.connections[$id]
    }
    $current | ConvertTo-Json -Depth 40 | Set-Content -LiteralPath $tempPath -Encoding utf8NoBOM
    $backupPath = $datasourcePath + '.before-trino-' + (Get-Date -Format 'yyyyMMddHHmmss')
    Invoke-Compose -Arguments @('exec', '-T', 'cloudbeaver', 'cp', $datasourcePath, $backupPath) | Out-Null
    Invoke-Compose -Arguments @('stop', 'cloudbeaver') | Out-Null
    $serverStopped = $true
    Invoke-Compose -Arguments @('cp', $tempPath, ('cloudbeaver:' + $datasourcePath)) | Out-Null
    Invoke-Compose -Arguments @('start', 'cloudbeaver') | Out-Null
    $serverStopped = $false
    Write-Output 'Configured Traffic Lakehouse (Trino). Open http://localhost:8978 and refresh connections.'
}
finally {
    if ($serverStopped) {
        & docker compose --project-directory $repoRoot start cloudbeaver | Out-Null
    }
    if (Test-Path -LiteralPath $tempPath) { Remove-Item -LiteralPath $tempPath }
}
