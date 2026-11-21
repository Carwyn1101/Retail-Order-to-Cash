#requires -Version 7.0
[CmdletBinding()]
param([string]$EvidenceDirectory = (Join-Path $PSScriptRoot '../artifacts/evidence/m1-t02' (Get-Date -Format 'yyyyMMdd-HHmmss-fff')))
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$EvidenceDirectory = [IO.Path]::GetFullPath($EvidenceDirectory)
if (Test-Path -LiteralPath $EvidenceDirectory) { throw 'Choose a new evidence directory; existing evidence is preserved.' }
New-Item -ItemType Directory -Path $EvidenceDirectory | Out-Null
$records = [Collections.Generic.List[object]]::new()
function Invoke-Step([string]$Name, [string[]]$Arguments) {
    $started = [DateTimeOffset]::UtcNow.ToString('o')
    $output = @(& docker @Arguments 2>&1 | ForEach-Object { $_.ToString() })
    $code = $LASTEXITCODE
    $record = [ordered]@{ step=$Name; kind='native Docker'; command_argv=$Arguments; started_utc=$started; expected_exit=0; actual_exit=$code; evidence=($Name+'.log') }
    $records.Add($record)
    $body = ($record | ConvertTo-Json -Depth 5) + [Environment]::NewLine + ($output -join [Environment]::NewLine)
    [IO.File]::WriteAllText((Join-Path $EvidenceDirectory ($Name+'.log')), $body, [Text.UTF8Encoding]::new($false))
    Write-Host "$Name => exit $code"
    if ($code -ne 0) { throw "Step $Name failed; inspect its original log." }
    return ($output -join [Environment]::NewLine)
}
Push-Location $repo
$failure = $null
try {
    $null = Invoke-Step 'compose-config' @('compose','config','--quiet')
    $null = Invoke-Step 'postgres-start' @('compose','up','-d','--wait','--wait-timeout','120','postgres')
    $null = Invoke-Step 'unit' @('compose','--profile','tools','run','--rm','ingest','-m','unittest','discover','-s','tests/ingestion','-p','test_unit.py','-v')
    $null = Invoke-Step 'postgres-integration' @('compose','--profile','tools','run','--rm','ingest','-m','unittest','discover','-s','tests/ingestion','-p','test_postgres.py','-v')
    foreach ($attempt in 1..3) {
        $null = Invoke-Step "cli-$attempt" @('compose','--profile','tools','run','--rm','ingest')
    }
    # Re-run the unchanged foundation contract after ingestion; fixtures are isolated.
    $sql = 'export PGPASSWORD="$RETAIL_DB_PASSWORD"; exec psql -X -h 127.0.0.1 -U retail -d retail -v ON_ERROR_STOP=1 -f /workspace/tests/schema_smoke.sql'
    $null = Invoke-Step 'foundation-schema' @('compose','exec','-T','postgres','sh','-c',$sql)
} catch {
    $failure = $_.Exception.Message
    Write-Warning $failure
} finally {
    [ordered]@{task='M1-T02';failure=$failure;steps=$records.ToArray();evidence_directory=$EvidenceDirectory;recorded_utc=[DateTimeOffset]::UtcNow.ToString('o')} |
        ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'summary.json') -Encoding utf8NoBOM
    Pop-Location
    Write-Host "Evidence: $EvidenceDirectory"
}
if ($failure) { exit 1 }
exit 0
