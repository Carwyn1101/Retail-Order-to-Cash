#requires -Version 7.0
<#
Real manual scheduler/LocalExecutor execution. Run the unchanged T01/T02/T03
regression harnesses sequentially first; they restart services/use owned fixtures.
Preserves existing evidence and restores the DAG's original paused state.
#>
[CmdletBinding()]
param([string]$EvidenceDirectory = (Join-Path $PSScriptRoot '../artifacts/evidence/m1-t04' (Get-Date -Format 'yyyyMMdd-HHmmss-fff')))
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$EvidenceDirectory = [IO.Path]::GetFullPath($EvidenceDirectory)
if (Test-Path -LiteralPath $EvidenceDirectory) { throw 'Choose a new directory; existing evidence is preserved.' }
New-Item -ItemType Directory -Path $EvidenceDirectory | Out-Null
$records = [Collections.Generic.List[object]]::new()
$native = $null
$failure = $null
function Invoke-Step([string]$Name, [string[]]$Arguments) {
    $started = [DateTimeOffset]::UtcNow.ToString('o')
    $output = @(& docker @Arguments 2>&1 | ForEach-Object { $_.ToString() })
    $code = $LASTEXITCODE
    $record = [ordered]@{step=$Name;command_argv=$Arguments;started_utc=$started;expected_exit=0;actual_exit=$code;evidence=($Name+'.log')}
    $records.Add($record)
    $body = ($record | ConvertTo-Json -Depth 5) + [Environment]::NewLine + ($output -join [Environment]::NewLine)
    [IO.File]::WriteAllText((Join-Path $EvidenceDirectory ($Name+'.log')), $body, [Text.UTF8Encoding]::new($false))
    Write-Host "$Name => exit $code"
    if ($code -ne 0) { throw "Step $Name failed; inspect original evidence." }
    return ($output -join [Environment]::NewLine)
}
Push-Location $repo
try {
    $null = Invoke-Step 'compose-config' @('compose','config','--quiet')
    $null = Invoke-Step 'runtime-start' @('compose','up','-d','--wait','--wait-timeout','180','postgres','airflow-scheduler','airflow-webserver')
    $null = Invoke-Step 'scheduler-job' @('compose','exec','-T','airflow-scheduler','airflow','jobs','check','--job-type','SchedulerJob')
    $output = Invoke-Step 'native-airflow' @('compose','exec','-T','airflow-webserver','python','/workspace/tests/airflow/runtime.py')
    $summaries = @($output -split '\r?\n' | Where-Object { $_.StartsWith('M1_T04_SUMMARY ') })
    if ($summaries.Count -ne 1) { throw 'Native client did not produce exactly one completion summary.' }
    $native = $summaries[0].Substring('M1_T04_SUMMARY '.Length) | ConvertFrom-Json
    if ($native.status -ne 'PASS' -or $native.evaluation_key -notmatch '^[a-f0-9]{64}$') { throw 'Native acceptance summary is invalid.' }
    $report = 'export PGPASSWORD="$RETAIL_DB_PASSWORD"; exec psql -X -h 127.0.0.1 -U retail -d retail -v ON_ERROR_STOP=1 "$@" -f /workspace/sql/demo_reconciliation.sql'
    $null = Invoke-Step 'sql-demonstration' @('compose','exec','-T','postgres','sh','-c',$report,'--','-v',('evaluation_key='+$native.evaluation_key))
} catch {
    $failure = $_.Exception.Message
    Write-Warning $failure
} finally {
    [ordered]@{task='M1-T04';failure=$failure;native=$native;steps=$records.ToArray();recorded_utc=[DateTimeOffset]::UtcNow.ToString('o')} |
        ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'summary.json') -Encoding utf8NoBOM
    Pop-Location
    Write-Host "Evidence: $EvidenceDirectory"
}
if ($failure) { exit 1 }
exit 0
