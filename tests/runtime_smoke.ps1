#requires -Version 7.0
<#
Native, local-only M1-T01 v1.1 checks. Requires generated .env and initial schema setup.
All inserted records are synthetic. Schema tests roll back; persistence sentinel is
deleted in finally. Never delete volumes or initialize a replacement database here.
#>
[CmdletBinding()]
param(
    [string]$EvidenceDirectory = (Join-Path $PSScriptRoot '../artifacts/evidence' (Get-Date -Format 'yyyyMMdd-HHmmss-fff'))
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
Set-StrictMode -Version Latest
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$EvidenceDirectory = [IO.Path]::GetFullPath($EvidenceDirectory)
New-Item -ItemType Directory -Path $EvidenceDirectory -Force | Out-Null
$checks = [ordered]@{}
$expectations = @(
    'Compose validates; PostgreSQL and Airflow services become healthy.',
    'LocalExecutor, bounded concurrency, healthy scheduler/UI and authenticated API.',
    'Separate retail/airflow databases and roles; cross-database login is denied.',
    'Native dbt debug connects successfully to Retail PostgreSQL.',
    'Exactly eight approved Retail tables exist.',
    'Duplicate batch/source-row keys are rejected with asserted SQLSTATE.',
    'Duplicate global batch_sequence is rejected with asserted SQLSTATE.',
    'Repeated business IDs remain representable as distinct raw occurrences.',
    'Invalid amount/status/time evidence and orphan references are retained.',
    'All seven monetary fields are BIGINT cents.',
    'Result/finding duplicate keys are rejected; distinct findings are allowed.',
    'Multiple run attempts share a stable evaluation_key.',
    'Schema rerun preserves table OIDs and the synthetic persistence sentinel.',
    'Ordinary service restart preserves table OIDs and committed synthetic data.'
)
for ($i = 1; $i -le 14; $i++) {
    $id = 'T01-{0:00}' -f $i
    $checks[$id] = [ordered]@{ test = $id; expected = $expectations[$i - 1]; status = 'NOT VERIFIED'; observed = 'Not executed'; commands = @() }
}
$script:currentTest = 'preflight'
$script:commandIndex = 0
$script:commands = [Collections.Generic.List[object]]::new()
$sentinel = 'synthetic-m1-t01-runtime-' + [guid]::NewGuid().ToString('N')
$sentinelCreated = $false
$failure = $null

function Invoke-Docker {
    param([string[]]$Arguments, [int]$ExpectedExit = 0)
    $script:commandIndex++
    $logName = '{0:000}-{1}.log' -f $script:commandIndex, $script:currentTest
    $started = [DateTimeOffset]::UtcNow.ToString('o')
    $output = @(& docker @Arguments 2>&1 | ForEach-Object { $_.ToString() })
    $code = $LASTEXITCODE
    $body = "started_utc: $started`ncommand_argv: $($Arguments | ConvertTo-Json -Compress)`nexpected_exit: $ExpectedExit`nactual_exit: $code`n`n" + ($output -join "`n")
    [IO.File]::WriteAllText((Join-Path $EvidenceDirectory $logName), $body + "`n", [Text.UTF8Encoding]::new($false))
    $entry = [ordered]@{ test = $script:currentTest; argv = $Arguments; expected_exit = $ExpectedExit; actual_exit = $code; evidence = $logName }
    $script:commands.Add($entry)
    if ($checks.Contains($script:currentTest)) { $checks[$script:currentTest].commands += $entry }
    Write-Host "[$script:currentTest] docker $($Arguments -join ' ') => $code ($logName)"
    if ($code -ne $ExpectedExit) { throw "Unexpected native exit $code (expected $ExpectedExit); inspect $logName" }
    return ($output -join "`n")
}

function Invoke-RetailSql {
    param([string]$Sql)
    $command = 'export PGPASSWORD="$RETAIL_DB_PASSWORD"; exec psql -X -h 127.0.0.1 -U retail -d retail -v ON_ERROR_STOP=1 -At "$@"'
    return Invoke-Docker -Arguments @('compose', 'exec', '-T', 'postgres', 'sh', '-c', $command, '--', '-c', $Sql)
}

function Assert-True {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) { throw $Message }
}

function Complete-Check {
    param([string]$Observed)
    $checks[$script:currentTest].status = 'PASS'
    $checks[$script:currentTest].observed = $Observed
    Write-Host "$script:currentTest PASS: $Observed"
}

$fingerprintSql = @"
SELECT string_agg(n.nspname || '.' || c.relname || ':' || c.oid::text, ',' ORDER BY n.nspname,c.relname)
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname IN ('bronze','core') AND c.relkind='r';
SELECT batch_id || ':' || content_digest || ':' || row_count::text || ':' || status
FROM bronze.input_batches WHERE batch_id='$sentinel';
"@

Push-Location $repo
try {
    if (-not (Test-Path -LiteralPath (Join-Path $repo '.env'))) { throw 'Missing .env; run the documented local environment generator first.' }
    $script:currentTest = 'T01-01'
    $null = Invoke-Docker -Arguments @('compose', 'config', '--quiet')
    $null = Invoke-Docker -Arguments @('compose', 'up', '-d', '--wait', '--wait-timeout', '180', 'postgres', 'airflow-scheduler', 'airflow-webserver')
    $null = Invoke-Docker -Arguments @('compose', 'ps', '-a')
    Complete-Check 'Compose config exit 0; required services reached healthy state.'

    $script:currentTest = 'T01-02'
    $configCheck = @'
import json
from airflow.configuration import conf
actual = {
    "executor": conf.get("core", "executor"),
    "parallelism": conf.getint("core", "parallelism"),
    "max_active_tasks_per_dag": conf.getint("core", "max_active_tasks_per_dag"),
    "max_active_runs_per_dag": conf.getint("core", "max_active_runs_per_dag"),
}
print(json.dumps(actual, sort_keys=True))
assert actual == {"executor": "LocalExecutor", "parallelism": 2, "max_active_tasks_per_dag": 2, "max_active_runs_per_dag": 1}, actual
'@
    $null = Invoke-Docker -Arguments @('compose', 'exec', '-T', 'airflow-scheduler', 'python', '-c', $configCheck)
    $null = Invoke-Docker -Arguments @('compose', 'exec', '-T', 'airflow-scheduler', 'airflow', 'jobs', 'check', '--job-type', 'SchedulerJob')
    $apiCheck = @'
import json, os, requests
base = "http://127.0.0.1:8080"
health = requests.get(base + "/health", timeout=20)
health.raise_for_status()
body = health.json()
assert body["metadatabase"]["status"] == "healthy", body
assert body["scheduler"]["status"] == "healthy", body
response = requests.get(base + "/api/v1/dags?limit=1", auth=(os.environ["AIRFLOW_ADMIN_USER"], os.environ["AIRFLOW_ADMIN_PASSWORD"]), timeout=20)
response.raise_for_status()
assert "dags" in response.json(), response.status_code
print(json.dumps({"health": body, "api_status": response.status_code, "api_total_entries": response.json().get("total_entries")}, sort_keys=True))
'@
    $null = Invoke-Docker -Arguments @('compose', 'exec', '-T', 'airflow-webserver', 'python', '-c', $apiCheck)
    Complete-Check 'Configured LocalExecutor with 2/2/1 concurrency; scheduler healthy; UI health and authenticated API HTTP 200.'

    $script:currentTest = 'T01-03'
    $isolationSql = @'
DO $test$
BEGIN
    IF current_database() <> 'retail' OR current_user <> 'retail' THEN RAISE EXCEPTION 'Wrong Retail connection context'; END IF;
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname='airflow') THEN RAISE EXCEPTION 'Airflow database missing'; END IF;
    IF has_database_privilege('retail','airflow','CONNECT') THEN RAISE EXCEPTION 'Retail can connect to Airflow metadata'; END IF;
    IF has_database_privilege('airflow_meta','retail','CONNECT') THEN RAISE EXCEPTION 'Airflow metadata role can connect to Retail'; END IF;
END $test$;
SELECT current_database(),current_user;
SELECT datname,pg_get_userbyid(datdba) AS owner FROM pg_database WHERE datname IN ('retail','airflow') ORDER BY datname;
'@
    $null = Invoke-RetailSql -Sql $isolationSql
    $airflowLogin = 'export PGPASSWORD="$AIRFLOW_DB_PASSWORD"; exec psql -X -h 127.0.0.1 -U airflow_meta -d airflow -v ON_ERROR_STOP=1 -At -c "SELECT current_database(),current_user;"'
    $airflowContext = Invoke-Docker -Arguments @('compose', 'exec', '-T', 'postgres', 'sh', '-c', $airflowLogin)
    Assert-True ($airflowContext -match 'airflow\|airflow_meta') 'Airflow metadata role/database login mismatch.'
    $crossLogin = 'export PGPASSWORD="$RETAIL_DB_PASSWORD"; exec psql -X -h 127.0.0.1 -U retail -d airflow -v ON_ERROR_STOP=1 -At -c "SELECT 1;"'
    $rejection = Invoke-Docker -Arguments @('compose', 'exec', '-T', 'postgres', 'sh', '-c', $crossLogin) -ExpectedExit 2
    Assert-True ($rejection -match 'permission denied for database') 'Cross-database rejection did not have the expected cause.'
    Complete-Check 'Both native role logins succeed in their own database; cross CONNECT privileges are false; Retail-to-Airflow login rejected (psql exit 2).'

    $script:currentTest = 'T01-04'
    $debug = Invoke-Docker -Arguments @('compose', '--profile', 'tools', 'run', '--rm', 'dbt', 'debug')
    Assert-True ($debug -match 'All checks passed') 'dbt debug did not report all checks passed.'
    Complete-Check 'Native dbt debug reported all checks passed, exit 0.'

    $script:currentTest = 'T01-05'
    $schemaCommand = 'export PGPASSWORD="$RETAIL_DB_PASSWORD"; exec psql -X -h 127.0.0.1 -U retail -d retail -v ON_ERROR_STOP=1 -f /workspace/tests/schema_smoke.sql'
    $schemaOutput = Invoke-Docker -Arguments @('compose', 'exec', '-T', 'postgres', 'sh', '-c', $schemaCommand)
    $schemaEvidence = $script:commands[$script:commands.Count - 1]
    for ($i = 5; $i -le 12; $i++) {
        $script:currentTest = 'T01-{0:00}' -f $i
        if ($i -ne 5) { $checks[$script:currentTest].commands += $schemaEvidence }
        Assert-True ($schemaOutput -match "$script:currentTest PASS") "Missing native assertion marker $script:currentTest PASS."
        Complete-Check "psql exit 0 and explicit $script:currentTest PASS assertion marker; inspect shared SQL evidence for observed values and SQLSTATEs."
    }

    $script:currentTest = 'T01-13'
    $insert = "INSERT INTO bronze.input_batches(batch_id,source_name,content_digest,input_reference,row_count,status,scope_complete) VALUES ('$sentinel','Orders','synthetic-runtime-smoke','synthetic://runtime-smoke',0,'REGISTERED',false);"
    $null = Invoke-RetailSql -Sql $insert
    $sentinelCreated = $true
    $before = Invoke-RetailSql -Sql $fingerprintSql
    Assert-True ($before -match [regex]::Escape($sentinel)) 'Synthetic sentinel missing before persistence checks.'
    $migration = 'export PGPASSWORD="$RETAIL_DB_PASSWORD"; exec psql -X -h 127.0.0.1 -U retail -d retail -v ON_ERROR_STOP=1 -f /workspace/sql/001_foundation.sql'
    $null = Invoke-Docker -Arguments @('compose', 'exec', '-T', 'postgres', 'sh', '-c', $migration)
    $after = Invoke-RetailSql -Sql $fingerprintSql
    Assert-True ($after -eq $before) 'Schema rerun changed table OIDs or synthetic sentinel.'
    Complete-Check 'Second schema application exit 0; all eight table OIDs and committed synthetic sentinel unchanged.'

    $script:currentTest = 'T01-14'
    $null = Invoke-Docker -Arguments @('compose', 'restart', 'postgres', 'airflow-scheduler', 'airflow-webserver')
    $null = Invoke-Docker -Arguments @('compose', 'up', '-d', '--wait', '--wait-timeout', '180', 'postgres', 'airflow-scheduler', 'airflow-webserver')
    $afterRestart = Invoke-RetailSql -Sql $fingerprintSql
    Assert-True ($afterRestart -eq $before) 'Ordinary service restart changed table OIDs or committed synthetic sentinel.'
    Complete-Check 'Ordinary restart returned services to healthy state; identical table OIDs and persisted synthetic sentinel; no volume deletion.'
} catch {
    $failure = $_.Exception.Message
    if ($checks.Contains($script:currentTest)) {
        $checks[$script:currentTest].status = 'FAIL'
        $checks[$script:currentTest].observed = $failure
    }
    Write-Warning "Native smoke stopped: $failure"
} finally {
    if ($sentinelCreated) {
        $script:currentTest = 'cleanup'
        try {
            $null = Invoke-RetailSql -Sql "DELETE FROM bronze.input_batches WHERE batch_id='$sentinel'; SELECT count(*) FROM bronze.input_batches WHERE batch_id='$sentinel';"
        } catch {
            $failure = "$failure Cleanup failed for $sentinel : $($_.Exception.Message)".Trim()
            Write-Warning $failure
        }
    }
    $summary = [ordered]@{
        task = 'M1-T01 v1.1'; environment = 'native local Docker/PostgreSQL/Airflow/dbt';
        recorded_utc = [DateTimeOffset]::UtcNow.ToString('o'); repository = $repo;
        evidence_directory = $EvidenceDirectory; failure = $failure;
        checks = @($checks.Values); commands = $script:commands.ToArray()
    }
    $summary | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'summary.json') -Encoding utf8
    Pop-Location
    Write-Host "Evidence: $EvidenceDirectory"
}
if ($failure) { exit 1 }
exit 0
