#requires -Version 7.0
[CmdletBinding()]
param([string]$EvidenceDirectory = (Join-Path $PSScriptRoot '../artifacts/evidence/m1-t03' (Get-Date -Format 'yyyyMMdd-HHmmss-fff')))
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$EvidenceDirectory = [IO.Path]::GetFullPath($EvidenceDirectory)
if (Test-Path -LiteralPath $EvidenceDirectory) { throw 'Choose a new directory; existing evidence is preserved.' }
New-Item -ItemType Directory -Path $EvidenceDirectory | Out-Null
$records = [Collections.Generic.List[object]]::new()
$contexts = [ordered]@{}
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
function Invoke-RetailQuery([string]$Name, [string]$Sql) {
    $shell = 'export PGPASSWORD="$RETAIL_DB_PASSWORD"; exec psql -X -h 127.0.0.1 -U retail -d retail -v ON_ERROR_STOP=1 -At "$@"'
    return Invoke-Step $Name @('compose','exec','-T','postgres','sh','-c',$shell,'--','-c',$Sql)
}
function Invoke-Evaluation([string]$Name, [string]$AsOf, [string]$Cutoff) {
    $output = Invoke-Step $Name @('compose','--profile','tools','run','--rm','reconcile','-m','retail_reconciliation','--snapshot-cutoff',$Cutoff,'--evaluation-as-of',$AsOf,'--rules-version','m1-thin-v1')
    $json = @($output -split '\r?\n' | Where-Object { $_ -match '^\{"' })
    if ($json.Count -ne 1) { throw 'CLI did not produce one complete evaluation JSON object.' }
    return ($json[0] | ConvertFrom-Json)
}
function Get-Canonical([string]$Name, [string]$Key) {
    if ($Key -notmatch '^[a-f0-9]{64}$') { throw 'Unexpected evaluation key.' }
    # Key is a strictly validated hex literal; this query only reads stored rows.
    $sql = "SELECT jsonb_build_object('results',(SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY order_id),'[]'::jsonb) FROM core.reconciliation_results r WHERE evaluation_key='$Key'),'findings',(SELECT coalesce(jsonb_agg(to_jsonb(f) ORDER BY order_id,reason_code),'[]'::jsonb) FROM core.reconciliation_findings f WHERE evaluation_key='$Key'));"
    return Invoke-RetailQuery $Name $sql
}
Push-Location $repo
$failure = $null
try {
    $null = Invoke-Step 'compose-config' @('compose','config','--quiet')
    $null = Invoke-Step 'postgres-start' @('compose','up','-d','--wait','--wait-timeout','120','postgres')
    $null = Invoke-Step 'dbt-debug' @('compose','--profile','tools','run','--rm','dbt','debug')
    $null = Invoke-Step 'unit' @('compose','--profile','tools','run','--rm','reconcile','-m','unittest','discover','-s','tests/reconciliation','-p','test_unit.py','-v')
    $null = Invoke-Step 'postgres-integration' @('compose','--profile','tools','run','--rm','reconcile','-m','unittest','discover','-s','tests/reconciliation','-p','test_postgres.py','-v')
    $null = Invoke-Step 'scenario-ingest' @('compose','--profile','tools','run','--rm','ingest','-m','retail_ingestion','--manifest','fixtures/reconciliation/manifest.json')
    $cutoff = (Invoke-RetailQuery 'snapshot-cutoff' "SELECT COALESCE(max(batch_sequence),0) FROM bronze.input_batches WHERE status='COMMITTED';").Trim()
    if ($cutoff -notmatch '^[0-9]+$') { throw 'No explicit numeric snapshot cutoff was captured.' }
    $before = Invoke-Evaluation 'before-due' '2026-02-01T23:59:59Z' $cutoff
    $at = Invoke-Evaluation 'at-due' '2026-02-02T00:00:00Z' $cutoff
    $after = Invoke-Evaluation 'after-due' '2026-02-03T00:00:00Z' $cutoff
    $contexts.before = $before.evaluation_key
    $contexts.at = $at.evaluation_key
    $contexts.after = $after.evaluation_key
    $contexts.cutoff = $cutoff
    $canonical = Get-Canonical 'canonical-before-replays' $after.evaluation_key
    foreach ($attempt in 1..2) {
        $replay = Invoke-Evaluation "replay-$attempt" '2026-02-03T00:00:00Z' $cutoff
        if ($replay.evaluation_key -ne $after.evaluation_key -or $replay.action -ne 'UNCHANGED') { throw 'Replay key/action mismatch.' }
        if ((Get-Canonical "canonical-after-replay-$attempt" $after.evaluation_key) -cne $canonical) { throw 'Canonical rows/findings changed during replay.' }
    }
    $report = 'export PGPASSWORD="$RETAIL_DB_PASSWORD"; exec psql -X -h 127.0.0.1 -U retail -d retail -v ON_ERROR_STOP=1 "$@" -f /workspace/sql/demo_reconciliation.sql'
    $null = Invoke-Step 'sql-demonstration' @('compose','exec','-T','postgres','sh','-c',$report,'--','-v',("evaluation_key="+$after.evaluation_key),'-v',("comparison_key="+$before.evaluation_key))
} catch {
    $failure = $_.Exception.Message
    Write-Warning $failure
} finally {
    [ordered]@{task='M1-T03';failure=$failure;contexts=$contexts;steps=$records.ToArray();recorded_utc=[DateTimeOffset]::UtcNow.ToString('o')} |
        ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'summary.json') -Encoding utf8NoBOM
    Pop-Location
    Write-Host "Evidence: $EvidenceDirectory"
}
if ($failure) { exit 1 }
exit 0
