# Generate disposable local-only credentials. Never print them or overwrite .env.
$ErrorActionPreference = 'Stop'
$repoPath = Split-Path $PSScriptRoot -Parent
$envPath = Join-Path $repoPath '.env'
if (Test-Path -LiteralPath $envPath) { throw '.env already exists; preserve existing volume credentials.' }
function New-HexSecret {
    $bytes = [byte[]]::new(32)
    [System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    return [Convert]::ToHexString($bytes).ToLowerInvariant()
}
$fernetBytes = [byte[]]::new(32)
[System.Security.Cryptography.RandomNumberGenerator]::Fill($fernetBytes)
$fernetKey = [Convert]::ToBase64String($fernetBytes).Replace('+', '-').Replace('/', '_')
$lines = @(
    '# Generated local-only credentials. Do not commit.'
    "POSTGRES_PASSWORD=$(New-HexSecret)"
    "RETAIL_DB_PASSWORD=$(New-HexSecret)"
    "AIRFLOW_DB_PASSWORD=$(New-HexSecret)"
    "AIRFLOW_ADMIN_PASSWORD=$(New-HexSecret)"
    "AIRFLOW_FERNET_KEY=$fernetKey"
    "AIRFLOW_WEBSERVER_SECRET_KEY=$(New-HexSecret)"
)
[System.IO.File]::WriteAllLines($envPath, $lines, [System.Text.UTF8Encoding]::new($false))
Write-Output 'Created ignored .env with local-only random credentials.'
