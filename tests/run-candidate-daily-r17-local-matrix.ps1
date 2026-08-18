param(
  [string]$Container = '',
  [int]$Port = 55438
)

$ErrorActionPreference = 'Stop'
$runner = Join-Path $PSScriptRoot 'run-candidate-daily-r16-local-matrix.ps1'
if ($Container) {
  & $runner -Container $Container -Port $Port
} else {
  & $runner -Port $Port
}
if ($LASTEXITCODE -ne 0) { throw 'Candidate Daily R17 local matrix failed' }
