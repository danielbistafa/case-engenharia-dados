# Script para configurar variaveis de ambiente na sessao atual do PowerShell
$projectRoot = "C:\SANTANDER\DATA_Master"
$javaHome = Join-Path $projectRoot "tools\java\jdk-21.0.12.1+1"
$gitCmd = Join-Path $projectRoot "tools\git\cmd"
$gitBin = Join-Path $projectRoot "tools\git\bin"

$env:JAVA_HOME = $javaHome
$env:PATH = "$javaHome\bin;$gitCmd;$gitBin;$env:PATH"

Write-Host "Ambiente configurado para esta sessao:" -ForegroundColor Green
Write-Host "JAVA_HOME = $env:JAVA_HOME"
Write-Host "Git = $(git --version)"
Write-Host "Java = $(java -version 2>&1 | Select-Object -First 1)"
