# Configuração do Ambiente Local

Este guia explica como preparar o ambiente Windows para desenvolver e executar o projeto localmente.

## 1. Pré-requisitos

- Windows 10 ou 11
- Conexão com a internet
- Acesso administrador local (opcional, usamos versões portáteis)

## 2. Componentes Instalados

Os seguintes componentes foram colocados na pasta `tools/`:

- **Git Portable** – versionamento
- **Eclipse Temurin JDK 21** – runtime Java exigido pelo PySpark
- **PySpark 4.x** – via `pip`
- **Delta Lake**, **Pandas**, **Requests** – bibliotecas Python auxiliares

## 3. Configuração das Variáveis de Ambiente

Após a instalação, configure permanentemente no Windows:

```powershell
$javaHome = "C:\SANTANDER\DATA_Master\tools\java\jdk-21.0.12.1+1"
[Environment]::SetEnvironmentVariable("JAVA_HOME", $javaHome, "User")

$path = [Environment]::GetEnvironmentVariable("PATH", "User")
$additions = @(
    "$javaHome\bin",
    "C:\SANTANDER\DATA_Master\tools\git\cmd",
    "C:\SANTANDER\DATA_Master\tools\git\bin"
)
foreach ($add in $additions) {
    if ($path -notlike "*$add*") { $path += ";$add" }
}
[Environment]::SetEnvironmentVariable("PATH", $path, "User")
```

Feche e reabra o terminal para carregar as novas variáveis.

## 4. Ativação Rápida (sessão atual)

Execute no PowerShell:

```powershell
.\scripts\setup_env.ps1
```

Isso configura `JAVA_HOME` e `PATH` apenas para a sessão atual.

## 5. Verificação

```powershell
git --version
java -version
python -c "import pyspark; print(pyspark.__version__)"
```

## 6. Instalação Manual das Bibliotecas Python

Se precisar reinstalar:

```powershell
pip install pyspark delta-spark pandas requests
```
