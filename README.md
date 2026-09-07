# Case Engenharia de Dados

Projeto de case técnico em Engenharia de Dados, processando dados públicos do **Campeonato Brasileiro de Futebol (Série A)** em uma arquitetura **Data Lakehouse** com as camadas **Raw / Bronze / Silver / Gold**, utilizando **PySpark** e **Databricks**.

## Objetivo

Demonstrar competências em:

- Extração e ingestão de dados
- Armazenamento em data lakehouse
- Processamento distribuído com PySpark
- Governança, segurança e mascaramento de dados
- Observabilidade de pipelines
- Escalabilidade de arquitetura
- Análise e visualização de dados

## Tema

Dados históricos do Campeonato Brasileiro de Futebol (Série A), abrangendo edições, rodadas, partidas, clubes, jogadores e estatísticas.

## Arquitetura

```
┌─────────────────┐
│  Fontes Públicas │  GitHub / Kaggle / APIs
│   (JSON/CSV)    │
└────────┬────────┘
         ▼
┌─────────────────┐
│      RAW        │  Dados brutos, sem transformação
│  (landing zone) │
└────────┬────────┘
         ▼
┌─────────────────┐
│     BRONZE      │  Ingestão estruturada em Delta Lake
│ (schema preserved)
└────────┬────────┘
         ▼
┌─────────────────┐
│     SILVER      │  Limpeza, normalização, relacionamentos
│   (curated)     │
└────────┬────────┘
         ▼
┌─────────────────┐
│      GOLD       │  Agregações e métricas de negócio
│   (analytics)   │
└────────┬────────┘
         ▼
┌─────────────────┐
│    Dashboard    │  HTML + Plotly ou Databricks SQL
│   (visualização)│
└─────────────────┘
```

## Tecnologias

- **PySpark** – processamento distribuído
- **Delta Lake** – armazenamento confiável em camadas
- **Databricks Community Edition** – ambiente de desenvolvimento e execução
- **Microsoft Azure Databricks** – replicação para apresentação final
- **Git / GitHub** – versionamento
- **HTML + Plotly** – dashboard local exportável para PDF

## Estrutura do Projeto

```
C:\SANTANDER\DATA_Master
├── data/                  # Dados processados localmente (não versionados)
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── notebooks/             # Notebooks PySpark organizados por camada
│   ├── 01_bronze/
│   ├── 02_silver/
│   ├── 03_gold/
│   └── 04_dashboard/
├── infra/                 # Scripts de provisionamento (Bicep/Terraform)
├── scripts/               # Scripts utilitários (upload, download, setup)
├── src/                   # Código Python reutilizável
├── tests/                 # Testes e validações
├── dashboard/             # Dashboard HTML/Plotly
├── docs/                  # Documentação e diagramas
├── tools/                 # Ferramentas portáteis (Git, Java)
└── README.md
```

## Configuração do Ambiente Local

1. Instale Git, Java JDK 21 e PySpark (instruções no backlog e scripts de setup).
2. Clone este repositório.
3. Configure as variáveis de ambiente:
   - `JAVA_HOME` → `tools/java/jdk-21.0.x`
   - Adicione `tools/git/cmd`, `tools/git/bin` e `%JAVA_HOME%\bin` ao `PATH`.

## Requisitos do Case Atendidos

| Requisito        | Como será atendido                                                |
|------------------|-------------------------------------------------------------------|
| Extração         | Download de dados públicos + ingestão via Spark                   |
| Ingestão         | Batch de arquivos JSON/CSV para Delta Lake                        |
| Armazenamento    | Data Lakehouse com camadas Raw/Bronze/Silver/Gold                 |
| Observabilidade  | Tabela de auditoria `pipeline_audit` e logs estruturados           |
| Segurança        | Secret scopes, ACLs, criptografia em trânsito/repouso            |
| Mascaramento     | Hash SHA-256 e ofuscação de campos sensíveis simulados            |
| Arquitetura      | Lakehouse escalável com Delta Lake e particionamento              |
| Escalabilidade   | Clusters auto-scaling, jobs orquestrados, ADLS Gen2               |

## Reprodutibilidade

Todo o código, instruções e scripts de configuração estarão versionados no GitHub. A execução poderá ser reproduzida no Databricks Community Edition sem custo.

## Fonte dos Dados

- `Brasileirao_Dataset` (GitHub – adaoduque): dados históricos do Brasileirão 2003-2024 em JSON.

## Autor

Case desenvolvido para apresentação em banca avaliadora.
