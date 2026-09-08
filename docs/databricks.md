# Execução no Databricks Free Edition

## Arquitetura de implantação

A execução em nuvem recria as camadas a partir dos JSON brutos. Os arquivos Delta gerados localmente não são enviados.

```text
JSON no Unity Catalog Volume
        -> Bronze Delta
        -> Silver Delta
        -> Gold Delta
        -> Camada protegida
        -> Tabelas registradas no Unity Catalog
```

O projeto usa, por padrão:

- catálogo: `workspace`;
- schema: `brasileirao`;
- volume gerenciado: `case_dados`;
- raiz: `/Volumes/workspace/brasileirao/case_dados`.

Os nomes podem ser alterados pelas variáveis do bundle.

## 1. Importar o repositório

No Databricks, abra **Workspace > Create > Git folder**, informe o repositório público e selecione a branch `main`. Como alternativa, use Databricks Declarative Automation Bundles conforme a seção 5.

## 2. Criar schema e volume

Execute `notebooks/00_setup/setup_databricks.py` em compute Serverless. O script cria o schema e o volume gerenciado de forma idempotente.

## 3. Enviar somente os dados Raw

Pela interface, use **New > Add or upload data > Upload files to a volume** e envie os 23 JSON para:

```text
/Volumes/workspace/brasileirao/case_dados/raw/Brasileirao_Dataset/data
```

Alternativamente, com o Databricks CLI autenticado:

```powershell
databricks fs cp "data/raw/Brasileirao_Dataset/data" "dbfs:/Volumes/workspace/brasileirao/case_dados/raw/Brasileirao_Dataset/data" --recursive --overwrite
```

Valide que os arquivos `brasileirao-2003.json` até `brasileirao-2025.json` estão no destino. Bronze, Silver, Gold e Security serão recriadas pelo pipeline.

## 4. Configurar o segredo de mascaramento

Crie um Secret Scope chamado `case-engenharia-dados` e uma chave chamada `masking-salt`. Não use o mesmo valor do exemplo local e não armazene o valor no Git.

Com o Databricks CLI, os comandos administrativos são:

```powershell
databricks secrets create-scope case-engenharia-dados
databricks secrets put-secret case-engenharia-dados masking-salt
```

O segundo comando solicita o valor de forma interativa. O notebook acessa o segredo com `dbutils.secrets.get`; o conteúdo não é impresso.

## 5. Implantar o Workflow

O arquivo `databricks.yml` define um Job serverless com estas dependências:

```text
bronze -> silver -> gold -> security -> register_tables -> health_check
```

Após configurar um perfil do Databricks CLI, execute:

```powershell
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run pipeline_brasileirao -t dev
```

Para usar outros objetos do Unity Catalog:

```powershell
databricks bundle deploy -t dev --var="catalog=workspace,schema=brasileirao,volume=case_dados"
```

O Job usa Serverless e `max_concurrent_runs: 1` para impedir duas atualizações simultâneas.

## 6. Tabelas publicadas

A task `register_tables` registra as tabelas externas Delta no Unity Catalog com prefixos de camada:

- `bronze_partidas`;
- `silver_*`;
- `gold_*`;
- `security_jogadores_protegidos`;
- `security_partidas_protegidas`;
- `pipeline_audit`.

Exemplo de validação:

```sql
SELECT temporada, rank, clube, pontos
FROM workspace.brasileirao.gold_classificacao_por_temporada
ORDER BY temporada DESC, rank;
```

## 7. Controle de acesso

Em uma implantação Azure corporativa, conceda permissões a grupos, não a usuários individuais. Um modelo mínimo é:

```sql
GRANT USE CATALOG ON CATALOG workspace TO `analistas`;
GRANT USE SCHEMA ON SCHEMA workspace.brasileirao TO `analistas`;
GRANT SELECT ON TABLE workspace.brasileirao.gold_classificacao_por_temporada TO `analistas`;
GRANT SELECT ON TABLE workspace.brasileirao.security_jogadores_protegidos TO `analistas`;
```

Não conceda acesso Raw ou Silver identificada aos consumidores do dashboard. A disponibilidade de comandos e concessões depende das permissões do usuário no workspace Free Edition.

## Limitações conhecidas

- O Free Edition é serverless e possui cotas de uso;
- o envio inicial dos JSON é uma etapa anterior ao Job;
- a criação do Secret Scope exige permissão administrativa;
- a implantação Azure com ADLS Gen2 e Key Vault será documentada separadamente;
- caminhos em Volumes são usados para preservar a portabilidade do código baseado em arquivos.
