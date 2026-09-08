# Segurança, mascaramento e LGPD

## Contexto dos dados

O dataset do Campeonato Brasileiro é público e não deve ser classificado como uma base de dados pessoais sensíveis. Os nomes de jogadores e técnicos são usados neste projeto apenas para demonstrar controles que seriam necessários em um cenário corporativo com dados pessoais.

## Controles demonstrados

O notebook `notebooks/06_seguranca/mascaramento_lgpd.py` produz duas tabelas protegidas:

- `jogadores_protegidos`: substitui o identificador e o clube por tokens SHA-256 e apresenta somente a inicial do jogador;
- `partidas_protegidas`: pseudonimiza clubes e mascara nomes de técnicos.

A pseudonimização combina o valor original com `MASKING_SALT`, mantido fora do código. Isso dificulta ataques por dicionário e mantém tokens determinísticos para relacionamentos analíticos. Hash sem segredo não é anonimização.

## Execução local

Defina um valor exclusivo para a sessão antes de executar o notebook:

```powershell
$env:MASKING_SALT = "valor-local-nao-versionado"
python notebooks/06_seguranca/mascaramento_lgpd.py
```

O valor não deve ser incluído no Git, em notebooks ou em logs. Em produção, deve ser armazenado em um gerenciador de segredos.

## Databricks e Azure

Na implantação em Databricks, o valor deve ser obtido por Databricks Secret Scope apoiado pelo Azure Key Vault. O acesso deve seguir o princípio do menor privilégio:

| Grupo | Raw/Bronze | Silver identificada | Camada protegida | Gold agregada |
|---|---:|---:|---:|---:|
| Engenharia de dados | Escrita | Escrita | Escrita | Escrita |
| Analistas autorizados | Sem acesso | Leitura restrita | Leitura | Leitura |
| Consumidores do dashboard | Sem acesso | Sem acesso | Sem acesso | Leitura |

Unity Catalog e ACLs devem controlar catálogos, schemas, tabelas, volumes e views. Credenciais de serviço devem usar identidades gerenciadas sempre que possível.

## Proteção em trânsito e em repouso

- TLS protege conexões entre clientes, Databricks e armazenamento;
- Azure Storage Service Encryption protege os arquivos em repouso;
- chaves gerenciadas pelo cliente podem ser adotadas quando exigidas pela organização;
- segredos não devem ser armazenados no repositório nem impressos em logs.

## Relação com a LGPD

Em uma aplicação com dados pessoais, seriam necessários finalidade definida, base legal, minimização, retenção limitada, rastreabilidade, atendimento aos direitos do titular e processo de resposta a incidentes. Pseudonimização reduz riscos, mas não elimina a aplicação da LGPD quando a reidentificação ainda for possível.

Para este case, os controles são uma demonstração técnica e não uma declaração de que os dados esportivos públicos são sensíveis ou de que o projeto, isoladamente, garante conformidade jurídica.
