# Análise Exploratória dos Dados do Brasileirão

## Fonte

- Repositório: `adaoduque/Brasileirao_Dataset`
- URL: https://github.com/adaoduque/Brasileirao_Dataset
- Dados públicos, utilizados apenas para fins de estudo e case técnico.

## Estrutura dos Arquivos

O dataset é composto por:

1. **Arquivos JSON** (`data/brasileirao-AAAA.json`): um arquivo por ano, de 2003 a 2025.
2. **Arquivos CSV** na raiz do repositório:
   - `campeonato-brasileiro-full.csv` – partidas (ID, rodada, data, hora, mandante, visitante, placar, etc.)
   - `campeonato-brasileiro-estatisticas-full.csv` – estatísticas por partida/clube
   - `campeonato-brasileiro-gols.csv` – registros de gols (atleta, minuto)
   - `campeonato-brasileiro-cartoes.csv` – registros de cartões (atleta, minuto, cor)

## Análise dos JSON

Cada arquivo JSON segue a estrutura:

```json
{
  "1": [  // número da rodada
    {
      "clubs": { "home": "Internacional", "away": "Bahia" },
      "goals": { "home": "2", "away": "1" },
      "cards": {
        "home": { "yellow": [...], "red": [...] },
        "away": { "yellow": [...], "red": [...] }
      },
      "hour": "18:30",
      "date": "13/04/24",
      "coach": { "home": "E. Coudet", "away": "R. Ceni" },
      "formation": { "home": "4-4-2", "away": "4-4-2" },
      "goalsPlayer": {
        "home": [{ "player": "Wesley Ribeiro Silva", "gols": ["72'"] }],
        "away": [...]
      },
      "stadium": "Estádio Beira-Rio",
      "stats": [
        { "home": "8", "stat": "Chutes", "away": "21" },
        ...
      ]
    }
  ],
  "2": [...]
}
```

## Volume

| Ano | Rodadas | Partidas | Partidas com stats |
|-----|---------|----------|--------------------|
| 2003 | 46 | 552 | 552 (vazias) |
| 2004 | 46 | 552 | 552 (vazias) |
| 2005 | 42 | 462 | 462 (vazias) |
| 2006 | 38 | 380 | 380 (vazias) |
| ... | ... | ... | ... |
| 2022 | 38 | 380 | 380 (preenchidas) |
| 2023 | 38 | 380 | 380 (preenchidas) |
| 2024 | 38 | 380 | 0 |
| 2025 | 38 | 380 | 380 (preenchidas) |

Total aproximado: **mais de 8.600 partidas**.

## Qualidade e Desafios

1. **Evolução do schema**: nos anos iniciais o campo `stats` existe, mas está vazio. Em alguns anos (ex: 2014) há apenas algumas partidas preenchidas.
2. **Formato de data**: `dd/MM/aa` (dois dígitos para o ano), exige padronização.
3. **Campos numéricos como texto**: gols, chutes, passes estão em string – precisam de casting.
4. **Dados aninhados**: `cards`, `goalsPlayer` e `stats` são listas/dicionários aninhados, exigindo `explode` no Spark.
5. **Formatos mistos**: JSON + CSV permitem demonstrar ingestão de múltiplas fontes.

## Potencial Analítico (camada Gold)

- **Classificação histórica**: pontos por time ao longo dos anos.
- **Aproveitamento mandante vs visitante**.
- **Artilharia**: gols por atleta, ano e clube.
- **Evolução das estatísticas**: chutes, posse de bola, passes precisos.
- **Cartões**: times mais/ menos disciplinados.
- **Técnicos e formações**: análise de estilo de jogo.

## Conclusão

A base é **excelente** para o case. Tem volume razoável, múltiplos formatos, dados aninhados, inconsistências reais que justificam o processamento em camadas e possibilitam análises ricas na camada Gold.
