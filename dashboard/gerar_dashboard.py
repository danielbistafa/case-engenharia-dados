"""
Gera um dashboard HTML local a partir das tabelas da camada Gold.

O dashboard e auto-contido (dados e bibliotecas Plotly embutidos) e pode ser
aberto diretamente no navegador e exportado para PDF.
"""

import sys
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.offline as pyo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from config import GOLD_DIR  # noqa: E402
from spark_session import create_spark_session  # noqa: E402


def load_gold_table(spark, table_name: str):
    """Carrega uma tabela Delta da camada Gold e converte para pandas."""
    path = GOLD_DIR / table_name
    print(f"Lendo {table_name}...")
    return spark.read.format("delta").load(str(path)).toPandas()


def create_html_dashboard(output_path: Path = Path(__file__).parent / "index.html") -> None:
    """Cria o arquivo HTML do dashboard."""
    spark = create_spark_session(app_name="DashboardBrasileirao")

    try:
        # Carrega tabelas Gold
        df_class = load_gold_table(spark, "classificacao_por_temporada")
        df_gols = load_gold_table(spark, "evolucao_gols_por_temporada")
        df_art = load_gold_table(spark, "artilharia")
        df_min = load_gold_table(spark, "gols_por_minuto")
        df_aprov = load_gold_table(spark, "aproveitamento_mandante_visitante")

        # 1. Evolucao de gols por temporada
        fig_gols = px.line(
            df_gols,
            x="temporada",
            y="media_gols_por_partida",
            markers=True,
            title="Evolucao da Media de Gols por Partida",
            labels={"temporada": "Temporada", "media_gols_por_partida": "Media de Gols"},
        )
        fig_gols.update_layout(template="plotly_white", height=450)

        # 2. Top 10 artilheiros
        df_top_art = df_art.sort_values("total_gols", ascending=False).head(10)
        fig_art = px.bar(
            df_top_art,
            x="total_gols",
            y="nome_jogador",
            orientation="h",
            title="Top 10 Artilheiros do Brasileirao",
            labels={"total_gols": "Total de Gols", "nome_jogador": "Jogador"},
            color="total_gols",
            color_continuous_scale="Greens",
        )
        fig_art.update_layout(template="plotly_white", height=450, yaxis={"autorange": "reversed"})

        # 3. Campeoes por temporada (rank 1)
        df_campeoes = df_class[df_class["rank"] == 1].sort_values("temporada", ascending=False)
        contagem_titulos = df_campeoes["clube"].value_counts().reset_index()
        contagem_titulos.columns = ["clube", "titulos"]
        fig_titulos = px.bar(
            contagem_titulos,
            x="titulos",
            y="clube",
            orientation="h",
            title="Maiores Campeoes do Brasileirao",
            labels={"titulos": "Titulos", "clube": "Clube"},
            color="titulos",
            color_continuous_scale="Blues",
        )
        fig_titulos.update_layout(template="plotly_white", height=450, yaxis={"autorange": "reversed"})

        # 4. Distribuicao de gols por minuto
        fig_min = px.pie(
            df_min,
            names="intervalo",
            values="quantidade_gols",
            title="Distribuicao de Gols por Intervalo de Minuto",
            hole=0.4,
        )
        fig_min.update_layout(template="plotly_white", height=450)

        # 5. Aproveitamento mandante vs visitante (media geral)
        df_aprov_media = df_aprov.groupby("tipo")["aproveitamento"].mean().reset_index()
        fig_aprov = px.bar(
            df_aprov_media,
            x="tipo",
            y="aproveitamento",
            title="Aproveitamento Medio: Mandante vs Visitante",
            labels={"tipo": "Tipo", "aproveitamento": "Aproveitamento (%)"},
            color="tipo",
            color_discrete_sequence=["#2E8B57", "#CD5C5C"],
        )
        fig_aprov.update_layout(template="plotly_white", height=450)

        # 6. Tabela de classificacao da ultima temporada completa
        ultima_temporada = df_class["temporada"].max()
        df_ultima = df_class[df_class["temporada"] == ultima_temporada].sort_values("rank").head(10)
        fig_table = go.Figure(
            data=[
                go.Table(
                    header=dict(values=["Rank", "Clube", "Pts", "J", "V", "E", "D", "SG", "Aprov."],
                                fill_color="#2c3e50", font=dict(color="white"), align="center"),
                    cells=dict(
                        values=[
                            df_ultima["rank"],
                            df_ultima["clube"],
                            df_ultima["pontos"],
                            df_ultima["jogos"],
                            df_ultima["vitorias"],
                            df_ultima["empates"],
                            df_ultima["derrotas"],
                            df_ultima["saldo_gols"],
                            df_ultima["aproveitamento"],
                        ],
                        align="center",
                    ),
                )
            ]
        )
        fig_table.update_layout(
            title=f"Top 10 Classificacao - Temporada {ultima_temporada}",
            template="plotly_white",
            height=450,
        )

        # Combina tudo em um HTML
        html_parts = [
            "<html><head><meta charset='utf-8'><title>Dashboard Brasileirao</title>",
            "<style>",
            "body { font-family: Arial, sans-serif; margin: 40px; background-color: #f5f6fa; color: #2c3e50; }",
            "h1 { color: #27ae60; }",
            "h2 { color: #34495e; margin-top: 40px; }",
            ".descricao { background: white; padding: 20px; border-radius: 8px; margin-bottom: 30px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }",
            ".grafico { background: white; padding: 15px; border-radius: 8px; margin-bottom: 30px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }",
            "</style></head><body>",
            "<h1>Dashboard - Campeonato Brasileiro de Futebol</h1>",
            "<div class='descricao'>",
            "<p>Este dashboard apresenta uma visao sintetica dos dados historicos do Brasileirao (2003-2025).</p>",
            "<p>Os dados foram processados em uma arquitetura Data Lakehouse com as camadas Raw, Bronze, Silver e Gold usando PySpark e Delta Lake.</p>",
            "</div>",
        ]

        # Adiciona cada grafico em uma secao
        for titulo, fig in [
            ("Evolucao da Media de Gols", fig_gols),
            ("Maiores Artilheiros", fig_art),
            ("Maiores Campeoes", fig_titulos),
            ("Gols por Intervalo de Minuto", fig_min),
            ("Aproveitamento Mandante vs Visitante", fig_aprov),
            (f"Classificacao - Temporada {ultima_temporada}", fig_table),
        ]:
            html_parts.append(f"<h2>{titulo}</h2>")
            html_parts.append(f"<div class='grafico'>{fig.to_html(full_html=False, include_plotlyjs='cdn')}</div>")

        html_parts.append("</body></html>")

        # Salva HTML
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(html_parts))

        print(f"\nDashboard gerado com sucesso em: {output_path}")
        print(f"Abra o arquivo no navegador e use Ctrl+P para exportar como PDF.")

    finally:
        spark.stop()


if __name__ == "__main__":
    create_html_dashboard()
