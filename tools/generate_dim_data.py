#!/usr/bin/env python3
"""
Gera os seeds de calendário para a dimensão dim_data.

Saídas:
  - dbt/bcb_warehouse/seeds/dim_data_seed.csv  (~3.652 linhas, 2020-2030)
  - dbt/bcb_warehouse/seeds/feriados_nacionais.csv

Nota: as colunas `data_sk` e `eh_feriado_nacional` e `eh_dia_util`
NÃO são geradas aqui — são calculadas no modelo dbt `dim_data.sql`.

Uso:
  python tools/generate_dim_data.py
  python tools/generate_dim_data.py --output-dir /caminho/para/seeds
"""

import argparse
import csv
import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

# ── Logging estruturado JSON ──────────────────────────────────────────────────


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
                "level": record.levelname,
                "message": record.getMessage(),
            }
        )


handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
log = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────────────

START_DATE = date(2020, 1, 1)
END_DATE = date(2030, 12, 31)

MES_NOMES: dict[int, str] = {
    1: "Janeiro",
    2: "Fevereiro",
    3: "Março",
    4: "Abril",
    5: "Maio",
    6: "Junho",
    7: "Julho",
    8: "Agosto",
    9: "Setembro",
    10: "Outubro",
    11: "Novembro",
    12: "Dezembro",
}

DIA_SEMANA_NOMES: dict[int, str] = {
    1: "Segunda-feira",
    2: "Terça-feira",
    3: "Quarta-feira",
    4: "Quinta-feira",
    5: "Sexta-feira",
    6: "Sábado",
    7: "Domingo",
}

# ── Feriados ──────────────────────────────────────────────────────────────────


def calcular_pascoa(ano: int) -> date:
    """Calcula a data da Páscoa pelo algoritmo anônimo gregoriano."""
    a = ano % 19
    b = ano // 100
    c = ano % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l_val = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l_val) // 451
    mes = (h + l_val - 7 * m + 114) // 31
    dia = ((h + l_val - 7 * m + 114) % 31) + 1
    return date(ano, mes, dia)


def gerar_feriados_nacionais(anos: range) -> list[dict[str, str]]:
    """
    Retorna registros de feriados nacionais brasileiros para os anos informados.

    Inclui feriados fixos (lei federal) e móveis (relativos à Páscoa).
    Feriados estaduais e municipais estão fora do escopo (ADR-0009).
    """
    feriados: list[tuple[date, str]] = []

    for ano in anos:
        pascoa = calcular_pascoa(ano)

        # Feriados fixos (Lei nº 662/1949 e alterações)
        feriados += [
            (date(ano, 1, 1), "Confraternização Universal"),
            (date(ano, 4, 21), "Tiradentes"),
            (date(ano, 5, 1), "Dia do Trabalho"),
            (date(ano, 9, 7), "Independência do Brasil"),
            (date(ano, 10, 12), "Nossa Senhora Aparecida"),
            (date(ano, 11, 2), "Finados"),
            (date(ano, 11, 15), "Proclamação da República"),
            (date(ano, 12, 25), "Natal"),
        ]

        # Feriados móveis (relativos à Páscoa)
        feriados += [
            (pascoa - timedelta(days=48), "Carnaval (Segunda-feira)"),
            (pascoa - timedelta(days=47), "Carnaval (Terça-feira)"),
            (pascoa - timedelta(days=2), "Sexta-feira Santa"),
            (pascoa, "Páscoa"),
            (pascoa + timedelta(days=60), "Corpus Christi"),
        ]

    return [
        {"data": d.isoformat(), "nome": nome}
        for d, nome in sorted(feriados, key=lambda x: x[0])
    ]


# ── dim_data ──────────────────────────────────────────────────────────────────


def gerar_dim_data(start: date, end: date) -> list[dict[str, object]]:
    """
    Gera registros do calendário entre start e end (inclusive).

    Colunas omitidas (calculadas no modelo dbt dim_data.sql):
      - data_sk           → macro generate_surrogate_key
      - eh_feriado_nacional → join com seed feriados_nacionais
      - eh_dia_util       → dia_da_semana <= 5 AND NOT eh_feriado_nacional
    """
    registros: list[dict[str, object]] = []
    atual = start
    while atual <= end:
        dia_semana = atual.isoweekday()  # 1=Segunda … 7=Domingo
        registros.append(
            {
                "data": atual.isoformat(),
                "ano": atual.year,
                "mes": atual.month,
                "mes_nome": MES_NOMES[atual.month],
                "trimestre": (atual.month - 1) // 3 + 1,
                "semestre": 1 if atual.month <= 6 else 2,
                "dia_da_semana": dia_semana,
                "dia_da_semana_nome": DIA_SEMANA_NOMES[dia_semana],
            }
        )
        atual += timedelta(days=1)
    return registros


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).parent.parent / "dbt" / "bcb_warehouse" / "seeds",
        help="Diretório de saída (default: dbt/bcb_warehouse/seeds/)",
    )
    args = parser.parse_args()
    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    anos = range(START_DATE.year, END_DATE.year + 1)

    # feriados_nacionais.csv
    log.info("Gerando feriados_nacionais.csv")
    feriados = gerar_feriados_nacionais(anos)
    feriados_path = output_dir / "feriados_nacionais.csv"
    with feriados_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["data", "nome"])
        writer.writeheader()
        writer.writerows(feriados)
    log.info("feriados_nacionais.csv: %d registros → %s", len(feriados), feriados_path)

    # dim_data_seed.csv
    log.info("Gerando dim_data_seed.csv")
    registros = gerar_dim_data(START_DATE, END_DATE)
    dim_data_path = output_dir / "dim_data_seed.csv"
    fieldnames = [
        "data",
        "ano",
        "mes",
        "mes_nome",
        "trimestre",
        "semestre",
        "dia_da_semana",
        "dia_da_semana_nome",
    ]
    with dim_data_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(registros)
    log.info("dim_data_seed.csv: %d registros → %s", len(registros), dim_data_path)


if __name__ == "__main__":
    main()
