# -*- coding: utf-8 -*-
"""
Script standalone: gera o arquivo SA1_Clientes filtrado (ultimos 24 meses).

Execute na maquina que tem acesso ao servidor de arquivos (Z:).
O arquivo gerado fica em Z:\...\Cadastro de Clientes\Tabela SA1 - 24 meses\

Uso:
    python gerar_sa1_clientes.py               # gera o arquivo
    python gerar_sa1_clientes.py --agendar     # agendamento automatico no dia 23 de cada mes
"""

import argparse
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

# ─── Configuracao ─────────────────────────────────────────────────────────────
PLANILHA_SA1 = Path(
    r"Z:\4 - Gestão de Receitas e Apuração de Resultados"
    r"\4.4 - Núcleo de Informações\Tabelas - Protheus\SA1_Clientes.csv"
)
MESES_LIMITE = 24
COL_CNPJ_IDX = 14   # O — CNPJ/CPF
COL_DATA_IDX = 17   # R — Último Contato

SAIDA_PADRAO = Path(
    r"Z:\4 - Gestão de Receitas e Apuração de Resultados"
    r"\4.4 - Núcleo de Informações\Cadastro de Clientes\Tabela SA1 - 24 meses"
)


def gerar(saida: Path) -> None:
    if not PLANILHA_SA1.exists():
        raise FileNotFoundError(f"SA1 nao encontrado: {PLANILHA_SA1}")

    print(f"Lendo: {PLANILHA_SA1}")
    df = pd.read_csv(PLANILHA_SA1, header=0, dtype=str, encoding="latin-1", sep=";")

    col_data = df.columns[COL_DATA_IDX]
    df["_dt"] = pd.to_datetime(df[col_data], dayfirst=True, errors="coerce")
    limite = datetime.now() - timedelta(days=MESES_LIMITE * 30)
    df = df[df["_dt"] >= limite].drop(columns=["_dt"])

    col_cnpj = df.columns[COL_CNPJ_IDX]
    df = df[df[col_cnpj].notna()]

    nome = f"SA1_Clientes_{datetime.now().strftime('%Y%m%d')}.xlsx"
    destino = saida / nome
    saida.mkdir(parents=True, exist_ok=True)
    df.to_excel(destino, index=False, sheet_name="SA1_Clientes")

    print(f"Arquivo gerado: {destino}")
    print(f"Total de clientes (ultimos {MESES_LIMITE} meses): {len(df)}")


def agendar() -> None:
    """Registra tarefa no Windows Task Scheduler para rodar todo dia 23 as 07:00."""
    script = Path(sys.argv[0]).resolve()
    python = Path(sys.executable).resolve()
    nome_tarefa = "TOTVS_SA1_Clientes_Dia23"

    cmd = (
        f'schtasks /Create /F /TN "{nome_tarefa}" '
        f'/TR "\\"{python}\\" \\"{script}\\"" '
        f'/SC MONTHLY /D 23 /ST 07:00 '
        f'/RL HIGHEST'
    )

    print(f"Registrando tarefa: {nome_tarefa}")
    print(f"  Script : {script}")
    print(f"  Python : {python}")
    print(f"  Agenda : Todo dia 23 as 07:00\n")

    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print("Tarefa agendada com sucesso!")
        print("Para verificar: Agendador de Tarefas > Biblioteca > TOTVS_SA1_Clientes_Dia23")
    else:
        print("Erro ao agendar:")
        print(result.stderr or result.stdout)
        print("\nTente rodar como Administrador.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Gera SA1_Clientes filtrado para o Portal de Automacoes"
    )
    parser.add_argument(
        "--saida",
        type=Path,
        default=SAIDA_PADRAO,
        help=f"Pasta onde salvar o arquivo (padrao: {SAIDA_PADRAO})",
    )
    parser.add_argument(
        "--agendar",
        action="store_true",
        help="Registra tarefa no Windows Task Scheduler (todo dia 23 as 07:00)",
    )
    args = parser.parse_args()

    if args.agendar:
        agendar()
    else:
        gerar(args.saida)
