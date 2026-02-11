# conciliacao_excel.py
# Motor da conciliacao bancaria
#
# Saida Excel com 3 abas:
# - CONCILIACAO
# - REVISAO (divergentes + possiveis matches por data/valor)
# - SALDO_FINAL (coluna J e K dos dois relatorios e diferenca)

import pandas as pd
import re
import os


def to_float_br(x) -> float:
    """Converte valores (inclusive formato BR) para float."""
    if isinstance(x, (int, float)) and not pd.isna(x):
        return float(x)

    if x is None or (isinstance(x, float) and pd.isna(x)):
        return 0.0

    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return 0.0

    s = s.replace(" ", "")
    if "," in s:
        s = s.replace(".", "").replace(",", ".")

    try:
        return float(s)
    except:
        return 0.0


def limpar_prefixo(txt: str) -> str:
    """Remove prefixos/sufixos comuns para melhorar match."""
    if not txt:
        return ""

    txt = str(txt).strip()

    prefixos_comuns = [
        "MED-", "FT-", "DP-", "BOL-", "NF-", "RC-", "FOL-",
        "MED", "FT", "DP", "BOL", "NF", "RC", "FOL"
    ]
    for p in prefixos_comuns:
        if txt.upper().startswith(p):
            txt = txt[len(p):]
            break

    txt = re.sub(r"-[A-Z]$", "", txt)       # -A, -B...
    txt = re.sub(r"-\d{1,2}$", "", txt)     # -01, -2...
    return txt


def normalizar_texto(txt: str) -> str:
    if txt is None:
        return ""
    txt = limpar_prefixo(txt)
    s = str(txt).upper().strip()
    s = re.sub(r"[^A-Z0-9]", "", s)
    return s


def extrair_numeros(txt: str) -> str:
    if not txt:
        return ""
    txt = limpar_prefixo(txt)
    numeros = re.sub(r"[^0-9]", "", str(txt))
    return numeros


def extrair_identificador_fin(texto: str) -> str:
    """Extrai identificador do Relatorio Financeiro."""
    if not texto:
        return ""

    texto = limpar_prefixo(texto)
    texto = re.sub(r"^\d+\s*-\s*", "", texto)  # remove "5 -"

    patterns = [
        r"(\d{6,})",
        r"([A-Z]{2,}\d{4,})",
    ]
    for p in patterns:
        m = re.search(p, texto)
        if m:
            return m.group(1)

    return texto


def extrair_identificador_contabil(historico: str) -> str:
    """Extrai identificador do Relatorio Contabil (historico)."""
    if not historico:
        return ""

    patterns = [
        r"5\s*-?\s*(\d{6,})",
        r"(\d{9,})",
        r"([A-Z]{2,}\d{4,})",
        r"([A-Z0-9]{6,})",
    ]
    for p in patterns:
        m = re.search(p, historico)
        if m:
            ident = re.sub(r"[^A-Z0-9]", "", m.group(1))
            if re.search(r"\d{6,}", ident) and len(ident) > 0 and ident[-1].isalpha():
                ident = ident[:-1]
            return ident

    return ""


def _promover_header(df: pd.DataFrame, required_cols=None) -> pd.DataFrame:
    """
    Promove a primeira linha para header SOMENTE quando necessario.
    - Se ja tem colunas esperadas, nao mexe.
    """
    required_cols = required_cols or []

    # 1) Ja tem as colunas obrigatorias? entao esta OK.
    if required_cols and all(c in df.columns for c in required_cols):
        return df

    # 2) Se colunas sao strings e nao sao "Unnamed", normalmente ja esta OK.
    if all(isinstance(c, str) for c in df.columns) and not any(str(c).startswith("Unnamed") for c in df.columns):
        return df

    # 3) Caso contrario, promove primeira linha
    header = df.iloc[0].tolist()
    out = df.iloc[1:].copy()
    out.columns = header
    return out


def ler_financeiro(financeiro_path: str) -> tuple:
    """
    Relatorio Financeiro (antigo FIN)
    Aba esperada: "2-Totais"
    Colunas esperadas (minimo):
      OPERACAO, PREFIXO/TITULO, ENTRADAS, SAIDAS
    DATA e opcional (se existir, melhora match)
    """
    try:
        df_raw = pd.read_excel(financeiro_path, sheet_name="2-Totais")
    except ValueError:
        xls = pd.ExcelFile(financeiro_path)
        abas = ', '.join(xls.sheet_names)
        raise ValueError(
            f"[Relatorio Financeiro] Aba '2-Totais' nao encontrada!\n\n"
            f"Abas disponiveis no arquivo: {abas}\n\n"
            f"Verifique se voce selecionou o arquivo correto no campo 'Financeiro'."
        )
    df = _promover_header(df_raw, required_cols=["OPERACAO", "PREFIXO/TITULO", "ENTRADAS", "SAIDAS"])

    obrig = ["OPERACAO", "PREFIXO/TITULO", "ENTRADAS", "SAIDAS"]
    if "DATA" in df.columns:
        obrig.append("DATA")

    for col in obrig:
        if col not in df.columns:
            raise ValueError(
                f"[Relatorio Financeiro] Coluna '{col}' nao encontrada!\n\n"
                f"Colunas esperadas: OPERACAO, PREFIXO/TITULO, ENTRADAS, SAIDAS\n"
                f"Colunas encontradas: {list(df.columns)}"
            )

    df["TEXTO_FIN"] = df["PREFIXO/TITULO"].fillna("").astype(str).str.strip()
    vazio = df["TEXTO_FIN"].eq("") | df["TEXTO_FIN"].str.lower().eq("nan")
    df.loc[vazio, "TEXTO_FIN"] = df.loc[vazio, "OPERACAO"].fillna("").astype(str).str.strip()

    df["TEXTO_FIN_NORM"] = df["TEXTO_FIN"].apply(normalizar_texto)
    df["TEXTO_FIN_NUM"] = df["TEXTO_FIN"].apply(extrair_numeros)
    df["IDENTIFICADOR_FIN"] = df["TEXTO_FIN"].apply(extrair_identificador_fin)

    df["ENTRADAS_F"] = df["ENTRADAS"].apply(to_float_br)
    df["SAIDAS_F"] = df["SAIDAS"].apply(to_float_br)

    df["TIPO_FIN"] = df.apply(lambda r: "SAIDA" if r["SAIDAS_F"] > 0 else "ENTRADA", axis=1)
    df["VALOR_FIN"] = df.apply(lambda r: r["SAIDAS_F"] if r["SAIDAS_F"] > 0 else r["ENTRADAS_F"], axis=1)

    if "DATA" in df.columns:
        df["DATA_FIN"] = pd.to_datetime(df["DATA"], errors="coerce", dayfirst=True)

    # remove linhas zeradas / vazias
    df = df[df["VALOR_FIN"] != 0.0].copy()
    df = df[df["TEXTO_FIN_NORM"].str.len() > 0].copy()

    # Totais (mantive simples: usa soma das colunas, que e o que voce quer pro SALDO_FINAL)
    totais = {
        "SOMA_ENTRADAS": float(df["ENTRADAS_F"].sum()),
        "SOMA_SAIDAS": float(df["SAIDAS_F"].sum()),
    }
    return df, totais


def _encontrar_aba(xl_file: pd.ExcelFile, nomes_possiveis: list) -> str:
    for sheet in xl_file.sheet_names:
        sheet_lower = sheet.lower()
        for nome in nomes_possiveis:
            if sheet == nome:
                return sheet
            nome_prefix = nome.split()[0].lower() if " " in nome else nome[:5].lower()
            if sheet_lower.startswith(nome_prefix):
                return sheet
            if nome and nome[0].isdigit() and sheet and sheet[0] == nome[0]:
                return sheet
    abas = ', '.join(xl_file.sheet_names)
    raise ValueError(
        f"Nenhuma aba compativel encontrada!\n\n"
        f"O sistema procura abas com nomes como: {', '.join(nomes_possiveis)}\n"
        f"Abas encontradas no arquivo: {abas}\n\n"
        f"Verifique se voce selecionou o arquivo correto."
    )


def ler_contabil(contabil_path: str) -> tuple:
    """
    Relatorio Contabil (antigo RAZAO)
    Aba esperada: "3-Lancamentos Contabeis" (variacoes aceitas)
    Colunas esperadas: HISTORICO, DEBITO (J), CREDITO (K)
    """
    xl = pd.ExcelFile(contabil_path)
    aba = _encontrar_aba(xl, ["3-Lancamentos Contabeis", "3-Lancamentos Contabeis", "3-"])

    df_raw = pd.read_excel(contabil_path, sheet_name=aba)
    df = _promover_header(df_raw, required_cols=["HISTORICO", "DEBITO", "CREDITO"])

    obrig = ["HISTORICO", "DEBITO", "CREDITO"]
    if "DATA" in df.columns:
        obrig.append("DATA")

    for col in obrig:
        if col not in df.columns:
            raise ValueError(
                f"[Relatorio Contabil] Coluna '{col}' nao encontrada!\n\n"
                f"Colunas esperadas: HISTORICO, DEBITO, CREDITO\n"
                f"Colunas encontradas: {list(df.columns)}"
            )

    df["HIST_TXT"] = df["HISTORICO"].fillna("").astype(str).str.strip()
    df["HIST_NORM"] = df["HIST_TXT"].apply(normalizar_texto)
    df["HIST_NUM"] = df["HIST_TXT"].apply(extrair_numeros)
    df["IDENTIFICADOR"] = df["HIST_TXT"].apply(extrair_identificador_contabil)

    df["DEBITO_F"] = df["DEBITO"].apply(to_float_br)
    df["CREDITO_F"] = df["CREDITO"].apply(to_float_br)

    df["TIPO_CONTABIL"] = df.apply(lambda r: "DEBITO" if r["DEBITO_F"] > 0 else "CREDITO", axis=1)
    df["VALOR_CONTABIL"] = df.apply(lambda r: r["DEBITO_F"] if r["DEBITO_F"] > 0 else r["CREDITO_F"], axis=1)

    if "DATA" in df.columns:
        df["DATA_CONTABIL"] = pd.to_datetime(df["DATA"], errors="coerce", dayfirst=True)

    df = df[df["VALOR_CONTABIL"] != 0.0].copy()
    df = df[df["HIST_NORM"].str.len() > 0].copy()

    totais = {
        "SOMA_DEBITO": float(df["DEBITO_F"].sum()),
        "SOMA_CREDITO": float(df["CREDITO_F"].sum()),
    }
    return df, totais


def conciliar(fin_df: pd.DataFrame, cont_df: pd.DataFrame, tolerancia=0.01, min_len=3) -> pd.DataFrame:
    """
    Regras:
    - Contabil DEBITO => Financeiro ENTRADA
    - Contabil CREDITO => Financeiro SAIDA
    - Valor bate (tolerancia)
    - Texto bate (contem) OU numeros batem OU identificador bate
    - Data (se existir nos dois): aceita diferenca de ate 1 dia
    - Cada linha contabil so pode ser usada 1 vez
    """
    usados_cont = set()
    out = []

    usar_data = ("DATA_FIN" in fin_df.columns) and ("DATA_CONTABIL" in cont_df.columns)

    # indice por (valor_arredondado, tipo_contabil)
    cont_by_val_tipo = {}
    for idx_c, r in cont_df.iterrows():
        k_val = round(float(r["VALOR_CONTABIL"]), 2)
        k_tipo = r["TIPO_CONTABIL"]
        cont_by_val_tipo.setdefault((k_val, k_tipo), []).append(idx_c)

    def candidatos_por_valor_tipo(v: float, tipo_fin: str, tol: float) -> list:
        tipo_cont = "DEBITO" if tipo_fin == "ENTRADA" else "CREDITO"
        base = round(float(v), 2)

        keys = [(base, tipo_cont)]
        if tol >= 0.01:
            keys += [
                (round(base - 0.01, 2), tipo_cont),
                (round(base + 0.01, 2), tipo_cont),
                (round(base - 0.02, 2), tipo_cont),
                (round(base + 0.02, 2), tipo_cont),
            ]

        cand = []
        for k in keys:
            cand.extend(cont_by_val_tipo.get(k, []))
        return list(set(cand))

    def score_match(txt_norm: str, hist_norm: str, txt_num: str, hist_num: str,
                    data_fin, data_cont, usar_data: bool, id_fin: str, id_cont: str) -> float:
        if not txt_norm or not hist_norm:
            return 0.0

        cov = min(len(txt_norm) / max(len(hist_norm), 1), 1.0)
        bonus_inicio = 0.2 if hist_norm.startswith(txt_norm) else 0.0
        bonus_num = 0.3 if txt_num and hist_num and txt_num in hist_num else 0.0
        bonus_id = 0.5 if id_fin and id_cont and len(id_fin) >= 6 and id_fin in id_cont else 0.0

        bonus_data = 0.0
        if usar_data and pd.notna(data_fin) and pd.notna(data_cont):
            dif_dias = abs((data_fin - data_cont).days)
            if dif_dias == 0:
                bonus_data = 0.5
            elif dif_dias == 1:
                bonus_data = 0.3

        return cov + bonus_inicio + bonus_num + bonus_id + bonus_data

    for _, f in fin_df.iterrows():
        txt_norm = str(f.get("TEXTO_FIN_NORM") or "")
        txt_num = str(f.get("TEXTO_FIN_NUM") or "")
        id_fin = str(f.get("IDENTIFICADOR_FIN") or "")

        if len(txt_norm) < min_len and len(id_fin) < min_len:
            continue

        v_fin = float(f["VALOR_FIN"])
        tipo_fin = f["TIPO_FIN"]
        data_fin = f.get("DATA_FIN") if usar_data else None

        cand_idxs = candidatos_por_valor_tipo(v_fin, tipo_fin, tolerancia)

        melhor_idx = None
        melhor_score = -1.0

        tipo_cont_esperado = "DEBITO" if tipo_fin == "ENTRADA" else "CREDITO"

        for idx_c in cand_idxs:
            if idx_c in usados_cont:
                continue

            r = cont_df.loc[idx_c]
            v_c = float(r["VALOR_CONTABIL"])

            if abs(v_fin - v_c) > tolerancia:
                continue

            if r["TIPO_CONTABIL"] != tipo_cont_esperado:
                continue

            hist_norm = r["HIST_NORM"]
            hist_num = r["HIST_NUM"]
            id_cont = str(r.get("IDENTIFICADOR") or "")
            data_cont = r.get("DATA_CONTABIL") if usar_data else None

            if usar_data and pd.notna(data_fin) and pd.notna(data_cont):
                if abs((data_fin - data_cont).days) > 1:
                    continue

            match_texto = txt_norm in hist_norm
            match_num = (txt_num and hist_num and txt_num in hist_num)
            match_id = (id_fin and id_cont and len(id_fin) >= 6 and id_fin in id_cont)

            if not (match_texto or match_num or match_id):
                continue

            sc = score_match(txt_norm, hist_norm, txt_num, hist_num, data_fin, data_cont, usar_data, id_fin, id_cont)
            if sc > melhor_score:
                melhor_score = sc
                melhor_idx = idx_c

        if melhor_idx is not None:
            usados_cont.add(melhor_idx)
            r = cont_df.loc[melhor_idx]

            out.append({
                "STATUS": "OK",

                "DATA_RELATORIO_FINANCEIRO": data_fin.strftime("%d/%m/%Y") if pd.notna(data_fin) else "",
                "TIPO_RELATORIO_FINANCEIRO": tipo_fin,
                "TEXTO_RELATORIO_FINANCEIRO": f["TEXTO_FIN"],
                "VALOR_RELATORIO_FINANCEIRO": v_fin,

                "DATA_RELATORIO_CONTABIL": r.get("DATA_CONTABIL").strftime("%d/%m/%Y") if pd.notna(r.get("DATA_CONTABIL")) else "",
                "TIPO_RELATORIO_CONTABIL": r["TIPO_CONTABIL"],
                "HISTORICO_RELATORIO_CONTABIL": r["HIST_TXT"],
                "VALOR_RELATORIO_CONTABIL": float(r["VALOR_CONTABIL"]),

                "DIF": abs(v_fin - float(r["VALOR_CONTABIL"])),
                "OBS": "Conciliado automaticamente"
            })
        else:
            out.append({
                "STATUS": "DIVERGENTE",

                "DATA_RELATORIO_FINANCEIRO": data_fin.strftime("%d/%m/%Y") if pd.notna(data_fin) else "",
                "TIPO_RELATORIO_FINANCEIRO": tipo_fin,
                "TEXTO_RELATORIO_FINANCEIRO": f["TEXTO_FIN"],
                "VALOR_RELATORIO_FINANCEIRO": v_fin,

                "DATA_RELATORIO_CONTABIL": "",
                "TIPO_RELATORIO_CONTABIL": "",
                "HISTORICO_RELATORIO_CONTABIL": "",
                "VALOR_RELATORIO_CONTABIL": 0.0,

                "DIF": v_fin,
                "OBS": "Nao conciliado"
            })

    # Sobras do Contabil (nao conciliadas)
    for idx_c, r in cont_df.iterrows():
        if idx_c not in usados_cont:
            data_contabil = r.get("DATA_CONTABIL")
            out.append({
                "STATUS": "DIVERGENTE",

                "DATA_RELATORIO_FINANCEIRO": "",
                "TIPO_RELATORIO_FINANCEIRO": "",
                "TEXTO_RELATORIO_FINANCEIRO": "",
                "VALOR_RELATORIO_FINANCEIRO": 0.0,

                "DATA_RELATORIO_CONTABIL": data_contabil.strftime("%d/%m/%Y") if pd.notna(data_contabil) else "",
                "TIPO_RELATORIO_CONTABIL": r["TIPO_CONTABIL"],
                "HISTORICO_RELATORIO_CONTABIL": r["HIST_TXT"],
                "VALOR_RELATORIO_CONTABIL": float(r["VALOR_CONTABIL"]),

                "DIF": float(r["VALOR_CONTABIL"]),
                "OBS": "Sem correspondencia no Financeiro"
            })

    return pd.DataFrame(out)


def encontrar_possiveis_matches_data_valor(fin_df: pd.DataFrame, cont_df: pd.DataFrame,
                                          conc_df: pd.DataFrame, tolerancia=0.02) -> pd.DataFrame:
    """
    Para revisao: tenta achar possiveis matches para divergentes
    apenas por DATA (+/-1 dia) e VALOR (tolerancia), respeitando o TIPO.
    """
    if "DATA_FIN" not in fin_df.columns or "DATA_CONTABIL" not in cont_df.columns:
        return pd.DataFrame()

    diverg_fin = conc_df[
        (conc_df["STATUS"] != "OK") &
        (conc_df["TEXTO_RELATORIO_FINANCEIRO"].notna()) &
        (conc_df["TEXTO_RELATORIO_FINANCEIRO"] != "")
    ].copy()

    possiveis = []

    for _, row in diverg_fin.iterrows():
        texto_fin = row["TEXTO_RELATORIO_FINANCEIRO"]
        tipo_fin = row["TIPO_RELATORIO_FINANCEIRO"]
        valor_fin = float(row["VALOR_RELATORIO_FINANCEIRO"])

        fin_match = fin_df[fin_df["TEXTO_FIN"] == texto_fin]
        if fin_match.empty:
            continue

        data_fin = fin_match.iloc[0].get("DATA_FIN")
        if pd.isna(data_fin):
            continue

        tipo_cont_esperado = "DEBITO" if tipo_fin == "ENTRADA" else "CREDITO"

        for _, r in cont_df.iterrows():
            if r["TIPO_CONTABIL"] != tipo_cont_esperado:
                continue

            data_c = r.get("DATA_CONTABIL")
            if pd.isna(data_c):
                continue

            valor_c = float(r["VALOR_CONTABIL"])
            if abs(valor_fin - valor_c) > tolerancia:
                continue

            dif_dias = abs((pd.to_datetime(data_fin) - pd.to_datetime(data_c)).days)
            if dif_dias > 1:
                continue

            possiveis.append({
                "STATUS": "REVISAR",
                "DATA_RELATORIO_FINANCEIRO": data_fin.strftime("%d/%m/%Y") if pd.notna(data_fin) else "",
                "TIPO_RELATORIO_FINANCEIRO": tipo_fin,
                "TEXTO_RELATORIO_FINANCEIRO": texto_fin,
                "VALOR_RELATORIO_FINANCEIRO": valor_fin,
                "DATA_RELATORIO_CONTABIL": data_c.strftime("%d/%m/%Y") if pd.notna(data_c) else "",
                "TIPO_RELATORIO_CONTABIL": r["TIPO_CONTABIL"],
                "HISTORICO_RELATORIO_CONTABIL": r["HIST_TXT"],
                "VALOR_RELATORIO_CONTABIL": valor_c,
                "DIF": abs(valor_fin - valor_c),
                "OBS": "Data e valor proximos"
            })

    return pd.DataFrame(possiveis)


def processar_conciliacao(financeiro_path: str, contabil_path: str, output_xlsx: str,
                          tolerancia=0.01, min_len=3) -> dict:
    """
    Gera Excel final com 3 abas:
    - CONCILIACAO
    - REVISAO
    - SALDO_FINAL
    """
    fin_df, fin_totais = ler_financeiro(financeiro_path)
    cont_df, cont_totais = ler_contabil(contabil_path)

    conc_df = conciliar(fin_df, cont_df, tolerancia=tolerancia, min_len=min_len)

    # REVISAO = divergentes + possiveis matches
    divergentes = conc_df[conc_df["STATUS"] != "OK"].copy()
    possiveis = encontrar_possiveis_matches_data_valor(fin_df, cont_df, conc_df, tolerancia=max(tolerancia, 0.02))

    if not possiveis.empty:
        revisao_df = pd.concat([divergentes, possiveis], ignore_index=True)
    else:
        revisao_df = divergentes

    # SALDO_FINAL (como voce pediu: soma J e K e faz J-K para ambos)
    # Contabil: J=DEBITO, K=CREDITO
    contabil_J = float(cont_totais["SOMA_DEBITO"])
    contabil_K = float(cont_totais["SOMA_CREDITO"])
    contabil_J_menos_K = contabil_J - contabil_K

    # Financeiro: J=ENTRADAS, K=SAIDAS
    financeiro_J = float(fin_totais["SOMA_ENTRADAS"])
    financeiro_K = float(fin_totais["SOMA_SAIDAS"])
    financeiro_J_menos_K = financeiro_J - financeiro_K

    saldo_final_df = pd.DataFrame([
        {
            "RELATORIO": "Relatorio Contabil",
            "SOMA_COLUNA_J": contabil_J,
            "SOMA_COLUNA_K": contabil_K,
            "J_MENOS_K": contabil_J_menos_K
        },
        {
            "RELATORIO": "Relatorio Financeiro",
            "SOMA_COLUNA_J": financeiro_J,
            "SOMA_COLUNA_K": financeiro_K,
            "J_MENOS_K": financeiro_J_menos_K
        },
        {
            "RELATORIO": "DIFERENCA (Financeiro - Contabil)",
            "SOMA_COLUNA_J": "",
            "SOMA_COLUNA_K": "",
            "J_MENOS_K": (financeiro_J_menos_K - contabil_J_menos_K)
        }
    ])

    # Salvar com apenas 3 abas
    os.makedirs(os.path.dirname(output_xlsx) or ".", exist_ok=True)
    with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
        conc_df.to_excel(writer, sheet_name="CONCILIACAO", index=False)
        revisao_df.to_excel(writer, sheet_name="REVISAO", index=False)
        saldo_final_df.to_excel(writer, sheet_name="SALDO_FINAL", index=False)

    # stats pro front
    total = len(conc_df)
    ok = int((conc_df["STATUS"] == "OK").sum()) if total else 0
    nao_ok = total - ok
    perc = round((ok / total * 100), 2) if total else 0.0

    stats = {
        "total": total,
        "ok": ok,
        "nao_ok": nao_ok,
        "percentual": perc,
        "financeiro_coluna_j": f"R$ {financeiro_J:,.2f}",
        "financeiro_coluna_k": f"R$ {financeiro_K:,.2f}",
        "financeiro_j_menos_k": f"R$ {financeiro_J_menos_K:,.2f}",
        "contabil_coluna_j": f"R$ {contabil_J:,.2f}",
        "contabil_coluna_k": f"R$ {contabil_K:,.2f}",
        "contabil_j_menos_k": f"R$ {contabil_J_menos_K:,.2f}",
        "diferenca_fin_menos_cont": f"R$ {(financeiro_J_menos_K - contabil_J_menos_K):,.2f}",
        "revisao_qtd": int(len(revisao_df)),
    }
    return stats