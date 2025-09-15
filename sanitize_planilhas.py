# sanitize_planilhas.py
import os, csv, io, shutil, re
import pandas as pd

ARQS_MULTAS = {
    "detalhamento": "Notificações de Multas - Detalhamento",
    "pastores":     "Notificações de Multas - Fase Pastores",
    "cond_ident":   "Notificações de Multas - Condutor Identificado",
}
ARQS_COMBUSTIVEL = {
    "extrato_geral":        "ExtratoGeral",
    "extrato_simplificado": "ExtratoSimplificado",
}

CANDS = {
    "fluig":  ["Nº Fluig","No Fluig","Num Fluig","FLUIG","Fluig","Nº  Fluig","N° Fluig","Nº do Fluig","Nº  do Fluig"],
    "status": ["Status","STATUS","SITUAÇÃO","SITUACAO"],
}

HEADERS_EXATOS = {
    "detalhamento": [
        "Nº Fluig","Status","UF","Placa","Bloco","Região","Igreja","Nome","CPF",
        "Título","Infração","AIT","Data Infração","Hora Infração","Data Limite",
        "Local","Data Solicitação","Valor Total"
    ],
    "pastores": [
        "Nº Fluig","UF","Placa","Bloco","Região","Igreja","Nome","CPF","Título",
        "Infração","Data Infração","Hora Infração","Data Limite","Local",
        "Data Solicitação","Data Pastores","Localização","Tipo","AIT","Qtd","Valor Total"
    ],
    "cond_ident": [
        "Nº Fluig","UF","Placa","Bloco","Região","Igreja","Título","Nome","CPF",
        "Depto","Nome Identificado","CPF Identificado","SOL_TXT_AIT",
        "Função Identificado","Qtd","Valor Total"
    ],
    "extrato_geral": [
        "CODIGO TRANSACAO","FORMA DE PAGAMENTO","CODIGO CLIENTE","NOME REDUZIDO",
        "DATA TRANSACAO","PLACA","TIPO FROTA","MODELO VEICULO","NUMERO FROTA","ANO",
        "MATRICULA","NOME MOTORISTA","SERVICO","TIPO COMBUSTIVEL","LITROS","VL/LITRO",
        "HODOMETRO OU HORIMETRO","KM RODADOS OU HORAS TRABALHADAS",
        "KM/LITRO OU LITROS/HORA","VALOR EMISSAO","CODIGO ESTABELECIMENTO",
        "NOME ESTABELECIMENTO","TIPO ESTABELECIMENTO","ENDERECO","BAIRRO","CIDADE","UF",
        "INFORMACAO ADIDIONAL 1","INFORMACAO ADICIONAL 2","INFORMACAO ADICIONAL 3",
        "INFORMACAO ADICIONAL 4","INFORMACAO ADICIONAL 5","FORMA TRANSACAO",
        "CODIGO LIBERACAO RESTRICAO","SERIE POS","NUMERO CARTAO","FAMILIA VEICULO",
        "GRUPO RESTRICAO","CODIGO EMISSORA","RESPONSAVEL","TIPO ENTRADA HODOMETRO"
    ],
    "extrato_simplificado": [
        "Cartão","Placa","Nr. Frota","Família","Tipo Frota","Modelo","Fabricante",
        "Cidade/UF","Nome Responsável","Limite","Valor Reservado","Limite Atual",
        "Compras (utilizado)","Saldo","Limite Próximo Período"
    ],
}

KEEP_ONLY_OFFICIAL_DEFAULT = True

def _detect_csv_sep(path: str) -> str:
    try:
        with open(path, "rb") as f:
            head = f.read(4096)
        try:
            txt = head.decode("utf-8")
        except UnicodeDecodeError:
            txt = head.decode("latin1", errors="ignore")
        first_line = txt.splitlines()[0] if txt else ""
        commas = first_line.count(",")
        semis  = first_line.count(";")
        if semis >= commas:
            return ";"
        return ","
    except Exception:
        return ";"

def _read_any(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext == ".csv":
            try:
                return pd.read_csv(path, dtype=str, sep=None, engine="python", header=None).fillna("")
            except UnicodeDecodeError:
                return pd.read_csv(path, dtype=str, sep=None, engine="python", encoding="latin1", header=None).fillna("")
        elif ext in (".xlsx",".xls",".xlsm"):
            return pd.read_excel(path, dtype=str, header=None).fillna("")
        else:
            return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

def _resolve_path(root, stem):
    for ext in (".xlsx",".xls",".xlsm",".csv"):
        p = os.path.join(root, stem+ext)
        if os.path.isfile(p): return p
    for item in os.listdir(root):
        if item.lower().startswith(stem.lower()) and os.path.isfile(os.path.join(root,item)):
            return os.path.join(root,item)
    return ""

def _looks_like_value(x: str) -> bool:
    s = str(x).strip()
    if s == "": return False
    if re.fullmatch(r"\d{3}\.\d{3}\.\d{3}-\d{2}", s): return True
    if re.fullmatch(r"[A-Z]{3}[- ]?\d{4}", s): return True
    if re.fullmatch(r"\d{4,}", s): return True
    if re.fullmatch(r"[A-ZÇÁÉÍÓÚÂÊÔÃÕ ]{3,}", s) and len(s) > 12: return True
    if re.fullmatch(r"[A-Z]\s?\d{6,}", s): return True
    if re.fullmatch(r"[ST]\s?\d{6,}", s): return True
    return False

def _score_header_row(cells, expected_set):
    norm = [str(c).strip() for c in cells]
    exact_hits = sum(1 for c in norm if c in expected_set)
    upper = [c.upper() for c in norm]
    loose_hits = sum(1 for c in upper if c in {e.upper() for e in expected_set})
    noise_penalty = sum(1 for c in norm if _looks_like_value(c))
    nonempty = sum(1 for c in norm if c != "")
    score = exact_hits*5 + loose_hits*2 + nonempty - noise_penalty*3
    return score, exact_hits, loose_hits, noise_penalty, nonempty

def _robust_headerize(df_raw: pd.DataFrame, expected_headers: list, scan_rows: int = 30) -> pd.DataFrame:
    if df_raw is None or df_raw.empty:
        return pd.DataFrame()
    exp_set = set(expected_headers)
    best = (-10**9, -1)
    for i in range(min(scan_rows, len(df_raw))):
        score_tuple = _score_header_row(df_raw.iloc[i].tolist(), exp_set)
        score = score_tuple[0]
        best = max(best, (score, i))
    header_idx = best[1] if best[1] >= 0 else 0
    data = df_raw.copy()
    cols = data.iloc[header_idx].map(lambda x: str(x).strip()).tolist()
    seen = {}
    uniq = []
    for c in cols:
        base = c if c != "" else "COL"
        k = base
        j = 1
        while k in seen:
            j += 1
            k = f"{base}.{j}"
        seen[k] = 1
        uniq.append(k)
    data.columns = uniq
    data = data.iloc[header_idx+1:].reset_index(drop=True)
    data = data.dropna(how="all")
    data = data[~(data.astype(str).apply(lambda s: s.str.strip()).eq("").all(axis=1))]
    data = data.applymap(lambda x: str(x).strip())
    return data.reset_index(drop=True)

def _find_existing_col(df, candidates):
    for c in candidates:
        if c in df.columns: return c
    up = {str(c).strip().upper(): c for c in df.columns}
    for c in candidates:
        key = str(c).strip().upper()
        if key in up: return up[key]
    return None

def _force_headers_exact(df: pd.DataFrame, headers: list) -> pd.DataFrame:
    if df is None:
        df = pd.DataFrame()
    df = df.copy()
    for h in headers:
        if h not in df.columns:
            df[h] = ""
    extras = [c for c in df.columns if c not in headers]
    ordered = headers + extras
    return df[ordered]

def _select_only_official(df: pd.DataFrame, headers: list) -> pd.DataFrame:
    keep = [h for h in headers if h in df.columns]
    return df[keep].copy()

def _official_fill_ratio(df: pd.DataFrame, headers: list, sample_rows: int = 50) -> float:
    if df is None or df.empty:
        return 0.0
    sub = df[headers] if all(h in df.columns for h in headers) else pd.DataFrame()
    if sub.empty:
        return 0.0
    n = min(sample_rows, len(sub))
    if n == 0:
        return 0.0
    sub = sub.iloc[:n]
    filled = (sub.astype(str).applymap(lambda x: x.strip() != "")).sum().sum()
    total = sub.shape[0] * sub.shape[1]
    return filled / max(total, 1)

def _write_back_same_format(df: pd.DataFrame, out_path: str, sheet_name: str = "Planilha"):
    ext = os.path.splitext(out_path)[1].lower()
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    if df is None:
        df = pd.DataFrame()
    if ext in (".xlsx", ".xls", ".xlsm"):
        if ext == ".xls":
            out_path = os.path.splitext(out_path)[0] + ".xlsx"
        with pd.ExcelWriter(out_path, engine="openpyxl", mode="w") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False, header=True)
            try:
                ws = writer.sheets[sheet_name]
                for col_idx, col_name in enumerate(df.columns, start=1):
                    max_len = max(
                        len(str(col_name)),
                        *(len(str(v)) for v in df[col_name].astype(str).values) if not df.empty else [len(str(col_name))]
                    )
                    max_len = min(max_len, 80)
                    ws.column_dimensions[ws.cell(row=1, column=col_idx).column_letter].width = max(10, max_len + 2)
            except Exception:
                pass
    else:
        sep = _detect_csv_sep(out_path)
        df.to_csv(
            out_path,
            index=False,
            sep=sep,
            header=True,
            quoting=csv.QUOTE_MINIMAL,
            encoding="utf-8-sig",
            lineterminator="\n",
        )

def _process_generic(df_raw: pd.DataFrame, expected_headers: list) -> pd.DataFrame:
    df = _robust_headerize(df_raw, expected_headers)
    df = _force_headers_exact(df, expected_headers)
    ratio = _official_fill_ratio(df, expected_headers)
    if ratio == 0.0:
        return df
    return _select_only_official(df, expected_headers)

def processar_todas_planilhas(root: str, log=lambda *_: None, progress=lambda *_: None) -> dict:
    relatorio = {"ok": True, "erros": [], "saidas": {}}
    total_steps = 14
    step = 0
    def bump(msg):
        nonlocal step
        step += 1
        log(msg)
        progress(int(step/total_steps*100))

    bump("Localizando arquivos na pasta PLANILHAS…")
    p_det_src = _resolve_path(root, ARQS_MULTAS["detalhamento"])
    p_pas_src = _resolve_path(root, ARQS_MULTAS["pastores"])
    p_cid_src = _resolve_path(root, ARQS_MULTAS["cond_ident"])
    p_eg_src  = _resolve_path(root, ARQS_COMBUSTIVEL["extrato_geral"])
    p_es_src  = _resolve_path(root, ARQS_COMBUSTIVEL["extrato_simplificado"])

    def dst_path(src): return os.path.join(".", os.path.basename(src)) if src else ""

    p_det = dst_path(p_det_src)
    p_pas = dst_path(p_pas_src)
    p_cid = dst_path(p_cid_src)
    p_eg  = dst_path(p_eg_src)
    p_es  = dst_path(p_es_src)

    bump("Copiando originais para a raiz do projeto…")
    for s, d in [(p_det_src,p_det),(p_pas_src,p_pas),(p_cid_src,p_cid),(p_eg_src,p_eg),(p_es_src,p_es)]:
        if s:
            try:
                shutil.copy2(s, d)
                log(f"Copiado: {os.path.basename(s)} → {d}")
            except Exception as e:
                relatorio["erros"].append(f"Falha ao copiar {s}: {e}")
        else:
            relatorio["erros"].append("Arquivo não encontrado: " + ("?" if not s else s))

    bump("Lendo cópias (header=None)…")
    det_raw = _read_any(p_det) if p_det and os.path.exists(p_det) else pd.DataFrame()
    pas_raw = _read_any(p_pas) if p_pas and os.path.exists(p_pas) else pd.DataFrame()
    cid_raw = _read_any(p_cid) if p_cid and os.path.exists(p_cid) else pd.DataFrame()
    eg_raw  = _read_any(p_eg)  if p_eg  and os.path.exists(p_eg)  else pd.DataFrame()
    es_raw  = _read_any(p_es)  if p_es  and os.path.exists(p_es)  else pd.DataFrame()

    bump("Processando: Detalhamento…")
    det = _process_generic(det_raw, HEADERS_EXATOS["detalhamento"])
    if not det.empty:
        status_col = _find_existing_col(det, CANDS["status"])
        if status_col and status_col in det.columns:
            # normaliza e mantém somente ABERTA e FINALIZADA
            st = det[status_col].astype(str).str.strip().str.upper()
            st = st.replace({
                "EM ABERTO": "ABERTA",
                "ABERTO": "ABERTA",
                "FINALIZADO": "FINALIZADA",
            })
            ok = st.isin(["ABERTA", "FINALIZADA"])
            det = det.loc[ok].reset_index(drop=True)

    bump("Processando: Fase Pastores…")
    pas = _process_generic(pas_raw, HEADERS_EXATOS["pastores"])

    bump("Processando: Condutor Identificado…")
    cid = _process_generic(cid_raw, HEADERS_EXATOS["cond_ident"])

    bump("Sincronizando FLUIG em Pastores/Condutor com Detalhamento…")
    fluig_det = None
    fluig_col_det = _find_existing_col(det, CANDS["fluig"])
    if fluig_col_det and not det.empty:
        fluig_det = set(det[fluig_col_det].astype(str).str.strip())
    if fluig_det:
        fluig_col_pas = _find_existing_col(pas, CANDS["fluig"])
        if fluig_col_pas and not pas.empty:
            pas = pas[pas[fluig_col_pas].astype(str).str.strip().isin(fluig_det)].copy()
        fluig_col_cid = _find_existing_col(cid, CANDS["fluig"])
        if fluig_col_cid and not cid.empty:
            cid = cid[cid[fluig_col_cid].astype(str).str.strip().isin(fluig_det)].copy()

    bump("Processando: ExtratoGeral…")
    eg  = _process_generic(eg_raw,  HEADERS_EXATOS["extrato_geral"])

    bump("Processando: ExtratoSimplificado…")
    es  = _process_generic(es_raw,  HEADERS_EXATOS["extrato_simplificado"])

    bump("Gravando limpeza nas CÓPIAS…")
    if p_det: _write_back_same_format(det, p_det, sheet_name="Detalhamento")
    if p_pas: _write_back_same_format(pas, p_pas, sheet_name="Fase Pastores")
    if p_cid: _write_back_same_format(cid, p_cid, sheet_name="Condutor Ident.")
    if p_eg:  _write_back_same_format(eg,  p_eg,  sheet_name="Extrato Geral")
    if p_es:  _write_back_same_format(es,  p_es,  sheet_name="Extrato Simplificado")

    relatorio["saidas"] = {
        "copias": {
            "detalhamento": p_det,
            "pastores":     p_pas,
            "condutor_ident": p_cid,
            "extrato_geral": p_eg,
            "extrato_simplificado": p_es,
        },
        "contagens": {
            "detalhamento": len(det),
            "pastores": len(pas),
            "condutor_ident": len(cid),
            "extrato_geral": len(eg),
            "extrato_simplificado": len(es),
        },
        "erros": relatorio["erros"],
    }
    progress(100)
    log("✔ Cópias feitas, limpas e com títulos originais aplicados.")
    return relatorio