# revisao_app.py
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, QPushButton, QTabWidget,
    QTableWidget, QTableWidgetItem, QComboBox, QFileDialog, QMessageBox, QSizePolicy,
    QApplication, QFrame
)

# ===================== Config & Const =====================
APP_DIR = Path(__file__).resolve().parent
CFG_PATH = APP_DIR / "revisao_paths.json"   # onde salvamos os caminhos
IGNORAR_STATUS = {"VENDIDO", "SAIU DA FROTA", "BAIXADO", "BAIXA"}

# ===================== Helpers =====================

def _norm_placa(s: str) -> str:
    if not isinstance(s, str):
        return ""
    return s.upper().replace("-", "").replace(" ", "").strip()

def _to_date(x):
    if pd.isna(x):
        return None
    if isinstance(x, (datetime, date)):
        return x.date() if isinstance(x, datetime) else x
    try:
        d = pd.to_datetime(x, dayfirst=True, errors="coerce")
        return None if pd.isna(d) else d.date()
    except Exception:
        return None

def _to_num(x):
    try:
        if pd.isna(x):
            return math.nan
        if isinstance(x, str):
            s = x.replace(".", "").replace(" ", "").replace("R$", "").replace(",", ".")
            return float(s)
        return float(x)
    except Exception:
        return math.nan

def _find_col(cols: List[str], *hints: str) -> Optional[str]:
    L = [c for c in cols]
    low = [c.lower() for c in cols]
    for hint in hints:
        parts = [p.strip() for p in hint.lower().split() if p.strip()]
        for i, lc in enumerate(low):
            if all(p in lc for p in parts):
                return L[i]
    return None

def _fmt_date(d):
    return "" if (d is None or pd.isna(d)) else d.strftime("%d/%m/%Y")

def _fmt_num(x):
    if x is None or pd.isna(x):
        return ""
    try:
        return f"{int(round(float(x))):,}".replace(",", ".")
    except Exception:
        try:
            return f"{float(x):,.0f}".replace(",", "_").replace(".", ",").replace("_", ".")
        except Exception:
            return str(x)

def _bool_tag(x, on="Renovar"):
    return on if bool(x) else "-"

# ===================== Colunas =====================

@dataclass
class ColunasMap:
    placa: str = "Placa"
    responsavel: Optional[str] = None
    unidade: Optional[str] = None
    regiao: Optional[str] = None
    bloco: Optional[str] = None
    igreja: Optional[str] = None
    marca: Optional[str] = None
    modelo: Optional[str] = None
    ano_modelo: Optional[str] = None

    data_rev: Optional[str] = None
    km_rev: Optional[str] = None
    oficina: Optional[str] = None
    custo_rev: Optional[str] = None

    data_inicio: Optional[str] = None
    status: Optional[str] = None

    data_ext: Optional[str] = None
    km_ext: Optional[str] = None

# ===================== Núcleo =====================

class RevisaoCore:
    """
    - Ignora STATUS: VENDIDO/SAIU DA FROTA/BAIXADO/BAIXA
    - Revisão por data (1 ano) OU km (10.000) a partir da base
    - Base = data da última revisão; se ausente, data de início/compra
    - Renovação: >= 3 anos do ANO MODELO (agora ou na próxima)
    """
    def __init__(self,
                 arq_resp: Optional[Path],
                 arq_rev: Optional[Path],
                 arq_cad: Optional[Path],
                 arq_ext: Optional[Path]):
        self.paths = {
            "resp": Path(arq_resp) if arq_resp else None,
            "rev":  Path(arq_rev)  if arq_rev  else None,
            "cad":  Path(arq_cad)  if arq_cad  else None,
            "ext":  Path(arq_ext)  if arq_ext  else None,
        }
        self.hoje: date = datetime.now().date()

        self.resp = self._load(self.paths["resp"])
        self.rev  = self._load(self.paths["rev"])
        self.cad  = self._load(self.paths["cad"])
        self.ext  = self._load(self.paths["ext"])

        self.cols = self._inferir_colunas()
        self._sanitize_all()
        self._apply_status_filters()

        self.base_ult_revisao = self._build_ultima_revisao_por_placa()
        self.km_por_abastecimento = self._build_km_abastecimentos()

        self.previsao = self._build_previsao()

    # -------- IO --------
    def _load(self, path: Optional[Path]) -> pd.DataFrame:
        if not path:
            return pd.DataFrame()
        if not path.exists():
            print(f"[Revisão] Arquivo não encontrado: {path}")
            return pd.DataFrame()
        try:
            print(f"[Revisão] Lendo: {path}")
            return pd.read_excel(path)
        except Exception as e:
            print(f"[Revisão] Falha ao ler {path}: {e} (tentando openpyxl)")
            return pd.read_excel(path, engine="openpyxl")

    # -------- Schema inference --------
    def _inferir_colunas(self) -> ColunasMap:
        cmap = ColunasMap()

        if not self.resp.empty:
            cols = list(self.resp.columns)
            cmap.placa       = _find_col(cols, "placa") or cmap.placa
            cmap.responsavel = _find_col(cols, "respons")
            cmap.unidade     = _find_col(cols, "setor", "unidade", "lotação", "depart")
            cmap.regiao      = _find_col(cols, "região", "regiao")
            cmap.bloco       = _find_col(cols, "bloco")
            cmap.igreja      = _find_col(cols, "igreja")
            cmap.marca       = _find_col(cols, "marca", "fabricante")
            cmap.modelo      = _find_col(cols, "modelo")
            cmap.ano_modelo  = _find_col(cols, "ano modelo", "ano_modelo", "ano")
            cmap.status      = _find_col(cols, "status")

        if not self.rev.empty:
            cols = list(self.rev.columns)
            cmap.placa    = _find_col(cols, "placa") or cmap.placa
            cmap.data_rev = (_find_col(cols, "data", "última revisão")
                             or _find_col(cols, "data revisão")
                             or _find_col(cols, "data"))
            cmap.km_rev   = _find_col(cols, "km", "quilometr")
            cmap.oficina  = _find_col(cols, "oficina")
            cmap.custo_rev = _find_col(cols, "custo", "valor")

        if not self.cad.empty:
            cols = list(self.cad.columns)
            cmap.placa       = _find_col(cols, "placa") or cmap.placa
            cmap.data_inicio = (_find_col(cols, "data início")
                                or _find_col(cols, "data entrada")
                                or _find_col(cols, "início")
                                or _find_col(cols, "inicio"))
            cmap.marca      = cmap.marca or _find_col(cols, "marca", "fabricante")
            cmap.modelo     = cmap.modelo or _find_col(cols, "modelo")
            cmap.ano_modelo = cmap.ano_modelo or _find_col(cols, "ano modelo", "ano fabricação", "ano")
            if not cmap.status:
                cmap.status = _find_col(cols, "status")

        if not self.ext.empty:
            cols = list(self.ext.columns)
            cmap.placa   = _find_col(cols, "placa") or cmap.placa
            cmap.data_ext = _find_col(cols, "data transa", "data")
            cmap.km_ext   = _find_col(cols, "hodometro", "horimetro", "km")

        return cmap

    # -------- Sanitize --------
    def _sanitize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]

        placa_col = next((c for c in df.columns if c.lower().startswith("placa")), None)
        df["placa_norm"] = df[placa_col].map(_norm_placa) if placa_col else ""

        for cname in df.columns:
            if "data" in cname.lower():
                df[cname] = df[cname].map(_to_date)

        for cname in df.columns:
            cl = cname.lower()
            if ("km" in cl) or ("hodometro" in cl) or ("horimetro" in cl):
                df[cname] = pd.to_numeric(df[cname], errors="coerce")

        for cname in df.columns:
            if ("valor" in cname.lower()) or ("custo" in cname.lower()):
                df[cname] = df[cname].map(_to_num)

        stcol = next((c for c in df.columns if "status" in c.lower()), None)
        df["status_norm_any"] = df[stcol].astype(str).str.upper().str.strip() if stcol else ""
        return df

    def _sanitize_all(self):
        self.resp = self._sanitize_df(self.resp)
        self.rev  = self._sanitize_df(self.rev)
        self.cad  = self._sanitize_df(self.cad)
        self.ext  = self._sanitize_df(self.ext)

    def _apply_status_filters(self):
        def _filter(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty or "status_norm_any" not in df.columns:
                return df
            mask_ign = df["status_norm_any"].isin(IGNORAR_STATUS)
            return df.loc[~mask_ign].copy()

        self.resp = _filter(self.resp)
        self.cad  = _filter(self.cad)

    # -------- Bases derivadas --------
    def _build_ultima_revisao_por_placa(self) -> pd.DataFrame:
        if self.rev.empty:
            return pd.DataFrame(columns=["placa_norm", "data_ult_rev", "km_na_rev", "oficina", "custo_rev"])
        df = self.rev.copy()
        col_data = self.cols.data_rev
        col_km   = self.cols.km_rev
        col_of   = self.cols.oficina
        col_cst  = self.cols.custo_rev

        if col_data and col_data in df.columns:
            df = df.sort_values(by=[col_data], ascending=True)
        g = df.groupby("placa_norm", as_index=False).last()
        ren = {}
        if col_data: ren[col_data] = "data_ult_rev"
        if col_km:   ren[col_km]   = "km_na_rev"
        if col_of:   ren[col_of]   = "oficina"
        if col_cst:  ren[col_cst]  = "custo_rev"
        g = g.rename(columns=ren)

        if "km_na_rev" in g.columns:
            g.loc[g["km_na_rev"] == 0, "km_na_rev"] = pd.NA  # ignora zeros suspeitos

        for need in ["data_ult_rev", "km_na_rev", "oficina", "custo_rev"]:
            if need not in g.columns:
                g[need] = pd.NA
        return g[["placa_norm", "data_ult_rev", "km_na_rev", "oficina", "custo_rev"]]

    def _build_km_abastecimentos(self) -> pd.DataFrame:
        if self.ext.empty:
            return pd.DataFrame(columns=["placa_norm", "data_abast", "km_abast"])
        df = self.ext.copy()
        col_data = self.cols.data_ext
        col_km   = self.cols.km_ext
        ren = {}
        if col_data: ren[col_data] = "data_abast"
        if col_km:   ren[col_km]   = "km_abast"
        df = df.rename(columns=ren)
        if "km_abast" not in df.columns:   df["km_abast"] = pd.NA
        if "data_abast" not in df.columns: df["data_abast"] = pd.NaT
        df = df[pd.notna(df["data_abast"])]
        return df[["placa_norm", "data_abast", "km_abast"]]

    def _closest_refuel_to(self, placa_norm, data_ref):
        d = self.km_por_abastecimento
        d = d[(d["placa_norm"] == placa_norm) & pd.notna(d["data_abast"])]
        if d.empty:
            return None, None
        if data_ref is None:
            d = d.sort_values("data_abast")
            row = d.iloc[-1]
            return row["km_abast"], row["data_abast"]
        d = d.assign(delta=d["data_abast"].map(lambda x: abs((x - data_ref).days)))
        row = d.sort_values("delta").iloc[0]
        return row["km_abast"], row["data_abast"]

    def _last_refuel_km(self, placa_norm):
        d = self.km_por_abastecimento
        d = d[(d["placa_norm"] == placa_norm) & pd.notna(d["data_abast"])]
        if d.empty:
            return None, None
        d = d.sort_values("data_abast")
        row = d.iloc[-1]
        return row["km_abast"], row["data_abast"]

    def _build_previsao(self) -> pd.DataFrame:
        bases = []
        for df in [self.resp, self.rev, self.cad, self.ext]:
            if not df.empty and "placa_norm" in df.columns:
                bases.append(df[["placa_norm"]])
        if not bases:
            return pd.DataFrame(columns=[
                "placa","responsavel","unidade","regiao","bloco","igreja","marca","modelo","ano_modelo",
                "data_base","prox_data_por_tempo","dias_faltando",
                "km_base","data_km_base","km_meta","km_ultimo","data_km_ultimo","km_faltando",
                "oficina","status","renovacao_agora","renovacao_na_proxima","prox_real_em_dias"
            ])
        placas = pd.concat(bases, ignore_index=True).drop_duplicates()
        placas = placas[placas["placa_norm"] != ""]

        det = pd.DataFrame({"placa_norm": placas["placa_norm"]})
        for src in (self.resp, self.cad):
            if src.empty: continue
            g = src.groupby("placa_norm").last().reset_index()
            def _take(col): return g[col] if (col and col in g.columns) else pd.Series([pd.NA]*len(g))
            pack = pd.DataFrame({
                "placa_norm": g["placa_norm"],
                "responsavel": _take(self.cols.responsavel),
                "unidade":     _take(self.cols.unidade),
                "regiao":      _take(self.cols.regiao),
                "bloco":       _take(self.cols.bloco),
                "igreja":      _take(self.cols.igreja),
                "marca":       _take(self.cols.marca),
                "modelo":      _take(self.cols.modelo),
                "ano_modelo":  _take(self.cols.ano_modelo),
            })
            det = det.merge(pack, on="placa_norm", how="left")
            for c in ["responsavel","unidade","regiao","bloco","igreja","marca","modelo","ano_modelo"]:
                det[c] = det[c].ffill().bfill() if c in det.columns else det.get(c, pd.Series())

        ult = self.base_ult_revisao
        cad = self.cad.copy()
        if self.cols.data_inicio and self.cols.data_inicio in cad.columns:
            cad = cad.rename(columns={self.cols.data_inicio: "data_inicio"})
        else:
            cad["data_inicio"] = pd.NaT

        base = (placas
                .merge(det, on="placa_norm", how="left")
                .merge(ult, on="placa_norm", how="left")
                .merge(cad[["placa_norm","data_inicio"]], on="placa_norm", how="left"))

        rows = []
        for _, r in base.iterrows():
            placa = r["placa_norm"]
            responsavel = r.get("responsavel")
            unidade     = r.get("unidade")
            regiao      = r.get("regiao")
            bloco       = r.get("bloco")
            igreja      = r.get("igreja")
            marca       = r.get("marca")
            modelo      = r.get("modelo")
            ano_mod_raw = r.get("ano_modelo")
            try:
                ano_mod = int(str(ano_mod_raw).strip()[:4]) if pd.notna(ano_mod_raw) and str(ano_mod_raw).strip() else None
            except Exception:
                ano_mod = None

            data_ult    = r.get("data_ult_rev")
            data_ini    = r.get("data_inicio")
            oficina     = r.get("oficina")

            data_base = data_ult if pd.notna(data_ult) else (data_ini if pd.notna(data_ini) else None)
            prox_data = (data_base + timedelta(days=365)) if data_base else None
            dias_falt = (prox_data - self.hoje).days if prox_data else None

            if pd.notna(data_ult):
                km_base, data_km_base = self._closest_refuel_to(placa, data_ult)
                km_meta = (km_base + 10_000) if (km_base is not None and not pd.isna(km_base)) else None
            else:
                km_base, data_km_base = 0, None
                km_meta = 10_000

            km_ultimo, data_km_ult = self._last_refuel_km(placa)
            if (km_meta is not None) and (km_ultimo is not None) and not pd.isna(km_ultimo):
                km_falt = km_meta - km_ultimo
            elif km_meta is not None and km_ultimo is None:
                km_falt = km_meta
            else:
                km_falt = None

            status = "Desconhecido"
            if dias_falt is not None or km_falt is not None:
                vencido = (dias_falt is not None and dias_falt < 0) or (km_falt is not None and km_falt < 0)
                atencao = (dias_falt is not None and dias_falt < 30) or (km_falt is not None and km_falt < 1000)
                if vencido:   status = "Vencido"
                elif atencao: status = "Atenção"
                else:         status = "Em dia"

            renov_agora = False
            renov_prox  = False
            if ano_mod:
                renov_agora = (self.hoje.year - ano_mod) >= 3
                if prox_data:
                    renov_prox = (prox_data.year - ano_mod) >= 3

            # “próxima real” (mínimo entre tempo e km em dias, usando 50 km/dia como heurística)
            prox_por_km_em_dias = int(km_falt/50) if (km_falt is not None) else None
            if dias_falt is not None and prox_por_km_em_dias is not None:
                prox_real_em_dias = min(dias_falt, prox_por_km_em_dias)
            else:
                prox_real_em_dias = dias_falt if dias_falt is not None else prox_por_km_em_dias

            rows.append({
                "placa": placa,
                "responsavel": responsavel,
                "unidade": unidade,
                "regiao": regiao,
                "bloco": bloco,
                "igreja": igreja,
                "marca": marca,
                "modelo": modelo,
                "ano_modelo": ano_mod,
                "data_base": data_base,
                "prox_data_por_tempo": prox_data,
                "dias_faltando": dias_falt,
                "km_base": km_base,
                "data_km_base": data_km_base,
                "km_meta": km_meta,
                "km_ultimo": km_ultimo,
                "data_km_ultimo": data_km_ult,
                "km_faltando": km_falt,
                "oficina": oficina,
                "status": status,
                "renovacao_agora": renov_agora,
                "renovacao_na_proxima": renov_prox,
                "prox_real_em_dias": prox_real_em_dias,
            })

        out = pd.DataFrame(rows)

        def _ord(row):
            a = row["dias_faltando"] if row["dias_faltando"] is not None else 9e9
            b = row["km_faltando"]   if row["km_faltando"]   is not None else 9e9
            return min(a, b)
        if len(out):
            out["ord"] = out.apply(_ord, axis=1)
            out = out.sort_values("ord").drop(columns=["ord"])
        return out

    # --------- Agregações ---------
    def agg_calendario(self) -> pd.DataFrame:
        df = self.previsao.copy()
        if df.empty or "prox_data_por_tempo" not in df.columns:
            return pd.DataFrame(columns=["ano_mes","qtd"])
        df = df[pd.notna(df["prox_data_por_tempo"])]
        if df.empty:
            return pd.DataFrame(columns=["ano_mes","qtd"])
        df["ano_mes"] = df["prox_data_por_tempo"].map(lambda d: f"{d.year}-{d.month:02d}")
        g = df.groupby("ano_mes").size().reset_index(name="qtd").sort_values("ano_mes")
        return g

    def agg_por_responsavel(self) -> pd.DataFrame:
        df = self.previsao.copy()
        if df.empty:
            return pd.DataFrame(columns=["responsavel","total","vencido","atencao","em_dia"])
        def cat(x): return x if isinstance(x, str) and x.strip() else "(Sem responsável)"
        df["responsavel"] = df["responsavel"].map(cat)
        g = df.groupby("responsavel").agg(
            total=("placa","count"),
            vencido=("status", lambda s: (s=="Vencido").sum()),
            atencao=("status", lambda s: (s=="Atenção").sum()),
            em_dia=("status", lambda s: (s=="Em dia").sum()),
        ).reset_index().sort_values(["vencido","atencao","total"], ascending=[False, False, False])
        return g

    def agg_por_unidade(self) -> pd.DataFrame:
        df = self.previsao.copy()
        if df.empty:
            return pd.DataFrame(columns=["unidade","total","vencido","atencao","em_dia"])
        def cat(x): return x if isinstance(x, str) and x.strip() else "(Sem unidade)"
        df["unidade"] = df["unidade"].map(cat)
        g = df.groupby("unidade").agg(
            total=("placa","count"),
            vencido=("status", lambda s: (s=="Vencido").sum()),
            atencao=("status", lambda s: (s=="Atenção").sum()),
            em_dia=("status", lambda s: (s=="Em dia").sum()),
        ).reset_index().sort_values(["vencido","atencao","total"], ascending=[False, False, False])
        return g

    def agg_por_oficina(self) -> pd.DataFrame:
        df = self.previsao.copy()
        if df.empty or "oficina" not in df.columns:
            return pd.DataFrame(columns=["oficina","qtd"])
        def cat(x): return x if isinstance(x, str) and x.strip() else "(Sem oficina)"
        df["oficina"] = df["oficina"].map(cat)
        g = df.groupby("oficina").size().reset_index(name="qtd").sort_values("qtd", ascending=False)
        return g

    def agg_por_regiao(self) -> pd.DataFrame:
        df = self.previsao.copy()
        if df.empty:
            return pd.DataFrame(columns=["regiao","total","vencido","atencao","em_dia"])
        def cat(x): return x if isinstance(x, str) and x.strip() else "(Sem região)"
        df["regiao"] = df["regiao"].map(cat)
        g = df.groupby("regiao").agg(
            total=("placa","count"),
            vencido=("status", lambda s: (s=="Vencido").sum()),
            atencao=("status", lambda s: (s=="Atenção").sum()),
            em_dia=("status", lambda s: (s=="Em dia").sum()),
        ).reset_index().sort_values(["vencido","atencao","total"], ascending=[False, False, False])
        return g

    def agg_por_ano_modelo(self) -> pd.DataFrame:
        df = self.previsao.copy()
        if df.empty:
            return pd.DataFrame(columns=["ano_modelo","qtd","renovacao_agora","renovacao_na_proxima"])
        g = df.groupby("ano_modelo").agg(
            qtd=("placa","count"),
            renovacao_agora=("renovacao_agora", "sum"),
            renovacao_na_proxima=("renovacao_na_proxima", "sum"),
        ).reset_index().sort_values("ano_modelo", ascending=True)
        return g

    def view_alertas(self) -> pd.DataFrame:
        df = self.previsao.copy()
        if df.empty:
            return df
        mask = (df["status"].isin(["Vencido","Atenção"]))
        cols = [
            "placa","responsavel","unidade","regiao","dias_faltando","km_faltando",
            "prox_data_por_tempo","status","renovacao_agora","renovacao_na_proxima","prox_real_em_dias"
        ]
        cols = [c for c in cols if c in df.columns]
        return df.loc[mask, cols].sort_values(
            ["status","dias_faltando","km_faltando"], ascending=[True, True, True]
        )

    def view_sem_historico(self) -> pd.DataFrame:
        df = self.previsao.copy()
        if df.empty:
            return df
        mask = df["data_base"].isna() | df["km_ultimo"].isna()
        cols = ["placa","responsavel","unidade","data_base","km_ultimo","status"]
        return df.loc[mask, cols].sort_values("placa")

    def projecao_orcamento(self) -> pd.DataFrame:
        df = self.previsao.copy()
        df_rev = self.base_ult_revisao.copy()
        if df.empty:
            return pd.DataFrame(columns=["ano_mes","custo_previsto"])
        custo_medio = None
        if not df_rev.empty and "custo_rev" in df_rev.columns:
            v = pd.to_numeric(df_rev["custo_rev"], errors="coerce")
            if v.notna().any():
                custo_medio = v.mean()
        if not custo_medio or math.isnan(custo_medio):
            custo_medio = 500.0
        df = df[pd.notna(df["prox_data_por_tempo"])]
        if df.empty:
            return pd.DataFrame(columns=["ano_mes","custo_previsto"])
        df["ano_mes"] = df["prox_data_por_tempo"].map(lambda d: f"{d.year}-{d.month:02d}")
        g = df.groupby("ano_mes").size().reset_index(name="qtd")
        g["custo_previsto"] = g["qtd"] * custo_medio
        return g[["ano_mes","custo_previsto"]].sort_values("ano_mes")

    def view_anomalias(self) -> pd.DataFrame:
        df = self.previsao.copy()
        if df.empty:
            return pd.DataFrame(columns=["placa","problema","obs"])
        rows = []
        for _, r in df.iterrows():
            placa = r["placa"]
            d_ult = r.get("data_km_ultimo")
            d_base = r.get("data_km_base")
            km_ult = r.get("km_ultimo")
            km_base = r.get("km_base")
            if pd.notna(d_ult) and pd.notna(d_base) and d_ult < d_base:
                rows.append({"placa": placa, "problema": "Ordem incoerente",
                             "obs": f"Último abastecimento ({d_ult}) < data base ({d_base})"})
            if (km_ult is not None) and (km_base is not None) and not pd.isna(km_ult) and not pd.isna(km_base):
                if km_ult < km_base:
                    rows.append({"placa": placa, "problema": "KM regrediu",
                                 "obs": f"km_ultimo ({km_ult}) < km_base ({km_base})"})
        return pd.DataFrame(rows)

# ============================== UI – Janela e Abas ==============================

class RevisaoWindow(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Revisão – Previsão & Controle")
        self.resize(1280, 720)

        # caminhos atuais (carrega cfg ou vazio)
        self.paths = self._load_paths()
        self.core = RevisaoCore(
            self.paths.get("arq_resp"),
            self.paths.get("arq_rev"),
            self.paths.get("arq_cad"),
            self.paths.get("arq_ext"),
        )

        self._build_ui()
        self._popular_filtros()
        self._refresh_all_tables()

    # ----- paths cfg -----
    def _load_paths(self) -> dict:
        if CFG_PATH.exists():
            try:
                return json.loads(CFG_PATH.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {"arq_resp": "", "arq_rev": "", "arq_cad": "", "arq_ext": ""}

    def _save_paths(self):
        CFG_PATH.write_text(json.dumps(self.paths, ensure_ascii=False, indent=2), encoding="utf-8")

    # ----- UI helpers -----
    def _build_ui(self):
        root = QVBoxLayout(self)

        # ========== faixa de seleção dos arquivos ==========
        files_box = QHBoxLayout()
        def add_picker(label: str, key: str):
            nonlocal files_box
            box = QHBoxLayout()
            lab = QLabel(label); lab.setMinimumWidth(110)
            ed  = QLineEdit(self.paths.get(key, "")); ed.setPlaceholderText("Escolha o arquivo…"); ed.setMinimumWidth(280)
            btn = QPushButton("…")
            def pick():
                path, _ = QFileDialog.getOpenFileName(self, f"Selecionar {label}", str(APP_DIR), "Planilhas (*.xlsx)")
                if path:
                    ed.setText(path)
                    self.paths[key] = path
                    self._save_paths()
            btn.clicked.connect(pick)
            setattr(self, f"ed_{key}", ed)
            box.addWidget(lab); box.addWidget(ed); box.addWidget(btn)
            files_box.addLayout(box)

        add_picker("Responsável:", "arq_resp")
        add_picker("Revisão:",     "arq_rev")
        add_picker("Chassi/Ren:",  "arq_cad")
        add_picker("Extrato:",     "arq_ext")

        btn_reload = QPushButton("Recarregar dados")
        btn_reload.clicked.connect(self._reload_core)
        files_box.addWidget(btn_reload)

        root.addLayout(files_box)

        # separador
        line = QFrame(); line.setFrameShape(QFrame.Shape.HLine); root.addWidget(line)

        # ====== Filtros topo ======
        top = QHBoxLayout()
        self.cb_unidade = QComboBox(); self.cb_unidade.setEditable(False); self.cb_unidade.addItem("Todas as unidades")
        self.cb_responsavel = QComboBox(); self.cb_responsavel.addItem("Todos os responsáveis")
        self.cb_regiao = QComboBox(); self.cb_regiao.addItem("Todas as regiões")
        self.ed_busca_placa = QLineEdit(); self.ed_busca_placa.setPlaceholderText("Buscar placa…")

        self.btn_aplicar = QPushButton("Aplicar filtros")
        self.btn_limpar = QPushButton("Limpar filtros")

        for w in [self.cb_unidade, self.cb_responsavel, self.cb_regiao, self.ed_busca_placa, self.btn_aplicar, self.btn_limpar]:
            top.addWidget(w)
        root.addLayout(top)

        self.btn_aplicar.clicked.connect(self._refresh_all_tables)
        self.btn_limpar.clicked.connect(self._limpar_filtros)

        # ====== Tabs ======
        self.tabs = QTabWidget(); root.addWidget(self.tabs)

        self.tab_geral = QWidget(); self.tabs.addTab(self.tab_geral, "Geral"); self._build_tab_geral()
        self.tab_cal = QWidget(); self.tabs.addTab(self.tab_cal, "Calendário"); self._build_tab_calendario()
        self.tab_resp = QWidget(); self.tabs.addTab(self.tab_resp, "Por Responsável"); self._build_tab_por_responsavel()
        self.tab_of = QWidget(); self.tabs.addTab(self.tab_of, "Por Oficina"); self._build_tab_por_oficina()
        self.tab_reg = QWidget(); self.tabs.addTab(self.tab_reg, "Por Região"); self._build_tab_por_regiao()
        self.tab_ano = QWidget(); self.tabs.addTab(self.tab_ano, "Ano Modelo / Renovação"); self._build_tab_ano_modelo()
        self.tab_alerta = QWidget(); self.tabs.addTab(self.tab_alerta, "Alertas"); self._build_tab_alertas()
        self.tab_sem = QWidget(); self.tabs.addTab(self.tab_sem, "Sem histórico"); self._build_tab_sem_historico()
        self.tab_proj = QWidget(); self.tabs.addTab(self.tab_proj, "Projeção & Orçamento"); self._build_tab_projecao()
        self.tab_anom = QWidget(); self.tabs.addTab(self.tab_anom, "Anomalias"); self._build_tab_anomalias()

    def _reload_core(self):
        # pega textos dos edits, salva cfg e recarrega core
        self.paths["arq_resp"] = self.ed_arq_resp.text().strip()
        self.paths["arq_rev"]  = self.ed_arq_rev.text().strip()
        self.paths["arq_cad"]  = self.ed_arq_cad.text().strip()
        self.paths["arq_ext"]  = self.ed_arq_ext.text().strip()
        self._save_paths()
        try:
            self.core = RevisaoCore(
                self.paths.get("arq_resp") or None,
                self.paths.get("arq_rev")  or None,
                self.paths.get("arq_cad")  or None,
                self.paths.get("arq_ext")  or None,
            )
            self._popular_filtros()
            self._refresh_all_tables()
            QMessageBox.information(self, "Recarregar", "Dados recarregados com sucesso.")
        except Exception as e:
            QMessageBox.critical(self, "Recarregar", f"Falha ao recarregar:\n{e}")

    def _popular_filtros(self):
        # limpa e repopula combos
        for cb, first in [(self.cb_unidade, "Todas as unidades"),
                          (self.cb_responsavel, "Todos os responsáveis"),
                          (self.cb_regiao, "Todas as regiões")]:
            current = cb.currentText() if cb.count() else first
            cb.clear(); cb.addItem(first)
            if self.core.previsao.empty: 
                continue
            col = {"Todas as unidades":"unidade","Todos os responsáveis":"responsavel","Todas as regiões":"regiao"}[first]
            vals = sorted(set([v for v in self.core.previsao[col].dropna().astype(str).tolist() if v.strip()])) if col in self.core.previsao.columns else []
            cb.addItems(vals)
            # tenta restaurar seleção anterior
            if current in vals:
                cb.setCurrentText(current)

    def _aplicar_filtros_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        placa_q = _norm_placa(self.ed_busca_placa.text())
        u_sel = self.cb_unidade.currentText()
        r_sel = self.cb_responsavel.currentText()
        g_sel = self.cb_regiao.currentText()
        out = df.copy()
        if u_sel and u_sel != "Todas as unidades" and "unidade" in out.columns:
            out = out[out["unidade"].astype(str) == u_sel]
        if r_sel and r_sel != "Todos os responsáveis" and "responsavel" in out.columns:
            out = out[out["responsavel"].astype(str) == r_sel]
        if g_sel and g_sel != "Todas as regiões" and "regiao" in out.columns:
            out = out[out["regiao"].astype(str) == g_sel]
        if placa_q:
            if "placa" in out.columns:
                out = out[out["placa"].astype(str).map(_norm_placa).str.contains(placa_q)]
            elif "placa_norm" in out.columns:
                out = out[out["placa_norm"].astype(str).str.contains(placa_q)]
        return out

    def _create_table(self, parent: QWidget, cols: List[str]) -> QTableWidget:
        tbl = QTableWidget(parent)
        tbl.setColumnCount(len(cols))
        tbl.setHorizontalHeaderLabels([c.replace("_"," ").title() for c in cols])
        tbl.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        tbl.setSortingEnabled(True)
        return tbl

    def _fill_table(self, tbl: QTableWidget, df: pd.DataFrame, cols: List[str], colorize_status: bool = False):
        df = df.copy() if df is not None else pd.DataFrame(columns=cols)
        df = df[cols] if (not df.empty and all(c in df.columns for c in cols)) else df
        tbl.setRowCount(len(df))
        for i, row in df.iterrows():
            for j, c in enumerate(cols):
                val = row.get(c)

                # formatação simpática
                if isinstance(val, (datetime, date)):
                    s = _fmt_date(val if isinstance(val, date) else val.date())
                elif c in {"data_base","prox_data_por_tempo","data_km_base","data_km_ultimo"}:
                    s = _fmt_date(val)
                elif c.lower().startswith(("km", "dias")) or c in {"prox_real_em_dias"}:
                    s = _fmt_num(val)
                elif c in {"renovacao_agora","renovacao_na_proxima"}:
                    s = _bool_tag(val)
                else:
                    s = "" if pd.isna(val) or val is None else str(val)

                it = QTableWidgetItem(s)
                # alinhamento numérico
                if c.lower().startswith(("km", "dias")) or c in {"prox_real_em_dias"}:
                    it.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                tbl.setItem(i, j, it)

        # coloração por status
        if colorize_status and df is not None and len(df):
            if "status" in df.columns and "status" in cols:
                st_idx = cols.index("status")
                for i in range(tbl.rowCount()):
                    st_item = tbl.item(i, st_idx)
                    st = st_item.text() if st_item else ""
                    for j in range(tbl.columnCount()):
                        it = tbl.item(i, j)
                        if not it:
                            continue
                        if st == "Vencido":
                            it.setBackground(Qt.GlobalColor.red)
                            it.setForeground(Qt.GlobalColor.white)
                        elif st == "Atenção":
                            it.setBackground(Qt.GlobalColor.yellow)
                        # Em dia fica normal
        tbl.resizeColumnsToContents()

    # ---- Tabs ----
    def _build_tab_geral(self):
        lay = QVBoxLayout(self.tab_geral)
        self.lbl_kpis = QLabel(); lay.addWidget(self.lbl_kpis)
        self.cols_geral = [
            "placa","responsavel","unidade","regiao","dias_faltando","km_faltando",
            "prox_data_por_tempo","km_meta","km_ultimo","status","renovacao_agora","renovacao_na_proxima","prox_real_em_dias"
        ]
        self.tbl_geral = self._create_table(self.tab_geral, self.cols_geral)
        lay.addWidget(self.tbl_geral)

        btns = QHBoxLayout()
        self.btn_export_xlsx = QPushButton("Exportar XLSX")
        self.btn_export_csv  = QPushButton("Exportar CSV")
        self.btn_copiar      = QPushButton("Copiar linhas")
        btns.addWidget(self.btn_export_xlsx); btns.addWidget(self.btn_export_csv); btns.addWidget(self.btn_copiar); btns.addStretch(1)
        lay.addLayout(btns)

        # exportam o QUE ESTÁ FILTRADO
        self.btn_export_xlsx.clicked.connect(lambda: self._exportar(self._aplicar_filtros_df(self.core.previsao), "PrevisaoRevisao.xlsx"))
        self.btn_export_csv.clicked.connect(lambda: self._exportar(self._aplicar_filtros_df(self.core.previsao), "PrevisaoRevisao.csv"))
        self.btn_copiar.clicked.connect(lambda: self._copiar_para_clipboard(self._aplicar_filtros_df(self.core.previsao)))

    def _build_tab_calendario(self):
        lay = QVBoxLayout(self.tab_cal)
        self.tbl_cal = self._create_table(self.tab_cal, ["ano_mes","qtd"])
        lay.addWidget(self.tbl_cal)

    def _build_tab_por_responsavel(self):
        lay = QVBoxLayout(self.tab_resp)
        self.tbl_resp = self._create_table(self.tab_resp, ["responsavel","total","vencido","atencao","em_dia"])
        lay.addWidget(self.tbl_resp)
        self.tbl_unid = self._create_table(self.tab_resp, ["unidade","total","vencido","atencao","em_dia"])
        lay.addWidget(QLabel("<b>Por Unidade</b>")); lay.addWidget(self.tbl_unid)

    def _build_tab_por_oficina(self):
        lay = QVBoxLayout(self.tab_of)
        self.tbl_of = self._create_table(self.tab_of, ["oficina","qtd"])
        lay.addWidget(self.tbl_of)

    def _build_tab_por_regiao(self):
        lay = QVBoxLayout(self.tab_reg)
        self.tbl_reg = self._create_table(self.tab_reg, ["regiao","total","vencido","atencao","em_dia"])
        lay.addWidget(self.tbl_reg)

    def _build_tab_ano_modelo(self):
        lay = QVBoxLayout(self.tab_ano)
        self.tbl_ano = self._create_table(self.tab_ano, ["ano_modelo","qtd","renovacao_agora","renovacao_na_proxima"])
        lay.addWidget(self.tbl_ano)

    def _build_tab_alertas(self):
        lay = QVBoxLayout(self.tab_alerta)
        cols = ["placa","responsavel","unidade","regiao","dias_faltando","km_faltando","prox_data_por_tempo","status","renovacao_agora","renovacao_na_proxima","prox_real_em_dias"]
        self.tbl_alerta = self._create_table(self.tab_alerta, cols)
        lay.addWidget(self.tbl_alerta)

    def _build_tab_sem_historico(self):
        lay = QVBoxLayout(self.tab_sem)
        cols = ["placa","responsavel","unidade","data_base","km_ultimo","status"]
        self.tbl_sem = self._create_table(self.tab_sem, cols)
        lay.addWidget(self.tbl_sem)

    def _build_tab_projecao(self):
        lay = QVBoxLayout(self.tab_proj)
        self.tbl_proj = self._create_table(self.tab_proj, ["ano_mes","custo_previsto"])
        lay.addWidget(self.tbl_proj)
        hint = QLabel("<i>Obs.: custo médio usa histórico (custo_rev) se existir; senão, R$ 500 como referência.</i>")
        lay.addWidget(hint)

    def _build_tab_anomalias(self):
        lay = QVBoxLayout(self.tab_anom)
        cols = ["placa","problema","obs"]
        self.tbl_anom = self._create_table(self.tab_anom, cols)
        lay.addWidget(self.tbl_anom)

    # ----- Refresh -----
    def _refresh_all_tables(self):
        df_main = self._aplicar_filtros_df(self.core.previsao)

        total = len(df_main)
        venc  = (df_main["status"] == "Vencido").sum() if total else 0
        atn   = (df_main["status"] == "Atenção").sum() if total else 0
        emdia = (df_main["status"] == "Em dia").sum() if total else 0
        self.lbl_kpis.setText(f"<b>Total:</b> {total} | <b>Vencidos:</b> {venc} | <b>Atenção:</b> {atn} | <b>Em dia:</b> {emdia}")

        self._fill_table(self.tbl_geral, df_main, self.cols_geral, colorize_status=True)

        df_cal = self.core.agg_calendario(); self._fill_table(self.tbl_cal, df_cal, ["ano_mes","qtd"])
        df_resp = self.core.agg_por_responsavel(); self._fill_table(self.tbl_resp, df_resp, ["responsavel","total","vencido","atencao","em_dia"])
        df_unid = self.core.agg_por_unidade(); self._fill_table(self.tbl_unid, df_unid, ["unidade","total","vencido","atencao","em_dia"])
        df_of = self.core.agg_por_oficina(); self._fill_table(self.tbl_of, df_of, ["oficina","qtd"])
        df_reg = self.core.agg_por_regiao(); self._fill_table(self.tbl_reg, df_reg, ["regiao","total","vencido","atencao","em_dia"])
        df_ano = self.core.agg_por_ano_modelo(); self._fill_table(self.tbl_ano, df_ano, ["ano_modelo","qtd","renovacao_agora","renovacao_na_proxima"])
        df_al = self.core.view_alertas(); df_al = self._aplicar_filtros_df(df_al)
        self._fill_table(self.tbl_alerta, df_al, ["placa","responsavel","unidade","regiao","dias_faltando","km_faltando","prox_data_por_tempo","status","renovacao_agora","renovacao_na_proxima","prox_real_em_dias"])
        df_sh = self.core.view_sem_historico(); df_sh = self._aplicar_filtros_df(df_sh)
        self._fill_table(self.tbl_sem, df_sh, ["placa","responsavel","unidade","data_base","km_ultimo","status"])
        df_pj = self.core.projecao_orcamento(); self._fill_table(self.tbl_proj, df_pj, ["ano_mes","custo_previsto"])
        df_an = self.core.view_anomalias(); self._fill_table(self.tbl_anom, df_an, ["placa","problema","obs"])

    # ----- Export / Clipboard -----
    def _exportar(self, df: pd.DataFrame, suggested_name: str):
        if df is None or df.empty:
            QMessageBox.warning(self, "Exportar", "Não há dados para exportar.")
            return
        dlg = QFileDialog(self, "Salvar arquivo", str(APP_DIR / suggested_name))
        if suggested_name.lower().endswith(".xlsx"):
            dlg.setDefaultSuffix("xlsx"); dlg.setNameFilters(["Planilha Excel (*.xlsx)", "CSV (*.csv)"])
        else:
            dlg.setDefaultSuffix("csv"); dlg.setNameFilters(["CSV (*.csv)", "Planilha Excel (*.xlsx)"])
        if dlg.exec():
            path = dlg.selectedFiles()[0]
            try:
                if path.lower().endswith(".xlsx"):
                    df.to_excel(path, index=False)
                else:
                    df.to_csv(path, index=False, sep=";")
                QMessageBox.information(self, "Exportar", f"Arquivo salvo em:\n{path}")
            except Exception as e:
                QMessageBox.critical(self, "Exportar", f"Erro ao salvar:\n{e}")

    def _copiar_para_clipboard(self, df: pd.DataFrame):
        if df is None or df.empty:
            return
        cols = [
            "placa","responsavel","unidade","dias_faltando","km_faltando",
            "prox_data_por_tempo","km_meta","km_ultimo","status","renovacao_agora","renovacao_na_proxima","prox_real_em_dias"
        ]
        sub = df[cols] if all(c in df.columns for c in cols) else df.copy()
        txt = sub.to_csv(index=False, sep="\t")
        QApplication.clipboard().setText(txt)
        QMessageBox.information(self, "Copiado", "Linhas copiadas para a área de transferência.")

    def _limpar_filtros(self):
        """Reseta os filtros do topo e recarrega as tabelas."""
        try:
            self.cb_unidade.setCurrentIndex(0)
        except Exception:
            pass
        try:
            self.cb_responsavel.setCurrentIndex(0)
        except Exception:
            pass
        try:
            self.cb_regiao.setCurrentIndex(0)
        except Exception:
            pass
        if hasattr(self, "ed_busca_placa"):
            self.ed_busca_placa.clear()
        self._refresh_all_tables()
