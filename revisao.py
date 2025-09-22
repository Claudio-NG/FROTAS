from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, QPushButton, QTabWidget,
    QTableWidget, QTableWidgetItem, QComboBox, QFileDialog, QMessageBox, QSplitter, QSizePolicy
)
BASE = Path("/mnt/data")

ARQ_RESP = BASE / "Responsavel.xlsx"
ARQ_REV  = BASE / "REVISÃO.xlsx"
ARQ_CAD  = BASE / "Chassi e Renavam.xlsx"
ARQ_EXT  = BASE / "ExtratoGeral.xlsx"


def _norm_placa(s: str) -> str:
    if not isinstance(s, str): return ""
    return s.upper().replace("-", "").replace(" ", "").strip()

def _to_date(x):
    if pd.isna(x): return None
    if isinstance(x, (datetime, date)):
        # normaliza para date
        return x.date() if isinstance(x, datetime) else x
    try:
        d = pd.to_datetime(x, dayfirst=True, errors="coerce")
        return None if pd.isna(d) else d.date()
    except Exception:
        return None

def _to_num(x):
    try:
        if pd.isna(x): return math.nan
        if isinstance(x, str):
            s = x.replace(".", "").replace(" ", "").replace("R$", "").replace(",", ".")
            return float(s)
        return float(x)
    except Exception:
        return math.nan

def _first(series: pd.Series, *cands) -> Optional[str]:
    for c in cands:
        if c in series.index:
            return c
    return None

def _find_col(cols: List[str], *hints: str) -> Optional[str]:
    """Encontra a primeira coluna cujo nome contenha TODOS os termos de um hint (casefold)."""
    L = [c for c in cols]
    low = [c.lower() for c in cols]
    for hint in hints:
        parts = [p.strip() for p in hint.lower().split() if p.strip()]
        for i, lc in enumerate(low):
            if all(p in lc for p in parts):
                return L[i]
    return None

def _safe_div(a, b, default=0):
    try:
        return a / b if b not in (0, None, math.nan) else default
    except Exception:
        return default


@dataclass
class ColunasMap:
    placa: str = "Placa"
    responsavel: Optional[str] = None
    unidade: Optional[str] = None
    data_rev: Optional[str] = None
    km_rev: Optional[str] = None
    data_inicio: Optional[str] = None
    data_ext: Optional[str] = None
    km_ext: Optional[str] = None
    oficina: Optional[str] = None
    custo_rev: Optional[str] = None

class RevisaoCore:
    """Carrega, normaliza e calcula previsões de revisão por DATA e KM."""
    def __init__(self, base: Path = BASE):
        self.base_path = base
        self.hoje: date = datetime.now().date()
        # DataFrames
        self.resp = self._load(ARQ_RESP)
        self.rev  = self._load(ARQ_REV)
        self.cad  = self._load(ARQ_CAD)
        self.ext  = self._load(ARQ_EXT)

        # Mapeamento de colunas
        self.cols = self._inferir_colunas()

        # Sanitização básica
        self._sanitize_all()

        # Estruturas derivadas
        self.base_ult_revisao = self._build_ultima_revisao_por_placa()
        self.km_por_abastecimento = self._build_km_abastecimentos()

        # Resultado principal
        self.previsao = self._build_previsao()  # DataFrame com linhas por placa

    def _load(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        try:
            return pd.read_excel(path)
        except Exception:
            # fallback: algumas instalações precisam engine
            return pd.read_excel(path, engine="openpyxl")

    def _inferir_colunas(self) -> ColunasMap:
        """Heurística: tenta descobrir nomes em cada DF."""
        cmap = ColunasMap()

        # Responsável
        if not self.resp.empty:
            cols = list(self.resp.columns)
            cmap.placa = _find_col(cols, "placa") or cmap.placa
            cmap.responsavel = _find_col(cols, "respons")  # Responsável
            cmap.unidade = _find_col(cols, "setor", "unidade", "lotação", "depart")

        # Revisão
        if not self.rev.empty:
            cols = list(self.rev.columns)
            cmap.placa = _find_col(cols, "placa") or cmap.placa
            cmap.data_rev = _find_col(cols, "data", "última revisão") or _find_col(cols, "data revisão") or _find_col(cols, "data")
            cmap.km_rev = _find_col(cols, "km") or _find_col(cols, "quilometr")
            cmap.oficina = _find_col(cols, "oficina")  # se existir
            cmap.custo_rev = _find_col(cols, "custo", "valor")  # se existir

        # Cadastro
        if not self.cad.empty:
            cols = list(self.cad.columns)
            cmap.placa = _find_col(cols, "placa") or cmap.placa
            cmap.data_inicio = _find_col(cols, "data início") or _find_col(cols, "data entrada") or _find_col(cols, "início") or _find_col(cols, "inicio")

        # Extrato
        if not self.ext.empty:
            cols = list(self.ext.columns)
            cmap.placa = _find_col(cols, "placa") or cmap.placa
            cmap.data_ext = _find_col(cols, "data")
            cmap.km_ext = _find_col(cols, "km") or _find_col(cols, "quilometr")

        return cmap

    def _sanitize_df(self, df: pd.DataFrame, cols_map: ColunasMap, source: str):
        if df.empty:
            return df
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        # Placa
        if cols_map.placa in df.columns:
            df["placa_norm"] = df[cols_map.placa].map(_norm_placa)
        else:
            poss = [c for c in df.columns if c.lower().startswith("placa")]
            df["placa_norm"] = df[poss[0]].map(_norm_placa) if poss else ""
        # Datas
        for cname in df.columns:
            if "data" in cname.lower():
                df[cname] = df[cname].map(_to_date)
        # KM
        for cname in df.columns:
            if "km" in cname.lower():
                df[cname] = pd.to_numeric(df[cname], errors="coerce")
        # Valores/Custos
        for cname in df.columns:
            if "valor" in cname.lower() or "custo" in cname.lower():
                df[cname] = df[cname].map(_to_num)
        return df

    def _sanitize_all(self):
        self.resp = self._sanitize_df(self.resp, self.cols, "Responsavel")
        self.rev  = self._sanitize_df(self.rev,  self.cols, "REVISAO")
        self.cad  = self._sanitize_df(self.cad,  self.cols, "Cadastro")
        self.ext  = self._sanitize_df(self.ext,  self.cols, "Extrato")

    # -------- bases derivadas --------

    def _build_ultima_revisao_por_placa(self) -> pd.DataFrame:
        if self.rev.empty:
            return pd.DataFrame(columns=["placa_norm", "data_ult_rev", "km_na_rev", "oficina", "custo_rev"])
        df = self.rev.copy()
        col_data = self.cols.data_rev
        col_km   = self.cols.km_rev
        col_of   = self.cols.oficina
        col_cst  = self.cols.custo_rev

        # ordena por data crescente e pega a última por placa
        if col_data and col_data in df.columns:
            df = df.sort_values(by=[col_data], ascending=True)
        g = df.groupby("placa_norm", as_index=False).last()
        rename = {}
        if col_data: rename[col_data] = "data_ult_rev"
        if col_km:   rename[col_km]   = "km_na_rev"
        if col_of:   rename[col_of]   = "oficina"
        if col_cst:  rename[col_cst]  = "custo_rev"
        g = g.rename(columns=rename)
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
        rename = {}
        if col_data: rename[col_data] = "data_abast"
        if col_km:   rename[col_km]   = "km_abast"
        df = df.rename(columns=rename)
        if "km_abast" not in df.columns:
            df["km_abast"] = pd.NA
        if "data_abast" not in df.columns:
            df["data_abast"] = pd.NaT
        # filtra somente linhas com alguma data
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
        # Universo de placas
        bases = []
        for df in [self.resp, self.rev, self.cad, self.ext]:
            if not df.empty and "placa_norm" in df.columns:
                bases.append(df[["placa_norm"]])
        if not bases:
            return pd.DataFrame(columns=[
                "placa","responsavel","unidade","data_base","prox_data_por_tempo",
                "dias_faltando","km_base","data_km_base","km_meta","km_ultimo",
                "data_km_ultimo","km_faltando","oficina","status"
            ])
        placas = pd.concat(bases, ignore_index=True).drop_duplicates()
        placas = placas[placas["placa_norm"] != ""]

        # Map de resp/unidade
        resp_map = pd.DataFrame(columns=["placa_norm","responsavel","unidade"])
        if not self.resp.empty:
            g = self.resp.groupby("placa_norm").last().reset_index()
            rcol = self.cols.responsavel if (self.cols.responsavel in g.columns) else None
            ucol = self.cols.unidade     if (self.cols.unidade     in g.columns) else None
            resp_map["placa_norm"] = g["placa_norm"]
            resp_map["responsavel"] = g[rcol] if rcol else pd.Series([pd.NA]*len(g))
            resp_map["unidade"]     = g[ucol] if ucol else pd.Series([pd.NA]*len(g))

        # Última revisão
        ult = self.base_ult_revisao

        # Data de início (cadastro)
        cad = self.cad.copy()
        if self.cols.data_inicio and self.cols.data_inicio in cad.columns:
            cad = cad.rename(columns={self.cols.data_inicio: "data_inicio"})
        else:
            cad["data_inicio"] = pd.NaT

        base = placas.merge(resp_map, on="placa_norm", how="left") \
                     .merge(ult, on="placa_norm", how="left") \
                     .merge(cad[["placa_norm","data_inicio"]], on="placa_norm", how="left")

        rows = []
        for _, r in base.iterrows():
            placa = r["placa_norm"]
            responsavel = r.get("responsavel", None)
            unidade     = r.get("unidade", None)
            data_ult    = r.get("data_ult_rev", None)
            data_ini    = r.get("data_inicio", None)
            oficina     = r.get("oficina", None)

            # Critério DATA
            data_base = data_ult if pd.notna(data_ult) else (data_ini if pd.notna(data_ini) else None)
            prox_data = (data_base + timedelta(days=365)) if data_base else None
            dias_falt = (prox_data - self.hoje).days if prox_data else None

            # Critério KM
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

            # Status
            status = "Desconhecido"
            if dias_falt is not None or km_falt is not None:
                vencido = (dias_falt is not None and dias_falt < 0) or (km_falt is not None and km_falt < 0)
                atencao = (dias_falt is not None and dias_falt < 30) or (km_falt is not None and km_falt < 1000)
                if vencido:   status = "Vencido"
                elif atencao: status = "Atenção"
                else:         status = "Em dia"

            rows.append({
                "placa": placa,
                "responsavel": responsavel,
                "unidade": unidade,
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
            })

        out = pd.DataFrame(rows)
        # Ordenação por criticidade
        def _ord(row):
            a = row["dias_faltando"] if row["dias_faltando"] is not None else 9e9
            b = row["km_faltando"] if row["km_faltando"] is not None else 9e9
            return min(a, b)
        if len(out):
            out["ord"] = out.apply(_ord, axis=1)
            out = out.sort_values("ord").drop(columns=["ord"])
        return out

    # --------- agregações para abas ---------

    def agg_calendario(self) -> pd.DataFrame:
        """Quantidade de revisões por Ano-Mês (via prox_data_por_tempo)."""
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

    def view_alertas(self) -> pd.DataFrame:
        df = self.previsao.copy()
        if df.empty: return df
        mask = (df["status"].isin(["Vencido","Atenção"]))
        cols = ["placa","responsavel","unidade","dias_faltando","km_faltando","prox_data_por_tempo","status"]
        return df.loc[mask, cols].sort_values(["status","dias_faltando","km_faltando"], ascending=[True, True, True])

    def view_sem_historico(self) -> pd.DataFrame:
        df = self.previsao.copy()
        if df.empty: return df
        mask = df["data_base"].isna() | df["km_ultimo"].isna()
        cols = ["placa","responsavel","unidade","data_base","km_ultimo","status"]
        return df.loc[mask, cols].sort_values("placa")

    def projecao_orcamento(self) -> pd.DataFrame:
        """Estimativa simples de custo por mês se existirem custos médios (custo_rev)."""
        df = self.previsao.copy()
        df_rev = self.base_ult_revisao.copy()
        if df.empty:
            return pd.DataFrame(columns=["ano_mes","custo_previsto"])
        # custo médio por revisão (se houver)
        custo_medio = None
        if not df_rev.empty and "custo_rev" in df_rev.columns:
            v = pd.to_numeric(df_rev["custo_rev"], errors="coerce")
            if v.notna().any():
                custo_medio = v.mean()
        if not custo_medio or math.isnan(custo_medio):
            custo_medio = 500.0  # fallback: valor médio hipotético

        # Contagem por mês
        df = df[pd.notna(df["prox_data_por_tempo"])]
        if df.empty:
            return pd.DataFrame(columns=["ano_mes","custo_previsto"])
        df["ano_mes"] = df["prox_data_por_tempo"].map(lambda d: f"{d.year}-{d.month:02d}")
        g = df.groupby("ano_mes").size().reset_index(name="qtd")
        g["custo_previsto"] = g["qtd"] * custo_medio
        return g[["ano_mes","custo_previsto"]].sort_values("ano_mes")

    def view_anomalias(self) -> pd.DataFrame:
        """
        - Abastecimento mais recente anterior à última revisão (ordem incoerente)
        - KM último < KM base (regressão)
        """
        df = self.previsao.copy()
        if df.empty: return pd.DataFrame(columns=["placa","problema","obs"])
        rows = []
        for _, r in df.iterrows():
            placa = r["placa"]
            d_ult = r["data_km_ultimo"]
            d_base = r["data_km_base"]
            km_ult = r["km_ultimo"]
            km_base = r["km_base"]
            # ordem incoerente
            if pd.notna(d_ult) and pd.notna(d_base) and d_ult < d_base:
                rows.append({"placa": placa, "problema": "Ordem incoerente",
                             "obs": f"Último abastecimento ({d_ult}) é anterior ao km_base ({d_base})"})
            # regressão de km
            if (km_ult is not None) and (km_base is not None) and not pd.isna(km_ult) and not pd.isna(km_base):
                if km_ult < km_base:
                    rows.append({"placa": placa, "problema": "KM regrediu",
                                 "obs": f"km_ultimo ({km_ult}) < km_base ({km_base})"})
        return pd.DataFrame(rows)

# ==============================
# UI – Janela e Abas
# ==============================

class RevisaoWindow(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Revisão – Previsão & Controle")
        self.core = RevisaoCore()

        self._build_ui()
        self._refresh_all_tables()

    # ----- UI helpers -----
    def _build_ui(self):
        root = QVBoxLayout(self)

        # Filtros topo
        top = QHBoxLayout()
        self.cb_unidade = QComboBox()
        self.cb_unidade.setEditable(False)
        self.cb_unidade.addItem("Todas as unidades")
        self.cb_responsavel = QComboBox()
        self.cb_responsavel.addItem("Todos os responsáveis")
        self.ed_busca_placa = QLineEdit()
        self.ed_busca_placa.setPlaceholderText("Buscar placa...")

        self.btn_aplicar = QPushButton("Aplicar filtros")
        self.btn_limpar = QPushButton("Limpar filtros")

        for w in [self.cb_unidade, self.cb_responsavel, self.ed_busca_placa, self.btn_aplicar, self.btn_limpar]:
            top.addWidget(w)
        root.addLayout(top)

        self.btn_aplicar.clicked.connect(self._refresh_all_tables)
        self.btn_limpar.clicked.connect(self._limpar_filtros)

        # Tabs
        self.tabs = QTabWidget()
        root.addWidget(self.tabs)

        # Tab: Geral
        self.tab_geral = QWidget()
        self.tabs.addTab(self.tab_geral, "Geral")
        self._build_tab_geral()

        # Tab: Calendário
        self.tab_cal = QWidget()
        self.tabs.addTab(self.tab_cal, "Calendário")
        self._build_tab_calendario()

        # Tab: Por Responsável
        self.tab_resp = QWidget()
        self.tabs.addTab(self.tab_resp, "Por Responsável")
        self._build_tab_por_responsavel()

        # Tab: Por Oficina
        self.tab_of = QWidget()
        self.tabs.addTab(self.tab_of, "Por Oficina")
        self._build_tab_por_oficina()

        # Tab: Alertas
        self.tab_alerta = QWidget()
        self.tabs.addTab(self.tab_alerta, "Alertas")
        self._build_tab_alertas()

        # Tab: Sem Histórico
        self.tab_sem = QWidget()
        self.tabs.addTab(self.tab_sem, "Sem histórico")
        self._build_tab_sem_historico()

        # Tab: Projeção & Orçamento
        self.tab_proj = QWidget()
        self.tabs.addTab(self.tab_proj, "Projeção & Orçamento")
        self._build_tab_projecao()

        # Tab: Anomalias
        self.tab_anom = QWidget()
        self.tabs.addTab(self.tab_anom, "Anomalias")
        self._build_tab_anomalias()

        # Popular combos de filtro com dados reais
        self._popular_filtros()

    def _popular_filtros(self):
        df = self.core.previsao
        if df.empty: return
        # Unidades
        unidades = sorted(set([u for u in df["unidade"].dropna().astype(str).tolist() if u.strip()]))
        self.cb_unidade.addItems(unidades)
        # Responsáveis
        resps = sorted(set([r for r in df["responsavel"].dropna().astype(str).tolist() if r.strip()]))
        self.cb_responsavel.addItems(resps)

    def _aplicar_filtros_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        placa_q = _norm_placa(self.ed_busca_placa.text())
        u_sel = self.cb_unidade.currentText()
        r_sel = self.cb_responsavel.currentText()

        out = df.copy()
        if u_sel and u_sel != "Todas as unidades" and "unidade" in out.columns:
            out = out[out["unidade"].astype(str) == u_sel]
        if r_sel and r_sel != "Todos os responsáveis" and "responsavel" in out.columns:
            out = out[out["responsavel"].astype(str) == r_sel]
        if placa_q:
            # tenta em colunas possíveis
            if "placa" in out.columns:
                out = out[out["placa"].astype(str).map(_norm_placa).str.contains(placa_q)]
            elif "placa_norm" in out.columns:
                out = out[out["placa_norm"].astype(str).str.contains(placa_q)]
        return out

    def _limpar_filtros(self):
        self.cb_unidade.setCurrentIndex(0)
        self.cb_responsavel.setCurrentIndex(0)
        self.ed_busca_placa.clear()
        self._refresh_all_tables()


    def _create_table(self, parent: QWidget, cols: List[str]) -> QTableWidget:
        tbl = QTableWidget(parent)
        tbl.setColumnCount(len(cols))
        tbl.setHorizontalHeaderLabels([c.replace("_"," ").title() for c in cols])
        tbl.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        return tbl

    def _fill_table(self, tbl: QTableWidget, df: pd.DataFrame, cols: List[str]):
        df = df.copy() if df is not None else pd.DataFrame(columns=cols)
        df = df[cols] if not df.empty else df
        tbl.setRowCount(len(df))
        for i, row in df.iterrows():
            for j, c in enumerate(cols):
                val = row[c]
                s = "" if pd.isna(val) or val is None else str(val)
                tbl.setItem(i, j, QTableWidgetItem(s))
        tbl.resizeColumnsToContents()

    # ---- Tab Geral ----
    def _build_tab_geral(self):
        lay = QVBoxLayout(self.tab_geral)
        # KPIs
        self.lbl_kpis = QLabel()
        lay.addWidget(self.lbl_kpis)

        cols = ["placa","responsavel","unidade","dias_faltando","km_faltando",
                "prox_data_por_tempo","km_meta","km_ultimo","status"]
        self.tbl_geral = self._create_table(self.tab_geral, cols)
        lay.addWidget(self.tbl_geral)

        btns = QHBoxLayout()
        self.btn_export_xlsx = QPushButton("Exportar XLSX")
        self.btn_export_csv  = QPushButton("Exportar CSV")
        self.btn_copiar      = QPushButton("Copiar linhas")
        btns.addWidget(self.btn_export_xlsx)
        btns.addWidget(self.btn_export_csv)
        btns.addWidget(self.btn_copiar)
        btns.addStretch(1)
        lay.addLayout(btns)

        self.btn_export_xlsx.clicked.connect(lambda: self._exportar(self.core.previsao, "PrevisaoRevisao.xlsx"))
        self.btn_export_csv.clicked.connect(lambda: self._exportar(self.core.previsao, "PrevisaoRevisao.csv"))
        self.btn_copiar.clicked.connect(lambda: self._copiar_para_clipboard(self.core.previsao))

    # ---- Tab Calendário ----
    def _build_tab_calendario(self):
        lay = QVBoxLayout(self.tab_cal)
        self.tbl_cal = self._create_table(self.tab_cal, ["ano_mes","qtd"])
        lay.addWidget(self.tbl_cal)

    # ---- Tab Por Responsável ----
    def _build_tab_por_responsavel(self):
        lay = QVBoxLayout(self.tab_resp)
        self.tbl_resp = self._create_table(self.tab_resp, ["responsavel","total","vencido","atencao","em_dia"])
        lay.addWidget(self.tbl_resp)

        # Também por unidade (extra)
        self.tbl_unid = self._create_table(self.tab_resp, ["unidade","total","vencido","atencao","em_dia"])
        lay.addWidget(QLabel("<b>Por Unidade</b>"))
        lay.addWidget(self.tbl_unid)

    # ---- Tab Por Oficina ----
    def _build_tab_por_oficina(self):
        lay = QVBoxLayout(self.tab_of)
        self.tbl_of = self._create_table(self.tab_of, ["oficina","qtd"])
        lay.addWidget(self.tbl_of)

    # ---- Tab Alertas ----
    def _build_tab_alertas(self):
        lay = QVBoxLayout(self.tab_alerta)
        cols = ["placa","responsavel","unidade","dias_faltando","km_faltando","prox_data_por_tempo","status"]
        self.tbl_alerta = self._create_table(self.tab_alerta, cols)
        lay.addWidget(self.tbl_alerta)

    # ---- Tab Sem histórico ----
    def _build_tab_sem_historico(self):
        lay = QVBoxLayout(self.tab_sem)
        cols = ["placa","responsavel","unidade","data_base","km_ultimo","status"]
        self.tbl_sem = self._create_table(self.tab_sem, cols)
        lay.addWidget(self.tbl_sem)

    # ---- Tab Projeção & Orçamento ----
    def _build_tab_projecao(self):
        lay = QVBoxLayout(self.tab_proj)
        self.tbl_proj = self._create_table(self.tab_proj, ["ano_mes","custo_previsto"])
        lay.addWidget(self.tbl_proj)
        hint = QLabel("<i>Observação: custo médio estimado é calculado a partir do histórico (custo_rev) se existir; caso contrário, usa R$ 500 como referência.</i>")
        lay.addWidget(hint)

    # ---- Tab Anomalias ----
    def _build_tab_anomalias(self):
        lay = QVBoxLayout(self.tab_anom)
        cols = ["placa","problema","obs"]
        self.tbl_anom = self._create_table(self.tab_anom, cols)
        lay.addWidget(self.tbl_anom)

    # ----- Refresh -----
    def _refresh_all_tables(self):
        # aplica filtros no dataset principal e re-renderiza todas as tabelas
        df_main = self._aplicar_filtros_df(self.core.previsao)

        # KPIs
        total = len(df_main)
        venc  = (df_main["status"] == "Vencido").sum() if total else 0
        atn   = (df_main["status"] == "Atenção").sum() if total else 0
        emdia = (df_main["status"] == "Em dia").sum() if total else 0
        self.lbl_kpis.setText(f"<b>Total:</b> {total} | <b>Vencidos:</b> {venc} | <b>Atenção:</b> {atn} | <b>Em dia:</b> {emdia}")

        # Geral
        cols_geral = ["placa","responsavel","unidade","dias_faltando","km_faltando",
                      "prox_data_por_tempo","km_meta","km_ultimo","status"]
        self._fill_table(self.tbl_geral, df_main, cols_geral)

        # Calendário
        df_cal = self.core.agg_calendario()
        df_cal = self._aplicar_filtros_df(df_cal) if "unidade" in df_cal.columns else df_cal
        self._fill_table(self.tbl_cal, df_cal, ["ano_mes","qtd"])

        # Por Responsável
        df_resp = self.core.agg_por_responsavel()
        df_unid = self.core.agg_por_unidade()
        # filtros não se aplicam diretamente porque são agregados do universo filtrado.
        # Para coerência, poderíamos recalcular a partir de df_main, mas manteremos simples:
        self._fill_table(self.tbl_resp, df_resp, ["responsavel","total","vencido","atencao","em_dia"])
        self._fill_table(self.tbl_unid, df_unid, ["unidade","total","vencido","atencao","em_dia"])

        # Por Oficina
        df_of = self.core.agg_por_oficina()
        self._fill_table(self.tbl_of, df_of, ["oficina","qtd"])

        # Alertas
        df_al = self.core.view_alertas()
        df_al = self._aplicar_filtros_df(df_al)
        self._fill_table(self.tbl_alerta, df_al, ["placa","responsavel","unidade","dias_faltando","km_faltando","prox_data_por_tempo","status"])

        # Sem histórico
        df_sh = self.core.view_sem_historico()
        df_sh = self._aplicar_filtros_df(df_sh)
        self._fill_table(self.tbl_sem, df_sh, ["placa","responsavel","unidade","data_base","km_ultimo","status"])

        # Projeção
        df_pj = self.core.projecao_orcamento()
        self._fill_table(self.tbl_proj, df_pj, ["ano_mes","custo_previsto"])

        # Anomalias
        df_an = self.core.view_anomalias()
        self._fill_table(self.tbl_anom, df_an, ["placa","problema","obs"])

    # ----- Export / Clipboard -----
    def _exportar(self, df: pd.DataFrame, suggested_name: str):
        if df is None or df.empty:
            QMessageBox.warning(self, "Exportar", "Não há dados para exportar.")
            return
        dlg = QFileDialog(self, "Salvar arquivo", str(BASE / suggested_name))
        if suggested_name.lower().endswith(".xlsx"):
            dlg.setDefaultSuffix("xlsx")
            dlg.setNameFilters(["Planilha Excel (*.xlsx)", "CSV (*.csv)"])
        else:
            dlg.setDefaultSuffix("csv")
            dlg.setNameFilters(["CSV (*.csv)", "Planilha Excel (*.xlsx)"])
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
        if df is None or df.empty: return
        cols = ["placa","responsavel","unidade","dias_faltando","km_faltando",
                "prox_data_por_tempo","km_meta","km_ultimo","status"]
        sub = df[cols] if all(c in df.columns for c in cols) else df.copy()
        txt = sub.to_csv(index=False, sep="\t")
        from PyQt6.QtWidgets import QApplication
        QApplication.clipboard().setText(txt)
        QMessageBox.information(self, "Copiado", "Linhas copiadas para a área de transferência.")