# -*- coding: utf-8 -*-
# cenarios_gerais.py — Janela de "Cenários Gerais" (Combustível e Multas)
# Requisitos: pandas, PyQt6, utils.GlobalFilterBar, utils.df_apply_global_texts

import os, re
import pandas as pd
from datetime import datetime
from PyQt6.QtCore import Qt, QDate
from PyQt6.QtGui import QFont, QColor
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QTabWidget, QFrame, QLabel,
    QPushButton, QDateEdit, QTableWidget, QTableWidgetItem, QHeaderView, QComboBox, QMessageBox
)
from utils import GlobalFilterBar, df_apply_global_texts

DATE_FORMAT = "dd/MM/yyyy"

def _num(s):
    s = str(s or "").strip()
    if not s: return 0.0
    s = re.sub(r"[^\d,.-]", "", s)
    s = s.replace(".", "").replace(",", ".") if ("," in s and "." in s) else s.replace(",", ".")
    try: return float(s)
    except: return 0.0

def _to_date(s):
    s = str(s or "").strip()
    if not s: return pd.NaT
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return pd.to_datetime(s, format=fmt, errors="raise")
        except Exception:
            pass
    return pd.to_datetime(s, dayfirst=True, errors="coerce")

def _norm_placa(x: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(x or "").upper())

def _first_nonempty(series: pd.Series) -> str:
    for x in series:
        s = str(x or "").strip()
        if s: return s
    return ""

def _fmt_money(x): 
    return f"{float(x or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X",".")

def _fmt_num(x): 
    return f"{float(x or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X",".")

def _guess_points(valor: float) -> int:
    v = float(valor or 0)
    v = round(v, 2)
    if abs(v - 88.38) <= 0.5:   return 3
    if abs(v - 130.16) <= 0.8:  return 4
    if abs(v - 195.23) <= 1.0:  return 5
    if abs(v - 293.47) <= 1.5:  return 7
    if v <= 100:   return 3
    if v <= 160:   return 4
    if v <= 230:   return 5
    return 7

# ------------------------ Loader/Normalizer ------------------------

class DataHub:
    """Lê e normaliza todas as fontes necessárias para os cenários gerais."""
    def __init__(self):
        # Combustível
        self.p_extrato = "ExtratoGeral.xlsx"
        self.p_simpl   = "ExtratoSimplificado.xlsx"
        self.p_resp    = "Responsavel.xlsx"

        # Multas
        self.p_multas_sources = [
            "Notificações de Multas - Detalhamento.xlsx",
            "Notificações de Multas - Detalhamento-2.xlsx",
            "Notificações de Multas - Detalhamento (1).xlsx",
            "Notificações de Multas - Fase Pastores.xlsx",
            "Notificações de Multas - Condutor Identificado.xlsx",
        ]

    # ---------- Combustível (Atual) ----------
    def load_combustivel_atual(self) -> pd.DataFrame:
        """
        Responsavel + ExtratoSimplificado -> visão atual por responsável (Nome).
        Retorna DF por responsável com colunas:
        ['NOME','Limite Atual','Compras','Saldo','Limite Próximo','Placa','Modelo','Fabricante','Cidade/UF']
        """
        if not (os.path.exists(self.p_simpl) or os.path.exists(self.p_resp)):
            return pd.DataFrame(columns=["NOME","Limite Atual","Compras","Saldo","Limite Próximo","Placa","Modelo","Fabricante","Cidade/UF"])

        # Responsável (para trazer placa/modelo e eventualmente cidade)
        dr = pd.DataFrame()
        if os.path.exists(self.p_resp):
            try:
                dr = pd.read_excel(self.p_resp, dtype=str).fillna("")
                # normaliza
                dr = dr.rename(columns={
                    "NOME":"NOME","PLACA":"Placa","MODELO":"Modelo","MARCA":"Fabricante",
                    "UF":"Cidade/UF","DATA INÍCIO":"DATA_INI","DATA FIM":"DATA_FIM","STATUS":"STATUS"
                })
                if "Cidade/UF" not in dr.columns:
                    dr["Cidade/UF"] = dr.get("CIDADE", "").astype(str).str.strip()+"/"+dr.get("UF","").astype(str).str.strip()
            except Exception:
                dr = pd.DataFrame()

        # Extrato Simplificado (limites/saldo)
        ds = pd.DataFrame()
        if os.path.exists(self.p_simpl):
            try:
                ds = pd.read_excel(self.p_simpl, dtype=str).fillna("")
                ds = ds.rename(columns={
                    "Nome Responsável":"NOME",
                    "Limite Atual":"Limite Atual",
                    "Compras (utilizado)":"Compras",
                    "Saldo":"Saldo",
                    "Limite Próximo Período":"Limite Próximo",
                    "Placa":"Placa","Modelo":"Modelo","Fabricante":"Fabricante","Cidade/UF":"Cidade/UF"
                })
            except Exception:
                ds = pd.DataFrame()

        # Join "aproximado": se houver placa em ambos, prioriza placa; senão, usa NOME
        out = pd.DataFrame()
        if not ds.empty:
            ds["Limite Atual_num"] = ds.get("Limite Atual","").map(_num)
            ds["Compras_num"]      = ds.get("Compras","").map(_num)
            ds["Saldo_num"]        = ds.get("Saldo","").map(_num)
            ds["Limite Próximo_num"] = ds.get("Limite Próximo","").map(_num)

            # completa campos com DR (caso exista)
            if not dr.empty:
                dr["_PL"] = dr.get("Placa","").map(_norm_placa)
                ds["_PL"] = ds.get("Placa","").map(_norm_placa)
                # merge por PL e depois por NOME (left fill)
                m1 = pd.merge(ds, dr[["NOME","Placa","Modelo","Fabricante","Cidade/UF","_PL"]], on="_PL", how="left", suffixes=("","_DR"))
                # se NOME vazio no DS, usar NOME_DR
                m1["NOME"] = m1["NOME"].where(m1["NOME"].astype(str).str.strip()!="", m1.get("NOME_DR",""))
                # completa campos faltantes com os do DR quando necessário
                for c in ["Placa","Modelo","Fabricante","Cidade/UF"]:
                    m1[c] = m1[c].where(m1[c].astype(str).str.strip()!="", m1.get(c+"_DR",""))
                out = m1
            else:
                out = ds.copy()

        # Seleção final
        cols = ["NOME","Limite Atual","Compras","Saldo","Limite Próximo","Placa","Modelo","Fabricante","Cidade/UF",
                "Limite Atual_num","Compras_num","Saldo_num","Limite Próximo_num"]
        for c in cols:
            if c not in out.columns: out[c] = "" if not c.endswith("_num") else 0.0
        return out[cols].copy()

    # ---------- Combustível (Histórico) ----------
    def load_combustivel_historico(self) -> pd.DataFrame:
        """
        ExtratoGeral -> visão histórica por responsável/motorista.
        Retorna DF de transações com colunas normalizadas:
        ['DT_C','NOME','PLACA','COMBUSTIVEL','LITROS_NUM','VL_LITRO_NUM','VALOR_NUM','ESTABELECIMENTO','CIDADE_UF']
        """
        if not os.path.exists(self.p_extrato):
            return pd.DataFrame(columns=["DT_C","NOME","PLACA","COMBUSTIVEL","LITROS_NUM","VL_LITRO_NUM","VALOR_NUM","ESTABELECIMENTO","CIDADE_UF"])
        try:
            df = pd.read_excel(self.p_extrato, dtype=str).fillna("")
        except Exception:
            return pd.DataFrame(columns=["DT_C","NOME","PLACA","COMBUSTIVEL","LITROS_NUM","VL_LITRO_NUM","VALOR_NUM","ESTABELECIMENTO","CIDADE_UF"])

        # nome do responsável/motorista
        nome_col = None
        for c in ("NOME MOTORISTA","Motorista","MOTORISTA","Responsável","RESPONSÁVEL","RESPONSAVEL","Nome Responsável"):
            if c in df.columns: nome_col = c; break
        df["NOME"] = df.get(nome_col, "")

        m = {
            "DATA TRANSACAO":"DATA_TRANSACAO","PLACA":"PLACA",
            "TIPO COMBUSTIVEL":"COMBUSTIVEL","LITROS":"LITROS","VL/LITRO":"VL_LITRO",
            "VALOR EMISSAO":"VALOR","NOME ESTABELECIMENTO":"ESTABELECIMENTO",
            "CIDADE":"CIDADE","UF":"UF","CIDADE/UF":"CIDADE_UF"
        }
        use = {k:v for k,v in m.items() if k in df.columns}
        df = df.rename(columns=use)

        if "CIDADE_UF" not in df.columns:
            df["CIDADE_UF"] = df.get("CIDADE","").astype(str).str.strip()+"/"+df.get("UF","").astype(str).str.strip()

        df["DT_C"] = df.get("DATA_TRANSACAO","").map(_to_date)
        df["LITROS_NUM"] = df.get("LITROS", "").map(_num)
        df["VL_LITRO_NUM"] = df.get("VL_LITRO", "").map(_num)
        df["VALOR_NUM"] = df.get("VALOR", "").map(_num)
        return df[["DT_C","NOME","PLACA","COMBUSTIVEL","LITROS_NUM","VL_LITRO_NUM","VALOR_NUM","ESTABELECIMENTO","CIDADE_UF"]].copy()

    # ---------- Multas (Geral) ----------
    def load_multas_geral(self) -> pd.DataFrame:
        """
        Consolida todas as fontes de multas, ignora CANCELADA,
        marca DESCONTADA pela Fase Pastores,
        e entrega **UMA LINHA POR FLUIG** preservando 'NOME'.
        Colunas finais:
        ['FLUIG','NOME','Status','DT_M','Data_raw','Placa','Infração','VALOR_NUM','DESCONTADA']
        """
        frames = []
        for path in self.p_multas_sources:
            if not os.path.exists(path): 
                continue
            try:
                df = pd.read_excel(path, dtype=str).fillna("")
            except Exception:
                continue

            # Nome (se existir)
            nome_col = None
            for c in ("Nome","NOME","Responsável","RESPONSÁVEL","RESPONSAVEL"):
                if c in df.columns: nome_col = c; break
            tmp_nome = df.get(nome_col, "")

            # ignora CANCELADA
            col_status = next((c for c in df.columns if c.strip().lower() == "status"), None)
            if col_status:
                df = df[df[col_status].astype(str).str.upper() != "CANCELADA"]

            # colunas essenciais
            col_fluig = next((c for c in df.columns if "FLUIG" in c.upper()), None)
            col_data  = next((c for c in df.columns if "DATA INFRA" in c.upper()), None)
            col_valor = next((c for c in df.columns if "VALOR TOTAL" in c.upper()), None)
            col_inf   = next((c for c in df.columns if c.upper() in ("INFRAÇÃO","INFRACAO")), None)
            col_placa = next((c for c in df.columns if c.strip().upper() == "PLACA"), None)

            tmp = pd.DataFrame()
            tmp["FLUIG"] = df.get(col_fluig, "")
            tmp["NOME"]  = tmp_nome
            tmp["Status"] = df.get(col_status, "")
            tmp["Data_raw"] = df.get(col_data, "")
            tmp["Placa"]  = df.get(col_placa, "")
            tmp["Infração"] = df.get(col_inf, "")
            tmp["Valor"]  = df.get(col_valor, df.get("Valor", ""))
            tmp["VALOR_NUM"] = tmp["Valor"].map(_num)
            tmp["DT_M"] = tmp["Data_raw"].map(_to_date)
            tmp["DESCONTADA"] = False

            # Fase Pastores -> DESCONTADA
            if "Fase Pastores" in os.path.basename(path):
                col_tipo = next((c for c in df.columns if c.strip().upper() == "TIPO"), None)
                col_data_past = next((c for c in df.columns if c.strip().upper() == "DATA PASTORES"), None)
                tipo = df.get(col_tipo, "")
                data_p = df.get(col_data_past, "")
                disc = (tipo.astype(str).str.upper() == "MULTAS PASTORES") & (data_p.astype(str).str.strip() != "")
                tmp["DESCONTADA"] = disc.astype(bool)

            frames.append(tmp)

        base = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
            columns=["FLUIG","NOME","Status","Data_raw","Placa","Infração","Valor","VALOR_NUM","DT_M","DESCONTADA"]
        )
        if base.empty:
            return base

        # Consolidação por FLUIG (preserva Nome e demais campos)
        grp = base.groupby(base["FLUIG"].astype(str), dropna=False)

        def agg_nonempty(col): return grp[col].apply(_first_nonempty)
        def agg_valor(): return grp["VALOR_NUM"].max()  # valor mais alto entre fontes
        def agg_data(): 
            return grp["DT_M"].min()

        consolidated = pd.DataFrame({
            "FLUIG": grp.apply(lambda g: str(g.name)),
            "NOME": agg_nonempty("NOME"),
            "Status": agg_nonempty("Status"),
            "DT_M": agg_data(),
            "Data_raw": agg_nonempty("Data_raw"),
            "Placa": agg_nonempty("Placa"),
            "Infração": agg_nonempty("Infração"),
            "VALOR_NUM": agg_valor(),
            "DESCONTADA": grp["DESCONTADA"].apply(lambda s: bool(s.astype(bool).any())),
        }).reset_index(drop=True)

        return consolidated

# ------------------------ Cálculos Top 10 ------------------------

class Leaderboards:
    """Gera rankings Top 10 a partir dos DataFrames normalizados do DataHub."""

    # ----- Combustível (Atual) -----
    @staticmethod
    def top10_saldos(df_atual: pd.DataFrame) -> pd.DataFrame:
        d = df_atual.copy()
        d = d[["NOME","Saldo_num","Limite Atual_num","Compras_num","Limite Próximo_num","Placa"]]
        d = d.sort_values("Saldo_num", ascending=False).head(10)
        d = d.rename(columns={"Saldo_num":"Saldo (R$)","Limite Atual_num":"Limite Atual (R$)","Compras_num":"Compras (R$)","Limite Próximo_num":"Limite Próx. (R$)"})
        return d

    @staticmethod
    def top10_limites(df_atual: pd.DataFrame) -> pd.DataFrame:
        d = df_atual.copy()
        d = d[["NOME","Limite Atual_num","Saldo_num","Compras_num","Limite Próximo_num","Placa"]]
        d = d.sort_values("Limite Atual_num", ascending=False).head(10)
        d = d.rename(columns={"Limite Atual_num":"Limite Atual (R$)","Saldo_num":"Saldo (R$)","Compras_num":"Compras (R$)","Limite Próximo_num":"Limite Próx. (R$)"})
        return d

    @staticmethod
    def top10_compras(df_atual: pd.DataFrame) -> pd.DataFrame:
        d = df_atual.copy()
        d = d[["NOME","Compras_num","Saldo_num","Limite Atual_num","Limite Próximo_num","Placa"]]
        d = d.sort_values("Compras_num", ascending=False).head(10)
        d = d.rename(columns={"Compras_num":"Compras (R$)","Saldo_num":"Saldo (R$)","Limite Atual_num":"Limite Atual (R$)","Limite Próximo_num":"Limite Próx. (R$)"})
        return d

    # ----- Combustível (Histórico) -----
    @staticmethod
    def _agg_hist(df_hist: pd.DataFrame, a: pd.Timestamp, b: pd.Timestamp) -> pd.DataFrame:
        d = df_hist.copy()
        d = d[(d["DT_C"].notna()) & (d["DT_C"] >= a) & (d["DT_C"] <= b)]
        if d.empty:
            return pd.DataFrame(columns=["NOME","Gasto (R$)","Litros","Preço Médio/L","Abastecimentos"])
        g = d.groupby("NOME", dropna=False).agg(
            gasto=("VALOR_NUM","sum"),
            litros=("LITROS_NUM","sum"),
            abastec=("NOME","count")
        ).reset_index()
        # preço médio ponderado
        # evita divisão por zero
        tmp = d.copy()
        tmp["valor_por_litro"] = tmp.apply(lambda r: (r["VALOR_NUM"]/r["LITROS_NUM"]) if r["LITROS_NUM"]>0 else 0.0, axis=1)
        g_preco = tmp.groupby("NOME")["valor_por_litro"].mean().reset_index().rename(columns={"valor_por_litro":"preco_medio"})
        g = pd.merge(g, g_preco, on="NOME", how="left")
        g = g.rename(columns={"gasto":"Gasto (R$)","litros":"Litros","preco_medio":"Preço Médio/L","abastec":"Abastecimentos"})
        return g

    @staticmethod
    def top10_gasto(df_hist: pd.DataFrame, a: pd.Timestamp, b: pd.Timestamp) -> pd.DataFrame:
        g = Leaderboards._agg_hist(df_hist, a, b)
        return g.sort_values("Gasto (R$)", ascending=False).head(10)

    @staticmethod
    def top10_litros(df_hist: pd.DataFrame, a: pd.Timestamp, b: pd.Timestamp) -> pd.DataFrame:
        g = Leaderboards._agg_hist(df_hist, a, b)
        return g.sort_values("Litros", ascending=False).head(10)

    @staticmethod
    def top10_preco_medio(df_hist: pd.DataFrame, a: pd.Timestamp, b: pd.Timestamp) -> pd.DataFrame:
        g = Leaderboards._agg_hist(df_hist, a, b)
        return g[g["Preço Médio/L"]>0].sort_values("Preço Médio/L", ascending=False).head(10)

    @staticmethod
    def top10_abastecimentos(df_hist: pd.DataFrame, a: pd.Timestamp, b: pd.Timestamp) -> pd.DataFrame:
        g = Leaderboards._agg_hist(df_hist, a, b)
        return g.sort_values("Abastecimentos", ascending=False).head(10)

    # ----- Multas (Geral) -----
    @staticmethod
    def _agg_multas(df_m: pd.DataFrame, a: pd.Timestamp, b: pd.Timestamp) -> pd.DataFrame:
        d = df_m.copy()
        # período
        d = d[(d["DT_M"].notna()) & (d["DT_M"] >= a) & (d["DT_M"] <= b)]
        if d.empty:
            return pd.DataFrame(columns=["NOME","Qtde Multas","Valor Total (R$)","Pontos Estimados","Valor Descontado (R$)","Valor Não Descontado (R$)","% Descontado"])
        grp = d.groupby("NOME", dropna=False)
        q = grp.size().reset_index(name="Qtde Multas")
        v = grp["VALOR_NUM"].sum().reset_index(name="Valor Total (R$)")
        pts = grp["VALOR_NUM"].apply(lambda s: sum(_guess_points(x) for x in s)).reset_index(name="Pontos Estimados")
        desc = grp.apply(lambda g: float(g.loc[g["DESCONTADA"],"VALOR_NUM"].sum())).reset_index(name="Valor Descontado (R$)")
        pend = grp.apply(lambda g: float(g.loc[~g["DESCONTADA"],"VALOR_NUM"].sum())).reset_index(name="Valor Não Descontado (R$)")
        out = q.merge(v, on="NOME").merge(pts, on="NOME").merge(desc, on="NOME").merge(pend, on="NOME")
        # % descontado
        out["% Descontado"] = out.apply(
            lambda r: (100.0 * r["Valor Descontado (R$)"] / r["Valor Total (R$)"]) if r["Valor Total (R$)"]>0 else 0.0,
            axis=1
        )
        return out

    @staticmethod
    def top10_qtde_multas(df_m: pd.DataFrame, a: pd.Timestamp, b: pd.Timestamp) -> pd.DataFrame:
        g = Leaderboards._agg_multas(df_m, a, b)
        return g.sort_values("Qtde Multas", ascending=False).head(10)

    @staticmethod
    def top10_valor_multas(df_m: pd.DataFrame, a: pd.Timestamp, b: pd.Timestamp) -> pd.DataFrame:
        g = Leaderboards._agg_multas(df_m, a, b)
        return g.sort_values("Valor Total (R$)", ascending=False).head(10)

    @staticmethod
    def top10_pontos(df_m: pd.DataFrame, a: pd.Timestamp, b: pd.Timestamp) -> pd.DataFrame:
        g = Leaderboards._agg_multas(df_m, a, b)
        return g.sort_values("Pontos Estimados", ascending=False).head(10)

    @staticmethod
    def top10_pct_descontado(df_m: pd.DataFrame, a: pd.Timestamp, b: pd.Timestamp) -> pd.DataFrame:
        g = Leaderboards._agg_multas(df_m, a, b)
        return g.sort_values("% Descontado", ascending=False).head(10)

    @staticmethod
    def top10_valor_nao_descontado(df_m: pd.DataFrame, a: pd.Timestamp, b: pd.Timestamp) -> pd.DataFrame:
        g = Leaderboards._agg_multas(df_m, a, b)
        return g.sort_values("Valor Não Descontado (R$)", ascending=False).head(10)

# ------------------------ UI ------------------------

class CenariosGeraisWindow(QWidget):
    """
    Janela com:
      - Aba Combustível: alternador (Atual|Histórico) + sub-abas Top10
      - Aba Multas: sub-abas Top10 (sempre geral; canceladas excluídas)
      - Período + Filtro Global aplicáveis a todas as visualizações
    """
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Cenários Gerais — Combustível e Multas")
        self.resize(1260, 820)

        self.hub = DataHub()
        self.df_comb_atual = pd.DataFrame()
        self.df_comb_hist  = pd.DataFrame()
        self.df_multas     = pd.DataFrame()

        self._build_ui()
        self._load_all()
        self.apply_all()

    # ------- UI building -------
    def _build_ui(self):
        root = QVBoxLayout(self)

        # Header
        head = QFrame(); self._shadow(head, blur=40)
        hv = QVBoxLayout(head); hv.setContentsMargins(16,16,16,16)
        t = QLabel("Cenários Gerais — Painel Consolidado"); t.setAlignment(Qt.AlignmentFlag.AlignCenter)
        t.setFont(QFont("Arial", 20, QFont.Weight.Bold))
        hv.addWidget(t)
        root.addWidget(head)

        # Controls bar
        bar = QFrame(); self._shadow(bar, blur=30)
        bl = QGridLayout(bar)

        self.de_ini = QDateEdit(); self.de_fim = QDateEdit()
        for de in (self.de_ini, self.de_fim):
            de.setCalendarPopup(True); de.setDisplayFormat(DATE_FORMAT)
        today = pd.Timestamp.today().normalize()
        self.de_ini.setDate(QDate(today.year, today.month, 1))
        self.de_fim.setDate(QDate(today.year, today.month, today.day))

        self.global_bar = GlobalFilterBar("Filtro global:")
        self.btn_refresh = QPushButton("Recarregar Arquivos")
        self.btn_apply   = QPushButton("Aplicar Período/Filtro")

        bl.addWidget(QLabel("Início:"), 0, 0); bl.addWidget(self.de_ini, 0, 1)
        bl.addWidget(QLabel("Fim:"),    0, 2); bl.addWidget(self.de_fim, 0, 3)
        bl.addWidget(self.global_bar,   1, 0, 1, 4)
        bl.addWidget(self.btn_refresh,  2, 0, 1, 2)
        bl.addWidget(self.btn_apply,    2, 2, 1, 2)

        root.addWidget(bar)

        # Tabs
        self.tabs = QTabWidget()
        root.addWidget(self.tabs, 1)

        # --- Combustível tab ---
        self.tab_comb = QWidget(); v1 = QVBoxLayout(self.tab_comb)
        top_line = QHBoxLayout()
        top_line.addWidget(QLabel("Fonte:"))
        self.cb_fonte = QComboBox(); self.cb_fonte.addItems(["Atual (Responsável + Extrato Simplificado)","Histórico (Extrato Geral)"])
        top_line.addWidget(self.cb_fonte); top_line.addStretch(1)
        self.lbl_info_comb = QLabel("")
        top_line.addWidget(self.lbl_info_comb)
        v1.addLayout(top_line)

        self.tabs_comb = QTabWidget()
        # sub-abas Combustível
        self.tbl_saldos = self._mk_table(["NOME","Saldo (R$)","Limite Atual (R$)","Compras (R$)","Limite Próx. (R$)","Placa"])
        self.tbl_limites = self._mk_table(["NOME","Limite Atual (R$)","Saldo (R$)","Compras (R$)","Limite Próx. (R$)","Placa"])
        self.tbl_compras = self._mk_table(["NOME","Compras (R$)","Saldo (R$)","Limite Atual (R$)","Limite Próx. (R$)","Placa"])
        self.tbl_gasto   = self._mk_table(["NOME","Gasto (R$)","Litros","Preço Médio/L","Abastecimentos"])
        self.tbl_litros  = self._mk_table(["NOME","Litros","Gasto (R$)","Preço Médio/L","Abastecimentos"])
        self.tbl_preco   = self._mk_table(["NOME","Preço Médio/L","Gasto (R$)","Litros","Abastecimentos"])
        self.tbl_abast   = self._mk_table(["NOME","Abastecimentos","Gasto (R$)","Litros","Preço Médio/L"])

        # grupos de abas conforme fonte
        self.tabs_comb.addTab(self._wrap(self.tbl_saldos), "Top 10 — Maiores Saldos (Atual)")
        self.tabs_comb.addTab(self._wrap(self.tbl_limites), "Top 10 — Maiores Limites (Atual)")
        self.tabs_comb.addTab(self._wrap(self.tbl_compras), "Top 10 — Maiores Compras (Atual)")
        self.tabs_comb.addTab(self._wrap(self.tbl_gasto),   "Top 10 — Maior Gasto (Hist.)")
        self.tabs_comb.addTab(self._wrap(self.tbl_litros),  "Top 10 — Mais Litros (Hist.)")
        self.tabs_comb.addTab(self._wrap(self.tbl_preco),   "Top 10 — Preço Médio/L ↑ (Hist.)")
        self.tabs_comb.addTab(self._wrap(self.tbl_abast),   "Top 10 — + Abastecimentos (Hist.)")
        v1.addWidget(self.tabs_comb, 1)

        self.tabs.addTab(self.tab_comb, "Combustível")

        # --- Multas tab ---
        self.tab_multas = QWidget(); v2 = QVBoxLayout(self.tab_multas)
        self.lbl_info_mult = QLabel("Canceladas excluídas.")
        v2.addWidget(self.lbl_info_mult)

        self.tabs_mult = QTabWidget()
        self.tbl_qtde  = self._mk_table(["NOME","Qtde Multas","Valor Total (R$)","Pontos Estimados","% Descontado","Valor Não Descontado (R$)"])
        self.tbl_valor = self._mk_table(["NOME","Valor Total (R$)","Qtde Multas","Pontos Estimados","% Descontado","Valor Não Descontado (R$)"])
        self.tbl_pontos= self._mk_table(["NOME","Pontos Estimados","Qtde Multas","Valor Total (R$)","% Descontado","Valor Não Descontado (R$)"])
        self.tbl_pct   = self._mk_table(["NOME","% Descontado","Valor Descontado (R$)","Valor Total (R$)","Qtde Multas","Pontos Estimados"])
        self.tbl_nao   = self._mk_table(["NOME","Valor Não Descontado (R$)","Valor Total (R$)","Qtde Multas","Pontos Estimados","% Descontado"])

        self.tabs_mult.addTab(self._wrap(self.tbl_qtde),  "Top 10 — + Multas")
        self.tabs_mult.addTab(self._wrap(self.tbl_valor), "Top 10 — Maior Valor Total")
        self.tabs_mult.addTab(self._wrap(self.tbl_pontos),"Top 10 — + Pontos (estim.)")
        self.tabs_mult.addTab(self._wrap(self.tbl_pct),   "Top 10 — % Descontado")
        self.tabs_mult.addTab(self._wrap(self.tbl_nao),   "Top 10 — Não Descontado")
        v2.addWidget(self.tabs_mult, 1)

        self.tabs.addTab(self.tab_multas, "Multas")

        # wiring
        self.btn_refresh.clicked.connect(self._load_all_and_apply)
        self.btn_apply.clicked.connect(self.apply_all)
        self.cb_fonte.currentIndexChanged.connect(self.apply_combustivel)

    def goto_multas(self):
        """Abra direto na aba Multas."""
        self.tabs.setCurrentWidget(self.tab_multas)

    # ------- helpers UI -------
    def _mk_table(self, headers):
        t = QTableWidget()
        t.setAlternatingRowColors(True)
        t.setSortingEnabled(True)
        t.horizontalHeader().setSortIndicatorShown(True)
        t.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        t.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        t.setColumnCount(len(headers))
        t.setHorizontalHeaderLabels(headers)
        return t

    def _wrap(self, w):
        box = QFrame(); v = QVBoxLayout(box); v.setContentsMargins(6,6,6,6); v.addWidget(w)
        return box

    def _shadow(self, w, blur=50, color=QColor(0,0,0,70)):
        from PyQt6.QtWidgets import QGraphicsDropShadowEffect
        eff = QGraphicsDropShadowEffect()
        eff.setOffset(0, 6)
        eff.setBlurRadius(blur)
        eff.setColor(color)
        w.setGraphicsEffect(eff)

    # ------- data loading -------
    def _load_all(self):
        try:
            self.df_comb_atual = self.hub.load_combustivel_atual()
        except Exception as e:
            self.df_comb_atual = pd.DataFrame()
            QMessageBox.warning(self, "Cenários Gerais", f"Erro ao carregar Combustível (Atual): {e}")

        try:
            self.df_comb_hist = self.hub.load_combustivel_historico()
        except Exception as e:
            self.df_comb_hist = pd.DataFrame()
            QMessageBox.warning(self, "Cenários Gerais", f"Erro ao carregar Combustível (Histórico): {e}")

        try:
            self.df_multas = self.hub.load_multas_geral()
        except Exception as e:
            self.df_multas = pd.DataFrame()
            QMessageBox.warning(self, "Cenários Gerais", f"Erro ao carregar Multas: {e}")

    def _load_all_and_apply(self):
        self._load_all()
        self.apply_all()

    # ------- apply filters & fill tables -------
    def _period(self):
        q0, q1 = self.de_ini.date(), self.de_fim.date()
        a = pd.Timestamp(q0.year(), q0.month(), q0.day())
        b = pd.Timestamp(q1.year(), q1.month(), q1.day())
        if a > b: a, b = b, a
        return a, b

    def _global_values(self):
        try:
            return self.global_bar.values()
        except Exception:
            return []

    def apply_all(self):
        self.apply_combustivel()
        self.apply_multas()

    # Combustível
    def apply_combustivel(self):
        a, b = self._period()
        fonte_idx = self.cb_fonte.currentIndex()
        glb = self._global_values()

        self.lbl_info_comb.setText(f"Período: {a.strftime('%d/%m/%Y')} a {b.strftime('%d/%m/%Y')} — {'Atual' if fonte_idx==0 else 'Histórico'}")

        if fonte_idx == 0:
            d = self.df_comb_atual.copy()
            if not d.empty:
                d = df_apply_global_texts(d, glb)
                # Preenche sub-abas "Atual"
                self._fill(self.tbl_saldos, Leaderboards.top10_saldos(d))
                self._fill(self.tbl_limites, Leaderboards.top10_limites(d))
                self._fill(self.tbl_compras, Leaderboards.top10_compras(d))
            else:
                for t in (self.tbl_saldos,self.tbl_limites,self.tbl_compras): self._fill(t, pd.DataFrame())
        else:
            d = self.df_comb_hist.copy()
            if not d.empty:
                # período + filtro global
                d = d[(d["DT_C"].notna()) & (d["DT_C"] >= a) & (d["DT_C"] <= b)]
                d = df_apply_global_texts(d, glb)
                self._fill(self.tbl_gasto,  Leaderboards.top10_gasto(d, a, b))
                self._fill(self.tbl_litros, Leaderboards.top10_litros(d, a, b))
                self._fill(self.tbl_preco,  Leaderboards.top10_preco_medio(d, a, b))
                self._fill(self.tbl_abast,  Leaderboards.top10_abastecimentos(d, a, b))
            else:
                for t in (self.tbl_gasto,self.tbl_litros,self.tbl_preco,self.tbl_abast): self._fill(t, pd.DataFrame())

    # Multas
    def apply_multas(self):
        a, b = self._period()
        glb = self._global_values()
        self.lbl_info_mult.setText(f"Canceladas excluídas. Período: {a.strftime('%d/%m/%Y')} a {b.strftime('%d/%m/%Y')}.")

        d = self.df_multas.copy()
        if d.empty:
            for t in (self.tbl_qtde,self.tbl_valor,self.tbl_pontos,self.tbl_pct,self.tbl_nao): self._fill(t, pd.DataFrame())
            return

        d = df_apply_global_texts(d, glb)
        # Período é aplicado nos cálculos internos

        self._fill(self.tbl_qtde,  Leaderboards.top10_qtde_multas(d, a, b))
        self._fill(self.tbl_valor, Leaderboards.top10_valor_multas(d, a, b))
        self._fill(self.tbl_pontos,Leaderboards.top10_pontos(d, a, b))

        g = Leaderboards._agg_multas(d, a, b)
        # % Descontado
        self._fill(self.tbl_pct, g.sort_values("% Descontado", ascending=False).head(10)[
            ["NOME","% Descontado","Valor Descontado (R$)","Valor Total (R$)","Qtde Multas","Pontos Estimados"]
        ])
        # Não descontado
        self._fill(self.tbl_nao, g.sort_values("Valor Não Descontado (R$)", ascending=False).head(10)[
            ["NOME","Valor Não Descontado (R$)","Valor Total (R$)","Qtde Multas","Pontos Estimados","% Descontado"]
        ])

    def _fill(self, tbl: QTableWidget, df: pd.DataFrame):
        headers = list(df.columns) if not df.empty else [h.text() for h in tbl.horizontalHeaderItem(i) and [tbl.horizontalHeaderItem(i) for i in range(tbl.columnCount())]]
        tbl.setSortingEnabled(False)
        tbl.clear()
        tbl.setColumnCount(len(headers))
        tbl.setHorizontalHeaderLabels(headers)
        n = len(df)
        tbl.setRowCount(n)
        for i in range(n):
            for j, c in enumerate(headers):
                v = df.iloc[i][c]
                if isinstance(v, float) and ("R$" in c or "Preço" in c or "Litros" in c):
                    if "R$" in c: s = _fmt_money(v)
                    elif "Litros" in c: s = _fmt_num(v)
                    else: s = _fmt_num(v)
                elif isinstance(v, float) and "%" in c:
                    s = f"{v:.2f}%"
                else:
                    s = str(v)
                it = QTableWidgetItem(s)
                it.setFlags(it.flags() & ~Qt.ItemFlag.ItemIsEditable)
                tbl.setItem(i, j, it)
        tbl.resizeColumnsToContents()
        tbl.horizontalHeader().setStretchLastSection(True)
        tbl.setSortingEnabled(True)