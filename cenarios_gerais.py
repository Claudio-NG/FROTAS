import os, re
import pandas as pd
from PyQt6.QtCore import Qt, QDate
from PyQt6.QtGui import QFont, QColor
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QTabWidget, QFrame, QLabel,
    QPushButton, QDateEdit, QTableWidget, QTableWidgetItem, QHeaderView, QComboBox,
    QMessageBox, QFileDialog
)
from utils import GlobalFilterBar, df_apply_global_texts

DATE_FORMAT = "dd/MM/yyyy"


class NumericItem(QTableWidgetItem):
    """
    Item que sabe ordenar numericamente quando tiver um valor float associado.
    Fallback: compara texto (case-insensitive).
    """
    def __init__(self, text: str, num_value: float | None = None):
        super().__init__(text)
        self._num = None
        if num_value is not None:
            try:
                self._num = float(num_value)
            except Exception:
                self._num = None
        # torna não-editável
        self.setFlags(self.flags() & ~Qt.ItemFlag.ItemIsEditable)

    def __lt__(self, other):
        # Se ambos têm número, compara número
        if isinstance(other, NumericItem) and self._num is not None and other._num is not None:
            return self._num < other._num
        # tenta converter a partir do texto (usa seu _num helper que já normaliza)
        try:
            a = float(_num(self.text()))
            b = float(_num(other.text()))
            return a < b
        except Exception:
            # fallback: texto
            return self.text().lower() < other.text().lower()


def _num(s):
    s = str(s or "").strip()
    if not s: return 0.0
    s = re.sub(r"[^\d,.-]", "", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
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

def _fmt_money(x):
    try:
        return f"{float(x or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(x)

def _fmt_num(x):
    try:
        return f"{float(x or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(x)

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

# --------------------- DataHub ---------------------

class DataHub:
    def __init__(self):
        self.p_extrato = "ExtratoGeral.xlsx"
        self.p_simpl   = "ExtratoSimplificado.xlsx"
        self.p_resp    = "Responsavel.xlsx"
        self.p_multas_sources = [
            "Notificações de Multas - Detalhamento.xlsx",
            "Notificações de Multas - Detalhamento-2.xlsx",
            "Notificações de Multas - Detalhamento (1).xlsx",
            "Notificações de Multas - Fase Pastores.xlsx",
            "Notificações de Multas - Condutor Identificado.xlsx",
        ]

    # -------- Responsável ativo (por placa) --------
    def _map_responsavel_por_placa(self) -> pd.DataFrame:
        if not os.path.exists(self.p_resp):
            return pd.DataFrame(columns=["PLACA_N","Responsavel","Modelo","Fabricante","Cidade/UF"])
        try:
            df = pd.read_excel(self.p_resp, dtype=str).fillna("")
        except Exception:
            return pd.DataFrame(columns=["PLACA_N","Responsavel","Modelo","Fabricante","Cidade/UF"])

        # colunas usuais
        c_nome = next((c for c in df.columns if c.strip().upper() == "NOME"), None)
        c_placa = next((c for c in df.columns if c.strip().upper() == "PLACA"), None)
        c_status = next((c for c in df.columns if c.strip().upper() == "STATUS"), None)
        c_fim = next((c for c in df.columns if "DATA" in c.upper() and "FIM" in c.upper()), None)
        c_ini = next((c for c in df.columns if "DATA" in c.upper() and ("INÍC" in c.upper() or "INIC" in c.upper())), None)
        c_modelo = next((c for c in df.columns if c.strip().upper() == "MODELO"), None)
        c_marca  = next((c for c in df.columns if c.strip().upper() in ("MARCA","FABRICANTE")), None)
        c_uf     = next((c for c in df.columns if c.strip().upper() in ("UF","CIDADE/UF")), None)

        if not c_nome or not c_placa:
            return pd.DataFrame(columns=["PLACA_N","Responsavel","Modelo","Fabricante","Cidade/UF"])

        df["_PL"] = df[c_placa].map(_norm_placa)
        df["_INI"] = pd.to_datetime(df[c_ini], dayfirst=True, errors="coerce") if c_ini else pd.NaT
        df["_FIM"] = pd.to_datetime(df[c_fim], dayfirst=True, errors="coerce") if c_fim else pd.NaT

        if c_status:
            act = df[(df[c_status].astype(str).str.upper() != "VENDIDO") & (df["_FIM"].isna())].copy()
        else:
            act = df[df["_FIM"].isna()].copy()
        if act.empty:
            act = df.copy()
        act = act.dropna(subset=["_PL"]).sort_values(["_INI"], ascending=[False])

        g = act.groupby("_PL").first().reset_index()
        out = pd.DataFrame({
            "PLACA_N": g["_PL"],
            "Responsavel": g[c_nome],
            "Modelo": g[c_modelo] if c_modelo else "",
            "Fabricante": g[c_marca] if c_marca else "",
            "Cidade/UF": g[c_uf] if c_uf else "",
        })
        return out

    
    def load_combustivel_atual(self) -> pd.DataFrame:
        if not os.path.exists(self.p_simpl):
            return pd.DataFrame(columns=[
                "Responsavel","Placa","Modelo","Fabricante","Cidade/UF",
                "Limite Atual","Compras","Saldo","Limite Próximo","pctSaldo"
            ])
        try:
            ds = pd.read_excel(self.p_simpl, dtype=str).fillna("")
        except Exception:
            return pd.DataFrame(columns=[
                "Responsavel","Placa","Modelo","Fabricante","Cidade/UF",
                "Limite Atual","Compras","Saldo","Limite Próximo","pctSaldo"
            ])

        # normalização
        ren = {}
        for a,b in {
            "Placa":"Placa","Nome Responsável":"Responsavel","RESPONSÁVEL":"Responsavel","RESPONSAVEL":"Responsavel",
            "Limite Atual":"Limite Atual","Compras (utilizado)":"Compras","Saldo":"Saldo",
            "Limite Próximo Período":"Limite Próximo","Modelo":"Modelo","Fabricante":"Fabricante","Cidade/UF":"Cidade/UF"
        }.items():
            if a in ds.columns: ren[a] = b
        ds = ds.rename(columns=ren)
        ds["PLACA_N"] = ds.get("Placa","").map(_norm_placa)

        for c in ["Limite Atual","Compras","Saldo","Limite Próximo"]:
            ds[c] = ds.get(c,"").map(_num)

        # injetar responsável/detalhes a partir de Responsavel.xlsx (prioriza o mapeado por placa)
        mapa = self._map_responsavel_por_placa()
        if not mapa.empty:
            ds = ds.merge(mapa, on="PLACA_N", how="left", suffixes=("","_MAP"))
            # escolhe: usar do mapa quando vazio
            ds["Responsavel"] = ds["Responsavel"].where(ds["Responsavel"].astype(str).str.strip()!="", ds["Responsavel_MAP"])
            for c in ["Modelo","Fabricante","Cidade/UF"]:
                ds[c] = ds[c].where(ds[c].astype(str).str.strip()!="", ds[f"{c}_MAP"])
            for c in ["Responsavel_MAP","Modelo_MAP","Fabricante_MAP","Cidade/UF_MAP"]:
                if c in ds.columns: ds.drop(columns=[c], inplace=True)

        ds["pctSaldo"] = (100.0 * ds["Saldo"] / ds["Limite Atual"]).replace([pd.NA, pd.NaT, float("inf")], 0.0)

        cols = ["Responsavel","Placa","Modelo","Fabricante","Cidade/UF","Limite Atual","Compras","Saldo","Limite Próximo","pctSaldo","PLACA_N"]
        for c in cols:
            if c not in ds.columns: ds[c] = "" if c not in ("Limite Atual","Compras","Saldo","Limite Próximo","pctSaldo") else 0.0
        return ds[cols].copy()

  
    def load_combustivel_historico(self) -> pd.DataFrame:
        if not os.path.exists(self.p_extrato):
            return pd.DataFrame(columns=[
                "DT","Responsavel","PLACA","PLACA_N","COMBUSTIVEL","LITROS_NUM","VL_LITRO_NUM","VALOR_NUM","ESTABELECIMENTO","CIDADE_UF"
            ])
        try:
            df = pd.read_excel(self.p_extrato, dtype=str).fillna("")
        except Exception:
            return pd.DataFrame(columns=[
                "DT","Responsavel","PLACA","PLACA_N","COMBUSTIVEL","LITROS_NUM","VL_LITRO_NUM","VALOR_NUM","ESTABELECIMENTO","CIDADE_UF"
            ])

        ren = {}
        for a,b in {
            "DATA TRANSACAO":"DATA","PLACA":"PLACA",
            "NOME MOTORISTA":"Responsavel","Motorista":"Responsavel","MOTORISTA":"Responsavel",
            "Responsável":"Responsavel","RESPONSÁVEL":"Responsavel","RESPONSAVEL":"Responsavel","Nome Responsável":"Responsavel",
            "TIPO COMBUSTIVEL":"COMBUSTIVEL","LITROS":"LITROS","VL/LITRO":"VL_LITRO","VALOR EMISSAO":"VALOR",
            "NOME ESTABELECIMENTO":"ESTABELECIMENTO","CIDADE":"CIDADE","UF":"UF","CIDADE/UF":"CIDADE_UF"
        }.items():
            if a in df.columns: ren[a] = b
        df = df.rename(columns=ren)

        if "CIDADE_UF" not in df.columns:
            df["CIDADE_UF"] = df.get("CIDADE","").astype(str).str.strip()+"/"+df.get("UF","").astype(str).str.strip()

        df["DT"] = df.get("DATA","").map(_to_date)
        df["LITROS_NUM"]   = df.get("LITROS","").map(_num)
        df["VL_LITRO_NUM"] = df.get("VL_LITRO","").map(_num)
        df["VALOR_NUM"]    = df.get("VALOR","").map(_num)
        df["PLACA_N"]      = df.get("PLACA","").map(_norm_placa)

        # reforça responsável via mapa por placa, quando estiver vazio
        mapa = self._map_responsavel_por_placa()
        if not mapa.empty:
            df = df.merge(mapa[["PLACA_N","Responsavel"]].rename(columns={"Responsavel":"Responsavel_MAP"}), on="PLACA_N", how="left")
            df["Responsavel"] = df["Responsavel"].where(df["Responsavel"].astype(str).str.strip()!="", df["Responsavel_MAP"])
            if "Responsavel_MAP" in df.columns: df.drop(columns=["Responsavel_MAP"], inplace=True)

        cols = ["DT","Responsavel","PLACA","PLACA_N","COMBUSTIVEL","LITROS_NUM","VL_LITRO_NUM","VALOR_NUM","ESTABELECIMENTO","CIDADE_UF"]
        for c in cols:
            if c not in df.columns: df[c] = "" if c in ("Responsavel","PLACA","PLACA_N","COMBUSTIVEL","ESTABELECIMENTO","CIDADE_UF") else 0.0
        return df[cols].copy()

    # -------- Multas (consolidado por FLUIG) --------
    def load_multas(self) -> pd.DataFrame:
        frames = []
        for path in self.p_multas_sources:
            if not os.path.exists(path): continue
            try:
                df = pd.read_excel(path, dtype=str).fillna("")
            except Exception:
                continue

            nome_col = next((c for c in ("Nome","NOME","Responsável","RESPONSÁVEL","RESPONSAVEL") if c in df.columns), None)
            col_status = next((c for c in df.columns if c.strip().lower() == "status"), None)
            if col_status:
                df = df[df[col_status].astype(str).str.upper() != "CANCELADA"]

            col_fluig = next((c for c in df.columns if "FLUIG" in c.upper()), None)
            col_data  = next((c for c in df.columns if "DATA INFRA" in c.upper()), None)
            col_valor = next((c for c in df.columns if "VALOR TOTAL" in c.upper()), None)
            col_inf   = next((c for c in df.columns if c.upper() in ("INFRAÇÃO","INFRACAO")), None)
            col_placa = next((c for c in df.columns if c.strip().upper() == "PLACA"), None)

            tmp = pd.DataFrame({
                "FLUIG": df.get(col_fluig, ""),
                "Responsavel": df.get(nome_col, ""),
                "Status": df.get(col_status, ""),
                "Data_raw": df.get(col_data, ""),
                "Placa": df.get(col_placa, ""),
                "Infracao": df.get(col_inf, ""),
                "Valor": df.get(col_valor, df.get("Valor", "")),
            })
            tmp["VALOR_NUM"] = tmp["Valor"].map(_num)
            tmp["DT_M"] = tmp["Data_raw"].map(_to_date)
            tmp["DESCONTADA"] = False

            if "Fase Pastores" in os.path.basename(path):
                col_tipo = next((c for c in df.columns if c.strip().upper() == "TIPO"), None)
                col_data_past = next((c for c in df.columns if c.strip().upper() == "DATA PASTORES"), None)
                tipo = df.get(col_tipo, "")
                data_p = df.get(col_data_past, "")
                disc = (tipo.astype(str).str.upper() == "MULTAS PASTORES") & (data_p.astype(str).str.strip() != "")
                tmp["DESCONTADA"] = disc.astype(bool)

            frames.append(tmp)

        base = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
            columns=["FLUIG","Responsavel","Status","Data_raw","Placa","Infracao","Valor","VALOR_NUM","DT_M","DESCONTADA"]
        )
        if base.empty:
            return base

        grp = base.groupby(base["FLUIG"].astype(str), dropna=False)
        consolidated = pd.DataFrame({
            "FLUIG": grp.apply(lambda g: str(g.name)),
            "Responsavel": grp["Responsavel"].apply(lambda s: next((x for x in s if str(x).strip()), "")),
            "Status": grp["Status"].apply(lambda s: next((x for x in s if str(x).strip()), "")),
            "DT_M": grp["DT_M"].min(),
            "Data_raw": grp["Data_raw"].apply(lambda s: next((x for x in s if str(x).strip()), "")),
            "Placa": grp["Placa"].apply(lambda s: next((x for x in s if str(x).strip()), "")),
            "Infracao": grp["Infracao"].apply(lambda s: next((x for x in s if str(x).strip()), "")),
            "VALOR_NUM": grp["VALOR_NUM"].max(),
            "DESCONTADA": grp["DESCONTADA"].apply(lambda s: bool(pd.Series(s).astype(bool).any())),
        }).reset_index(drop=True)
        return consolidated

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QTabWidget, QLabel, QMessageBox, QShortcut
)
from PyQt6.QtGui import QKeySequence

class CenariosGeraisContainer(QWidget):

    def __init__(self, parent=None, titulo_base: str = "Cenários Gerais"):
        super().__init__(parent)
        self.titulo_base = titulo_base
        self._seq = 0  # contador para nomes das abas
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        top = QHBoxLayout()
        self.btn_new = QPushButton("Novo Cenário")
        self.btn_new.setToolTip("Abrir uma nova aba de Cenários Gerais (Ctrl+N)")
        top.addWidget(QLabel(self.titulo_base))
        top.addStretch(1)
        top.addWidget(self.btn_new)
        layout.addLayout(top)

        self.tabsHost = QTabWidget()
        self.tabsHost.setTabsClosable(True)
        self.tabsHost.setMovable(True)
        self.tabsHost.tabCloseRequested.connect(self._close_tab)
        layout.addWidget(self.tabsHost, 1)

        # atalhos
        QShortcut(QKeySequence("Ctrl+N"), self, activated=self.new_tab)
        QShortcut(QKeySequence("Ctrl+W"), self, activated=self._close_current_tab)

        # primeira aba
        self.new_tab()

        # sinais
        self.btn_new.clicked.connect(self.new_tab)

    # ---- API pública ----
    def new_tab(self):
        """Cria uma nova aba com uma instância de CenariosGeraisWindow."""
        try:
            page = CenariosGeraisWindow()  # usa sua classe existente (já é QWidget)
        except Exception as e:
            QMessageBox.critical(self, "Cenários Gerais", f"Falha ao criar cenário: {e}")
            return
        self._seq += 1
        title = f"{self.titulo_base} {self._seq}"
        idx = self.tabsHost.addTab(page, title)
        self.tabsHost.setCurrentIndex(idx)

    # ---- helpers ----
    def _close_tab(self, index: int):
        w = self.tabsHost.widget(index)
        self.tabsHost.removeTab(index)
        if w is not None:
            w.deleteLater()
        if self.tabsHost.count() == 0:
            # opcional: sempre manter ao menos uma aba aberta
            self.new_tab()

    def _close_current_tab(self):
        idx = self.tabsHost.currentIndex()
        if idx >= 0:
            self._close_tab(idx)

class CenariosGeraisWindow(QWidget):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Cenários Gerais — Combustível e Multas")
        self.resize(1280, 860)

        self.hub = DataHub()
        self.df_comb_atual = pd.DataFrame()
        self.df_comb_hist  = pd.DataFrame()
        self.df_multas     = pd.DataFrame()

        self._build_ui()
        self._load_all()
        self.apply_all()



    def _toggle_sort(self, tbl: QTableWidget, col: int):
        hdr = tbl.horizontalHeader()
        current_col = hdr.sortIndicatorSection()
        current_order = hdr.sortIndicatorOrder()
        if current_col == col:
            # Alterna
            new_order = Qt.SortOrder.DescendingOrder if current_order == Qt.SortOrder.AscendingOrder else Qt.SortOrder.AscendingOrder
        else:
            # Nova coluna: começa crescente
            new_order = Qt.SortOrder.AscendingOrder
        tbl.sortItems(col, new_order)



    def _build_ui(self):
        root = QVBoxLayout(self)

        # Header
        head = QFrame(); self._shadow(head, blur=40)
        hv = QGridLayout(head); hv.setContentsMargins(12,12,12,12)

        self.de_ini = QDateEdit(); self.de_fim = QDateEdit()
        for de in (self.de_ini, self.de_fim):
            de.setCalendarPopup(True); de.setDisplayFormat(DATE_FORMAT)
        today = pd.Timestamp.today().normalize()
        self.de_ini.setDate(QDate(today.year, today.month, 1))
        self.de_fim.setDate(QDate(today.year, today.month, today.day))

        self.global_bar = GlobalFilterBar("Filtro global:")
        self.btn_refresh = QPushButton("Recarregar Arquivos")
        self.btn_apply   = QPushButton("Aplicar")
        self.btn_export  = QPushButton("Exportar")
        self.btn_export.setToolTip("Exporta a tabela da sub-aba visível (CSV ou Excel)")

        
        hv.addWidget(QLabel("Início:"), 0, 0); hv.addWidget(self.de_ini, 0, 1)
        hv.addWidget(QLabel("Fim:"),    0, 2); hv.addWidget(self.de_fim, 0, 3)

        
        hv.addWidget(self.global_bar,   1, 0, 1, 4)

        # Linha 2: botões (Exportar ao lado de Aplicar, mesmo padrão)
        hv.addWidget(self.btn_refresh,  2, 0, 1, 1)
        hv.addWidget(self.btn_apply,    2, 1, 1, 1)
        hv.addWidget(self.btn_export,   2, 2, 1, 1)

        # dá respiro na última coluna e evita o botão sumir em telas estreitas
        hv.setColumnStretch(3, 1)

        root.addWidget(head)

        # Tabs raiz
        self.tabs = QTabWidget(); root.addWidget(self.tabs, 1)

        # ---- Combustível
        self.tab_comb = QWidget(); v1 = QVBoxLayout(self.tab_comb)
        top_line = QHBoxLayout()
        top_line.addWidget(QLabel("Fonte:"))
        self.cb_fonte = QComboBox(); self.cb_fonte.addItems(["Atual (por Placa)","Histórico"])
        top_line.addWidget(self.cb_fonte); top_line.addStretch(1)
        self.lbl_info_comb = QLabel("")
        top_line.addWidget(self.lbl_info_comb)
        v1.addLayout(top_line)

        self.tabs_comb = QTabWidget()
        # Atual (por placa)
        self.tbl_atual = self._mk_table(["Responsável","Placa","Modelo","Fabricante","Cidade/UF",
                                        "Limite Atual (R$)","Compras (R$)","Saldo (R$)","Limite Próx. (R$)","% Saldo","PLACA_N"])
        self.tabs_comb.addTab(self._wrap(self.tbl_atual), "Atual • por Placa")

        # Histórico: por responsável e por placa
        self.tbl_hist_resp = self._mk_table(["Responsável","Abastecimentos","Litros","Custo (R$)","R$/L"])
        self.tbl_hist_placa = self._mk_table(["Placa","Abastecimentos","Litros","Custo (R$)","R$/L"])
        self.tabs_comb.addTab(self._wrap(self.tbl_hist_resp), "Histórico • por Responsável")
        self.tabs_comb.addTab(self._wrap(self.tbl_hist_placa), "Histórico • por Placa")

        # Detalhe fixo combustível
        self.tbl_det_comb = self._mk_table(["DT","Responsável","Placa","Combustível","Litros","R$/L","Valor (R$)","Estabelecimento","Cidade/UF"])
        self.tabs_comb.addTab(self._wrap(self.tbl_det_comb), "Detalhe (fixo)")

        v1.addWidget(self.tabs_comb, 1)
        self.tabs.addTab(self.tab_comb, "Combustível")

        # ---- Multas
        self.tab_multas = QWidget(); v2 = QVBoxLayout(self.tab_multas)
        self.lbl_info_mult = QLabel("Canceladas excluídas.")
        v2.addWidget(self.lbl_info_mult)

        self.tabs_mult = QTabWidget()
        self.tbl_multas_resp = self._mk_table(["Responsável","Qtde Multas","Valor Total (R$)","Pontos Estimados","Valor Descontado (R$)","Valor Não Descontado (R$)","% Descontado"])
        self.tbl_multas_det = self._mk_table(["FLUIG","Responsável","Status","Data","Placa","Infração","Valor (R$)","Descontada?"])

        self.tabs_mult.addTab(self._wrap(self.tbl_multas_resp), "Resumo • por Responsável")
        self.tabs_mult.addTab(self._wrap(self.tbl_multas_det),  "Detalhe (fixo)")
        v2.addWidget(self.tabs_mult, 1)

        self.tabs.addTab(self.tab_multas, "Multas")

        # sinais
        self.btn_refresh.clicked.connect(self._load_all_and_apply)
        self.btn_apply.clicked.connect(self.apply_all)
        self.cb_fonte.currentIndexChanged.connect(self.apply_combustivel)
        self.global_bar.changed.connect(self.apply_all)
        self.de_ini.dateChanged.connect(self.apply_all)
        self.de_fim.dateChanged.connect(self.apply_all)
        self.btn_export.clicked.connect(self._export_current_table)

        # clique nas tabelas para preencher o detalhe fixo
        self.tbl_hist_resp.cellClicked.connect(self._on_click_hist_resp)
        self.tbl_hist_placa.cellClicked.connect(self._on_click_hist_placa)
        self.tbl_atual.cellClicked.connect(self._on_click_atual)
        self.tbl_multas_resp.cellClicked.connect(self._on_click_multas_resp)





        # ---------- helpers UI ----------
    def _mk_table(self, headers):
        t = QTableWidget()
        t.setAlternatingRowColors(True)
        t.setSortingEnabled(True)  # manter ligado; a gente desliga/religa no fill
        hdr = t.horizontalHeader()
        hdr.setSortIndicatorShown(True)
        hdr.setSectionsClickable(True)  # garante clique no título
        hdr.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        t.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        t.setColumnCount(len(headers))
        t.setHorizontalHeaderLabels(headers)

        hdr.sectionClicked.connect(lambda idx, tbl=t: self._toggle_sort(tbl, idx))
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

    # ---------- data ----------
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
            self.df_multas = self.hub.load_multas()
        except Exception as e:
            self.df_multas = pd.DataFrame()
            QMessageBox.warning(self, "Cenários Gerais", f"Erro ao carregar Multas: {e}")

    def _load_all_and_apply(self):
        self._load_all()
        self.apply_all()

    # ---------- período / filtro ----------
    def _period(self):
        q0, q1 = self.de_ini.date(), self.de_fim.date()
        a = pd.Timestamp(q0.year(), q0.month(), q0.day())
        b = pd.Timestamp(q1.year(), q1.month(), q1.day())
        if a > b: a, b = b, a
        return a, b

    def _global_values(self):
        try: return self.global_bar.values()
        except Exception: return []

    # ---------- apply ----------
    def apply_all(self):
        self.apply_combustivel()
        self.apply_multas()

    # Combustível
    def apply_combustivel(self):
        a, b = self._period()
        glb = self._global_values()
        fonte_idx = self.cb_fonte.currentIndex()
        self.lbl_info_comb.setText(f"Período: {a.strftime('%d/%m/%Y')} a {b.strftime('%d/%m/%Y')} — {'Atual' if fonte_idx==0 else 'Histórico'}")

        if fonte_idx == 0:
            d = self.df_comb_atual.copy()
            if not d.empty:
                d = df_apply_global_texts(d, glb)
                d_show = d.rename(columns={
                    "Responsavel":"Responsável","Placa":"Placa","Modelo":"Modelo","Fabricante":"Fabricante","Cidade/UF":"Cidade/UF",
                    "Limite Atual":"Limite Atual (R$)","Compras":"Compras (R$)","Saldo":"Saldo (R$)","Limite Próximo":"Limite Próx. (R$)","pctSaldo":"% Saldo"
                })
                d_show["% Saldo"] = d_show["% Saldo"].map(lambda x: f"{float(x or 0):.1f}%")
                self._fill(self.tbl_atual, d_show[["Responsável","Placa","Modelo","Fabricante","Cidade/UF","Limite Atual (R$)","Compras (R$)","Saldo (R$)","Limite Próx. (R$)","% Saldo","PLACA_N"]])
            else:
                self._fill(self.tbl_atual, pd.DataFrame())
            # limpa detalhe
            self._fill(self.tbl_det_comb, pd.DataFrame())
        else:
            d = self.df_comb_hist.copy()
            if not d.empty:
                d = d[(d["DT"].notna()) & (d["DT"] >= a) & (d["DT"] <= b)]
                d = df_apply_global_texts(d, glb)
                # Por responsável
                g1 = d.groupby(d["Responsavel"].astype(str).str.strip()).agg(
                    Abastecimentos=("VALOR_NUM","count"),
                    Litros=("LITROS_NUM","sum"),
                    Custo=("VALOR_NUM","sum")
                ).reset_index().rename(columns={"Responsavel":"Responsável", "Custo":"Custo (R$)"})
                g1["R$/L"] = (g1["Custo (R$)"] / g1["Litros"]).replace([pd.NA, pd.NaT, float("inf")], 0.0)
                self._fill(self.tbl_hist_resp, g1[["Responsável","Abastecimentos","Litros","Custo (R$)","R$/L"]].sort_values(["Custo (R$)","Abastecimentos"], ascending=[False, False]))

                # Por placa
                g2 = d.groupby(d["PLACA_N"].astype(str)).agg(
                    Abastecimentos=("VALOR_NUM","count"),
                    Litros=("LITROS_NUM","sum"),
                    Custo=("VALOR_NUM","sum")
                ).reset_index().rename(columns={"PLACA_N":"Placa", "Custo":"Custo (R$)"})
                g2["R$/L"] = (g2["Custo (R$)"] / g2["Litros"]).replace([pd.NA, pd.NaT, float("inf")], 0.0)
                self._fill(self.tbl_hist_placa, g2[["Placa","Abastecimentos","Litros","Custo (R$)","R$/L"]].sort_values(["Custo (R$)","Abastecimentos"], ascending=[False, False]))
            else:
                self._fill(self.tbl_hist_resp, pd.DataFrame()); self._fill(self.tbl_hist_placa, pd.DataFrame())
            # limpa detalhe
            self._fill(self.tbl_det_comb, pd.DataFrame())

    # Multas
    def apply_multas(self):
        a, b = self._period()
        glb = self._global_values()
        self.lbl_info_mult.setText(f"Canceladas excluídas. Período: {a.strftime('%d/%m/%Y')} a {b.strftime('%d/%m/%Y')}.")

        d = self.df_multas.copy()
        if d.empty:
            self._fill(self.tbl_multas_resp, pd.DataFrame()); self._fill(self.tbl_multas_det, pd.DataFrame()); return

        d = df_apply_global_texts(d, glb)
        dm = d[(d["DT_M"].notna()) & (d["DT_M"] >= a) & (d["DT_M"] <= b)]

        if dm.empty:
            self._fill(self.tbl_multas_resp, pd.DataFrame()); self._fill(self.tbl_multas_det, pd.DataFrame()); return

        grp = dm.groupby(dm["Responsavel"].astype(str).str.strip())
        resumo = pd.DataFrame({
            "Responsável": grp.apply(lambda g: g.name).reset_index(drop=True),
            "Qtde Multas": grp.size().reset_index(drop=True),
            "Valor Total (R$)": grp["VALOR_NUM"].sum().reset_index(drop=True),
            "Pontos Estimados": grp["VALOR_NUM"].apply(lambda s: sum(_guess_points(v) for v in s)).reset_index(drop=True),
            "Valor Descontado (R$)": grp.apply(lambda g: float(g.loc[g["DESCONTADA"],"VALOR_NUM"].sum())).reset_index(drop=True),
        })
        resumo["Valor Não Descontado (R$)"] = resumo["Valor Total (R$)"] - resumo["Valor Descontado (R$)"]
        resumo["% Descontado"] = resumo.apply(lambda r: (100.0*r["Valor Descontado (R$)"]/r["Valor Total (R$)"]) if r["Valor Total (R$)"]>0 else 0.0, axis=1)

        self._fill(self.tbl_multas_resp, resumo.sort_values(["Valor Total (R$)","Qtde Multas"], ascending=[False, False]))

        # detalhe padrão: todas as multas do período
        det = dm.copy().rename(columns={
            "Infracao":"Infração","VALOR_NUM":"Valor (R$)"
        })
        det["Data"] = det["DT_M"].dt.strftime("%d/%m/%Y").fillna(det["Data_raw"])
        det["Descontada?"] = det["DESCONTADA"].map(lambda b: "Sim" if bool(b) else "Não")
        self._fill(self.tbl_multas_det, det[["FLUIG","Responsavel","Status","Data","Placa","Infração","Valor (R$)","Descontada?"]].sort_values(["Data","FLUIG"]))


    def _fill(self, tbl: QTableWidget, df: pd.DataFrame):
        tbl.setSortingEnabled(False)  # evita reordenação durante o preenchimento

        if df is None or df.empty:
            # Mantém headers atuais se possível
            rows = 0
            headers = [tbl.horizontalHeaderItem(i).text() for i in range(tbl.columnCount())] if tbl.columnCount() else []
            if not headers and df is not None and not df.empty:
                headers = list(df.columns)
            tbl.clear()
            if headers:
                tbl.setColumnCount(len(headers))
                tbl.setHorizontalHeaderLabels(headers)
            tbl.setRowCount(rows)
            tbl.setSortingEnabled(True)
            return

        headers = list(df.columns)
        tbl.clear()
        tbl.setColumnCount(len(headers))
        tbl.setHorizontalHeaderLabels(headers)
        tbl.setRowCount(len(df))

        # Identifica colunas numéricas por convenção do header
        money_cols = {i for i, c in enumerate(headers) if "R$" in c}
        pct_cols   = {i for i, c in enumerate(headers) if "%" in c}
        num_cols   = {i for i, c in enumerate(headers) if c in ("Litros", "R$/L")}

        for i, (_, r) in enumerate(df.iterrows()):
            for j, c in enumerate(headers):
                v = r[c]
                # Formatação visual
                if j in money_cols:
                    s = _fmt_money(v)
                elif j in pct_cols:
                    try:
                        s = f"{float(v or 0):.1f}%"
                    except Exception:
                        s = f"{_fmt_num(v)}%"
                elif j in num_cols:
                    s = _fmt_num(v)
                else:
                    s = "" if pd.isna(v) else str(v)

                # Item
                if j in money_cols or j in num_cols or j in pct_cols:
                    # tenta extrair valor numérico para ordenação correta
                    numv = None
                    try:
                        numv = float(v)
                    except Exception:
                        try:
                            numv = float(_num(v))
                        except Exception:
                            numv = None
                    it = NumericItem(s, numv)
                else:
                    it = QTableWidgetItem(s)
                    it.setFlags(it.flags() & ~Qt.ItemFlag.ItemIsEditable)

                tbl.setItem(i, j, it)

        tbl.resizeColumnsToContents()
        tbl.horizontalHeader().setStretchLastSection(True)
        tbl.setSortingEnabled(True)   # reativa; clique no título alterna asc/desc


    def _export_current_table(self):
        tbl = self._current_table_widget()
        if tbl is None:
            QMessageBox.information(self, "Exportar", "Nada para exportar nesta aba.")
            return

        # Extrai cabeçalhos
        headers = [tbl.horizontalHeaderItem(i).text() for i in range(tbl.columnCount())]

        # Constrói DataFrame a partir da grade visível
        rows = []
        for i in range(tbl.rowCount()):
            row = []
            for j in range(tbl.columnCount()):
                it = tbl.item(i, j)
                row.append("" if it is None else it.text())
            rows.append(row)
        df = pd.DataFrame(rows, columns=headers)

        # Detecta colunas numéricas por header e normaliza valores para tipos numéricos
        money_cols = [h for h in headers if "R$" in h]
        pct_cols   = [h for h in headers if "%" in h]
        # inclui colunas com nomes que sabemos que são numéricas
        numeric_named = [h for h in headers if h in ("Litros", "R$/L", "Abastecimentos", "Qtde Multas", "Pontos Estimados",
                                                    "Valor Total (R$)", "Valor Descontado (R$)", "Valor Não Descontado (R$)")]
        numeric_cols = list(set(money_cols + pct_cols + numeric_named))

        def _clean_num(x: str):
            # remove símbolos e normaliza decimal para ponto
            s = str(x or "").strip()
            s = s.replace("R$", "").replace("%", "").strip()
            # troca separadores brasileiros para formato machine
            s = s.replace(".", "").replace(",", ".")
            # evita strings vazias
            try:
                return float(s)
            except Exception:
                try:
                    return float(_num(s))
                except Exception:
                    return None

        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].map(_clean_num)

        # Dialogo para salvar
        p, _ = QFileDialog.getSaveFileName(self, "Exportar", "export.xlsx", "Excel (*.xlsx);;CSV (*.csv)")
        if not p:
            return

        try:
            ext = os.path.splitext(p)[1].lower()
            if ext == ".csv":
                df.to_csv(p, index=False, encoding="utf-8-sig")
            else:
                with pd.ExcelWriter(p, engine="openpyxl") as w:
                    # Garante que o Pandas não injete tipos errados
                    df.to_excel(w, index=False, sheet_name="export")
            QMessageBox.information(self, "Exportar", "Arquivo exportado com sucesso.")
        except Exception as e:
            QMessageBox.critical(self, "Exportar", f"Falha ao exportar:\n{e}")



    def _current_table_widget(self) -> QTableWidget | None:
        # retorna a QTableWidget da sub-aba visível (Combustível ou Multas)
        curr = self.tabs.currentWidget()
        if curr is self.tab_comb:
            sub = self.tabs_comb.currentIndex()
            return {
                0: self.tbl_atual,
                1: self.tbl_hist_resp,
                2: self.tbl_hist_placa,
                3: self.tbl_det_comb
            }.get(sub)
        if curr is self.tab_multas:
            sub = self.tabs_mult.currentIndex()
            return {
                0: self.tbl_multas_resp,
                1: self.tbl_multas_det
            }.get(sub)
        return None

    # ---------- handlers de clique para preencher aba fixa ----------
    def _on_click_hist_resp(self, row, col):
        # filtra transações históricas pelo responsável clicado
        try:
            key = self.tbl_hist_resp.item(row, 0).text()
        except Exception:
            return
        a, b = self._period()
        d = self.df_comb_hist.copy()
        d = d[(d["DT"].notna()) & (d["DT"] >= a) & (d["DT"] <= b)]
        d = d[d["Responsavel"].astype(str).str.strip() == key]
        det = d.copy()
        det["DT_STR"] = det["DT"].dt.strftime("%d/%m/%Y %H:%M")
        show = det.rename(columns={
            "DT_STR":"DT","Responsavel":"Responsável","PLACA":"Placa","COMBUSTIVEL":"Combustível",
            "LITROS_NUM":"Litros","VL_LITRO_NUM":"R$/L","VALOR_NUM":"Valor (R$)","ESTABELECIMENTO":"Estabelecimento","CIDADE_UF":"Cidade/UF"
        })[["DT","Responsável","Placa","Combustível","Litros","R$/L","Valor (R$)","Estabelecimento","Cidade/UF"]]
        self.tabs_comb.setCurrentWidget(self._wrap(self.tbl_det_comb))  # garante seleção da aba fixa
        self.tabs_comb.setCurrentIndex(3)
        self._fill(self.tbl_det_comb, show.sort_values("DT"))

    def _on_click_hist_placa(self, row, col):
        try:
            key = self.tbl_hist_placa.item(row, 0).text()
        except Exception:
            return
        a, b = self._period()
        d = self.df_comb_hist.copy()
        d = d[(d["DT"].notna()) & (d["DT"] >= a) & (d["DT"] <= b)]
        d = d[d["PLACA_N"].astype(str) == key]
        det = d.copy()
        det["DT_STR"] = det["DT"].dt.strftime("%d/%m/%Y %H:%M")
        show = det.rename(columns={
            "DT_STR":"DT","Responsavel":"Responsável","PLACA":"Placa","COMBUSTIVEL":"Combustível",
            "LITROS_NUM":"Litros","VL_LITRO_NUM":"R$/L","VALOR_NUM":"Valor (R$)","ESTABELECIMENTO":"Estabelecimento","CIDADE_UF":"Cidade/UF"
        })[["DT","Responsável","Placa","Combustível","Litros","R$/L","Valor (R$)","Estabelecimento","Cidade/UF"]]
        self.tabs_comb.setCurrentIndex(3)
        self._fill(self.tbl_det_comb, show.sort_values("DT"))

    def _on_click_atual(self, row, col):
        # mostra histórico da placa clicada
        try:
            placa_n = self.tbl_atual.item(row, self.tbl_atual.columnCount()-1).text()  # PLACA_N está na última coluna
        except Exception:
            return
        a, b = self._period()
        d = self.df_comb_hist.copy()
        d = d[(d["DT"].notna()) & (d["DT"] >= a) & (d["DT"] <= b)]
        d = d[d["PLACA_N"].astype(str) == placa_n]
        det = d.copy()
        det["DT_STR"] = det["DT"].dt.strftime("%d/%m/%Y %H:%M")
        show = det.rename(columns={
            "DT_STR":"DT","Responsavel":"Responsável","PLACA":"Placa","COMBUSTIVEL":"Combustível",
            "LITROS_NUM":"Litros","VL_LITRO_NUM":"R$/L","VALOR_NUM":"Valor (R$)","ESTABELECIMENTO":"Estabelecimento","CIDADE_UF":"Cidade/UF"
        })[["DT","Responsável","Placa","Combustível","Litros","R$/L","Valor (R$)","Estabelecimento","Cidade/UF"]]
        self.tabs_comb.setCurrentIndex(3)
        self._fill(self.tbl_det_comb, show.sort_values("DT"))

    def _on_click_multas_resp(self, row, col):
        try:
            key = self.tbl_multas_resp.item(row, 0).text()
        except Exception:
            return
        a, b = self._period()
        dm = self.df_multas.copy()
        dm = dm[(dm["DT_M"].notna()) & (dm["DT_M"] >= a) & (dm["DT_M"] <= b)]
        dm = dm[dm["Responsavel"].astype(str).str.strip() == key]
        det = dm.copy().rename(columns={"Infracao":"Infração","VALOR_NUM":"Valor (R$)"})
        det["Data"] = det["DT_M"].dt.strftime("%d/%m/%Y").fillna(det["Data_raw"])
        det["Descontada?"] = det["DESCONTADA"].map(lambda b: "Sim" if bool(b) else "Não")
        show = det[["FLUIG","Responsavel","Status","Data","Placa","Infração","Valor (R$)","Descontada?"]].sort_values(["Data","FLUIG"])
        self.tabs_mult.setCurrentIndex(1)
        self._fill(self.tbl_multas_det, show)
