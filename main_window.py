# main_window.py
import os, re
from multas import InfraMultasWindow
import pandas as pd
from pathlib import Path

from cenarios_gerais import CenariosGeraisWindow  

from PyQt6.QtWidgets import (
    QApplication, QWidget, QMainWindow, QTabWidget, QVBoxLayout, QFrame, QHBoxLayout,
    QLabel, QPushButton, QGridLayout, QLineEdit, QComboBox, QMessageBox,
    QFileDialog, QTableWidget, QTableWidgetItem, QHeaderView, QDateEdit, QScrollArea,
    QDialog
)
from PyQt6.QtCore import Qt, QDate
from PyQt6.QtGui import QFont, QColor

from gestao_frota_single import (
    BaseTab, MODULES, DATE_FORMAT, DATE_COLS, STATUS_COLOR,
    cfg_get, cfg_set, cfg_all
)
from utils import apply_shadow, CheckableComboBox, ensure_status_cols, df_apply_global_texts
from multas import InfraMultasWindow
from relatorios import RelatorioWindow
from combustivel import CombustivelMenu, CombustivelWindow


def _collect_alertas(df):
    """
    Monta as linhas (FLUIG, INFRATOR, PLACA, ETAPA, DATA, STATUS)
    usando SOMENTE as colunas listadas em constants.DATE_COLS.
    """
    linhas = []
    if df.empty:
        return linhas
    use_cols = [c for c in DATE_COLS if c in df.columns]  # garante só as 3
    for _, row in df.iterrows():
        fluig = str(row.get("FLUIG", "")).strip()
        infr = str(row.get("INFRATOR", "") or row.get("NOME", "")).strip()
        placa = str(row.get("PLACA", "")).strip()
        for col in use_cols:
            dt = str(row.get(col, "")).strip()
            st = str(row.get(f"{col}_STATUS", "")).strip()
            if dt or st:
                linhas.append([fluig, infr, placa, col, dt, st])
    return linhas








class AlertsTab(QWidget):
    def __init__(self):
        super().__init__()
        self.df_original = pd.DataFrame()
        self.df_filtrado = pd.DataFrame()
        self.mode_filtros = {}
        self.multi_filtros = {}
        self.global_boxes = []

        root = QVBoxLayout(self)

        # Header
        header = QFrame(); header.setObjectName("card"); apply_shadow(header, radius=18)
        hv = QVBoxLayout(header)
        actions = QHBoxLayout()
        btn_reload = QPushButton("Recarregar"); btn_reload.clicked.connect(self.recarregar)
        btn_clear  = QPushButton("Limpar filtros"); btn_clear.clicked.connect(self.limpar_filtros)
        btn_export = QPushButton("Exportar Excel"); btn_export.clicked.connect(self.exportar_excel)
        actions.addWidget(btn_reload); actions.addWidget(btn_clear); actions.addStretch(1); actions.addWidget(btn_export)
        hv.addLayout(actions)

        # Filtro global (+)
        row_global = QHBoxLayout()
        row_global.addWidget(QLabel("Filtro global:"))
        def _add_box():
            le = QLineEdit()
            le.setPlaceholderText("Digite para filtrar em TODAS as colunas…")
            le.textChanged.connect(self._apply_filters)
            self.global_boxes.append(le)
            row_global.addWidget(le, 1)
        _add_box()
        btn_plus = QPushButton("+"); btn_plus.setFixedWidth(28); btn_plus.clicked.connect(_add_box)
        row_global.addWidget(btn_plus)
        hv.addLayout(row_global)

        # filtros por coluna
        self.filters_scroll = QScrollArea(); self.filters_scroll.setWidgetResizable(True)
        self.filters_host = QWidget(); self.filters_grid = QGridLayout(self.filters_host)
        self.filters_grid.setContentsMargins(0,0,0,0); self.filters_grid.setHorizontalSpacing(12); self.filters_grid.setVerticalSpacing(8)
        self.filters_scroll.setWidget(self.filters_host)
        hv.addWidget(self.filters_scroll)

        root.addWidget(header)

        # Tabela
        table_card = QFrame(); table_card.setObjectName("glass")
        apply_shadow(table_card, radius=18, blur=60, color=QColor(0,0,0,80))
        tv = QVBoxLayout(table_card)
        self.tabela = QTableWidget()
        self.tabela.setAlternatingRowColors(True)
        self.tabela.setSortingEnabled(True)
        self.tabela.horizontalHeader().setSortIndicatorShown(True)
        self.tabela.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.tabela.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        tv.addWidget(self.tabela)
        root.addWidget(table_card)

        self.recarregar()

    def _load_df(self):
        path = cfg_get("geral_multas_csv")
        if not path or not os.path.exists(path):
            QMessageBox.warning(self, "Alertas", "Caminho do GERAL_MULTAS.csv não configurado.")
            return pd.DataFrame()

        base = ensure_status_cols(pd.read_csv(path, dtype=str).fillna(""), csv_path=path)

        rows = []
        use_cols = [c for c in DATE_COLS if c in base.columns]  # só DATA INDICAÇÃO / BOLETO / SGU
        for _, r in base.iterrows():
            fluig = str(r.get("FLUIG", "")).strip()
            infr  = str(r.get("INFRATOR", "") or r.get("NOME", "")).strip()
            placa = str(r.get("PLACA", "")).strip()
            orgao = str(r.get("ORGÃO", "") or r.get("ORG", "") or r.get("ORGAO", "")).strip()  # 👈 novo

            for col in use_cols:
                dt = str(r.get(col, "")).strip()
                st = str(r.get(f"{col}_STATUS", "")).strip()
                if dt or st:
                    rows.append([fluig, infr, placa, orgao, col, dt, st])

        return pd.DataFrame(rows, columns=["FLUIG","INFRATOR","PLACA","ORGÃO","ETAPA","DATA","STATUS"])

    def recarregar(self):
        self.df_original = self._load_df()
        self.df_filtrado = self.df_original.copy()
        self._montar_filtros()
        self._fill_table(self.df_filtrado)

    def _montar_filtros(self):
        while self.filters_grid.count():
            it = self.filters_grid.takeAt(0)
            if it.widget(): it.widget().setParent(None)
        self.mode_filtros.clear(); self.multi_filtros.clear()

        cols = list(self.df_original.columns)
        for i, col in enumerate(cols):
            wrap = QFrame(); v = QVBoxLayout(wrap)
            lab = QLabel(col); lab.setObjectName("colTitle"); v.addWidget(lab)
            line = QHBoxLayout()
            mode = QComboBox(); mode.addItems(["Todos","Excluir vazios","Somente vazios"]); mode.currentTextChanged.connect(self._apply_filters)
            ms = CheckableComboBox(self.df_original[col].dropna().astype(str).unique()); ms.changed.connect(self._apply_filters)
            line.addWidget(mode); line.addWidget(ms); v.addLayout(line)
            self.mode_filtros[col] = mode; self.multi_filtros[col] = ms
            self.filters_grid.addWidget(wrap, i//3, i%3)

    def limpar_filtros(self):
        for le in self.global_boxes:
            le.blockSignals(True); le.clear(); le.blockSignals(False)
        for mode in self.mode_filtros.values():
            mode.blockSignals(True); mode.setCurrentIndex(0); mode.blockSignals(False)
        for ms in self.multi_filtros.values():
            vals = [ms.itemText(i) for i in range(ms.count())]
            ms.set_values(vals)
        self._apply_filters()

    def _apply_filters(self):
        df = self.df_original.copy()
        texts = [le.text() for le in self.global_boxes if le.text().strip()]
        df = df_apply_global_texts(df, texts)
        for col in df.columns:
            mode = self.mode_filtros[col].currentText()
            if mode == "Excluir vazios":
                df = df[df[col].astype(str).str.strip() != ""]
            elif mode == "Somente vazios":
                df = df[df[col].astype(str).str.strip() == ""]
            sels = [s for s in self.multi_filtros[col].selected_values() if s]
            if sels:
                df = df[df[col].astype(str).isin(sels)]
        self.df_filtrado = df
        self._fill_table(self.df_filtrado)

        # atualizar listas mantendo seleção
        for col in self.df_filtrado.columns:
            ms = self.multi_filtros[col]
            current_sel = ms.selected_values()
            ms.set_values(self.df_filtrado[col].dropna().astype(str).unique())
            if current_sel:
                for i in range(ms.count()):
                    if ms.itemText(i) in current_sel:
                        idx = ms.model().index(i, 0)
                        ms.model().setData(idx, Qt.CheckState.Checked, Qt.ItemDataRole.CheckStateRole)
                ms._update_text()

    def _fill_table(self, df):
        headers = list(df.columns)
        self.tabela.clear()
        self.tabela.setColumnCount(len(headers))
        self.tabela.setHorizontalHeaderLabels(headers)
        self.tabela.setRowCount(len(df))
        for i, (_, r) in enumerate(df.iterrows()):
            for j, col in enumerate(headers):
                val = "" if pd.isna(r[col]) else str(r[col])
                it = QTableWidgetItem(val)
                it.setFlags(it.flags() & ~Qt.ItemFlag.ItemIsEditable)
                if col.upper() == "STATUS":
                    st = val.strip()
                    if st in STATUS_COLOR:
                        bg = STATUS_COLOR[st]
                        it.setBackground(bg)
                        yiq = (bg.red()*299 + bg.green()*587 + bg.blue()*114)/1000
                        it.setForeground(QColor("#000000" if yiq >= 160 else "#FFFFFF"))
                self.tabela.setItem(i, j, it)
        self.tabela.resizeColumnsToContents()
        self.tabela.horizontalHeader().setStretchLastSection(True)
        self.tabela.resizeRowsToContents()

    def exportar_excel(self):
        try:
            self.df_filtrado.to_excel("alertas_filtrado.xlsx", index=False)
            QMessageBox.information(self, "Exportado", "alertas_filtrado.xlsx criado.")
        except Exception as e:
            QMessageBox.critical(self, "Erro", str(e))


class _AlertasDialog(QDialog):
    """
    Dialog de Alertas com tabela organizada e o padrão de filtros:
    - 1 campo de texto global (com botão +) que filtra em TODAS as colunas
    - por coluna: modo (Todos/Excluir vazios/Somente vazios) + multiseleção
    - cores de STATUS preservadas
    """
    def __init__(self, parent, df):
        super().__init__(parent)
        self.setWindowTitle("Alertas de Datas")
        self.resize(1100, 640)

        self.df_original = df.copy()
        # Reduz às colunas de interesse do alerta
        cols_fixas = ["FLUIG", "INFRATOR", "PLACA", "ETAPA", "DATA", "STATUS"]
        if not set(cols_fixas).issubset(self.df_original.columns):
            # Se vier a planilha bruta, montamos as linhas a partir de DATE_COLS
            rows = []
            use_cols = [c for c in DATE_COLS if c in self.df_original.columns]
            for _, row in self.df_original.iterrows():
                fluig = str(row.get("FLUIG", "")).strip()
                infr  = str(row.get("INFRATOR", "") or row.get("NOME", "")).strip()
                placa = str(row.get("PLACA", "")).strip()
                for col in use_cols:
                    dt = str(row.get(col, "")).strip()
                    st = str(row.get(f"{col}_STATUS", "")).strip()
                    if dt or st:
                        rows.append([fluig, infr, placa, col, dt, st])
            self.df_original = pd.DataFrame(rows, columns=cols_fixas)

        self.df_filtrado = self.df_original.copy()

        # ===== UI =====
        root = QVBoxLayout(self)

        # Card dos filtros (padrão)
        header = QFrame(); header.setObjectName("card"); apply_shadow(header, radius=18)
        hv = QVBoxLayout(header)

        # Filtro global com "+"
        row_global = QHBoxLayout()
        row_global.addWidget(QLabel("Filtro global:"))
        self.global_boxes = []
        def add_box():
            le = QLineEdit()
            le.setPlaceholderText("Digite para filtrar em todas as colunas…")
            le.textChanged.connect(self._apply_filters)
            row_global.addWidget(le, 1)
            self.global_boxes.append(le)
        add_box()
        btn_plus = QPushButton("+"); btn_plus.setFixedWidth(28); btn_plus.clicked.connect(add_box)
        row_global.addWidget(btn_plus)
        hv.addLayout(row_global)

        # Filtros por coluna (modo + multiseleção)
        from utils import CheckableComboBox  # já existe
        self.mode_filtros = {}
        self.multi_filtros = {}

        scroll = QScrollArea(); scroll.setWidgetResizable(True)
        inner = QWidget(); gl = QGridLayout(inner); gl.setContentsMargins(0,0,0,0); gl.setHorizontalSpacing(12); gl.setVerticalSpacing(8)
        cols = list(self.df_original.columns)

        for i, col in enumerate(cols):
            wrap = QFrame(); wv = QVBoxLayout(wrap)
            lab = QLabel(col); lab.setObjectName("colTitle"); wv.addWidget(lab)

            line = QHBoxLayout()
            mode = QComboBox(); mode.addItems(["Todos","Excluir vazios","Somente vazios"])
            mode.currentTextChanged.connect(self._apply_filters)
            ms = CheckableComboBox(self.df_original[col].dropna().astype(str).unique())
            ms.changed.connect(self._apply_filters)
            line.addWidget(mode); line.addWidget(ms)
            wv.addLayout(line)

            self.mode_filtros[col] = mode
            self.multi_filtros[col] = ms
            gl.addWidget(wrap, i//3, i%3)

        host = QWidget(); host.setLayout(gl)
        scroll.setWidget(host)
        hv.addWidget(scroll)
        root.addWidget(header)

        # Tabela
        table_card = QFrame(); table_card.setObjectName("glass")
        apply_shadow(table_card, radius=18, blur=60, color=QColor(0,0,0,80))
        tv = QVBoxLayout(table_card)
        self.tabela = QTableWidget()
        self.tabela.setAlternatingRowColors(True)
        self.tabela.setSortingEnabled(True)
        self.tabela.horizontalHeader().setSortIndicatorShown(True)
        self.tabela.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.tabela.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        tv.addWidget(self.tabela)

        # Botões
        bar = QHBoxLayout()
        btn_limpar = QPushButton("Limpar filtros"); btn_limpar.clicked.connect(self._limpar_filtros)
        btn_fechar = QPushButton("Fechar"); btn_fechar.clicked.connect(self.accept)
        bar.addWidget(btn_limpar); bar.addStretch(1); bar.addWidget(btn_fechar)
        tv.addLayout(bar)

        root.addWidget(table_card)

        self._apply_filters()

    def _limpar_filtros(self):
        for le in self.global_boxes:
            le.blockSignals(True); le.clear(); le.blockSignals(False)
        for mode in self.mode_filtros.values():
            mode.blockSignals(True); mode.setCurrentIndex(0); mode.blockSignals(False)
        for ms in self.multi_filtros.values():
            vals = [ms.itemText(i) for i in range(ms.count())]
            ms.set_values(vals)
        self._apply_filters()

    def _apply_filters(self):
        from utils import df_apply_global_texts
        df = self.df_original.copy()

        # Global (todas as colunas) — suporta múltiplas caixas se clicar +
        texts = [le.text() for le in self.global_boxes if le.text().strip()]
        df = df_apply_global_texts(df, texts)

        # Por coluna: modo + multiseleção
        for col in df.columns:
            mode = self.mode_filtros[col].currentText()
            if mode == "Excluir vazios":
                df = df[df[col].astype(str).str.strip() != ""]
            elif mode == "Somente vazios":
                df = df[df[col].astype(str).str.strip() == ""]
            sels = [s for s in self.multi_filtros[col].selected_values() if s]
            if sels:
                df = df[df[col].astype(str).isin(sels)]

        self.df_filtrado = df
        self._fill_table()

    def _fill_table(self):
        headers = list(self.df_filtrado.columns)
        self.tabela.clear()
        self.tabela.setColumnCount(len(headers))
        self.tabela.setHorizontalHeaderLabels(headers)
        self.tabela.setRowCount(len(self.df_filtrado))

        for i, (_, r) in enumerate(self.df_filtrado.iterrows()):
            for j, col in enumerate(headers):
                val = "" if pd.isna(r[col]) else str(r[col])
                it = QTableWidgetItem(val)
                it.setFlags(it.flags() & ~Qt.ItemFlag.ItemIsEditable)
                # Pintar STATUS
                if col.upper() == "STATUS":
                    st = val.strip()
                    if st in STATUS_COLOR:
                        bg = STATUS_COLOR[st]
                        it.setBackground(bg)
                        yiq = (bg.red()*299 + bg.green()*587 + bg.blue()*114)/1000
                        it.setForeground(QColor("#000000" if yiq >= 160 else "#FFFFFF"))
                self.tabela.setItem(i, j, it)

        self.tabela.resizeColumnsToContents()
        self.tabela.horizontalHeader().setStretchLastSection(True)
        self.tabela.resizeRowsToContents()


def _parse_dt(val):
    s = str(val).strip()
    if not s:
        return pd.NaT
    for fmt in ("%d/%m/%Y","%d-%m-%Y","%Y-%m-%d","%Y/%m/%d"):
        try:
            return pd.to_datetime(s, format=fmt, dayfirst=True, errors="raise")
        except:
            pass
    return pd.to_datetime(s, dayfirst=True, errors="coerce")


def _parse_money(s):
    """
    Converte números em pt-BR para float:
    - "R$ 1.234,56" -> 1234.56
    - "6,590" -> 6.59
    - "1,234.56" (se vier assim) -> 1234.56
    """
    if s is None:
        return 0.0
    txt = str(s).strip()
    if not txt:
        return 0.0
    import re
    txt = re.sub(r"[^\d.,-]", "", txt)  # remove tudo menos dígito, vírgula, ponto, sinal

    # Sem separadores -> só número inteiro
    if ("," not in txt) and ("." not in txt):
        try:
            return float(txt)
        except:
            return 0.0

    if "," in txt and "." in txt:
        # Heurística: o último símbolo define o separador decimal
        last_comma = txt.rfind(",")
        last_dot = txt.rfind(".")
        if last_comma > last_dot:
            # vírgula é decimal; ponto é milhar
            txt = txt.replace(".", "").replace(",", ".")
        else:
            # ponto é decimal; vírgula é milhar
            txt = txt.replace(",", "")
    else:
        # só vírgula -> decimal BR
        if "," in txt:
            txt = txt.replace(",", ".")
        # só ponto -> já é decimal internacional

    try:
        return float(txt)
    except:
        return 0.0
    


    




class CenarioGeralWindow(QWidget):
    """
    Painel consolidado (mini-BI):
    - Base: Fase Pastores, complementada por Detalhamento e Condutor Identificado
    - Filtro de tempo por Data de Infração
    - Filtros por coluna (modelo igual ao das outras telas): texto ao digitar, multiseleção, modo vazio/cheio
    - Abas: GERAL, FLUIG, DATA, NOME, TIPO, PLACA, REGIÃO/IGREJA
    """
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Cenário Geral")
        self.resize(1280, 860)

        self.det_path = cfg_get("detalhamento_path")
        self.past_path = cfg_get("pastores_file")
        self.cond_path = cfg_get("condutor_identificado_path")

        self._load_data()
        self._build_ui()
        self._apply_filters_and_refresh()

    # ----- carga e preparação
    def _read_excel_safely(self, path):
        if not path or not os.path.exists(path):
            return pd.DataFrame()
        try:
            return pd.read_excel(path, dtype=str).fillna("")
        except Exception as e:
            QMessageBox.warning(self, "Aviso", f"Erro abrindo {os.path.basename(path)}: {e}")
            return pd.DataFrame()

    def _norm_cols(self, df, rename_map):
        if df.empty: return df
        use = {src: dst for src, dst in rename_map.items() if src in df.columns}
        return df.rename(columns=use)

    def _load_data(self):
        det = self._read_excel_safely(self.det_path)
        past = self._read_excel_safely(self.past_path)
        cond = self._read_excel_safely(self.cond_path)

        past = self._norm_cols(past, {
            "Nº Fluig":"FLUIG","UF":"UF","Placa":"PLACA","Bloco":"BLOCO","Região":"REGIAO","Igreja":"IGREJA",
            "Nome":"NOME","CPF":"CPF","Título":"TITULO","Infração":"INFRACAO","AIT":"AIT",
            "Data Infração":"DATA_INFRACAO","Hora Infração":"HORA_INFRACAO","Data Limite":"DATA_LIMITE",
            "Local":"LOCAL","Data Solicitação":"DATA_SOLICITACAO","Data Pastores":"DATA_PASTORES",
            "Localização":"LOCALIZACAO","Tipo":"TIPO","Qtd":"QTD","Valor Total":"VALOR_TOTAL"
        })
        det = self._norm_cols(det, {
            "Nº Fluig":"FLUIG","Status":"STATUS","UF":"UF","Placa":"PLACA","Bloco":"BLOCO","Região":"REGIAO",
            "Igreja":"IGREJA","Nome":"NOME","CPF":"CPF","Título":"TITULO","Infração":"INFRACAO","AIT":"AIT",
            "Data Infração":"DATA_INFRACAO","Hora Infração":"HORA_INFRACAO","Data Limite":"DATA_LIMITE",
            "Local":"LOCAL","Data Solicitação":"DATA_SOLICITACAO","Valor Total":"VALOR_TOTAL"
        })
        cond = self._norm_cols(cond, {
            "Nº Fluig":"FLUIG","UF":"UF","Placa":"PLACA","Bloco":"BLOCO","Região":"REGIAO","Igreja":"IGREJA",
            "Título":"TITULO","Nome":"NOME","CPF":"CPF","Depto":"DEPTO","Nome Identificado":"NOME_IDENT",
            "CPF Identificado":"CPF_IDENT","SOL_TXT_AIT":"AIT","Função Identificado":"FUNCAO_IDENT",
            "Qtd":"QTD","Valor Total":"VALOR_TOTAL"
        })

        self.df_det = det.copy()
        self.df_past = past.copy()
        self.df_cond = cond.copy()

        det_keep = pd.DataFrame()
        if not det.empty:
            det_keep = det[["FLUIG","STATUS","DATA_INFRACAO","AIT","VALOR_TOTAL","NOME","PLACA"]].copy()

        cond_keep = pd.DataFrame()
        if not cond.empty:
            cols = [c for c in ["FLUIG","AIT","NOME_IDENT","CPF_IDENT","FUNCAO_IDENT"] if c in cond.columns]
            cond_keep = cond[cols].copy()

        base = past.copy()
        if "FLUIG" not in base.columns:
            base["FLUIG"] = ""

        if not det_keep.empty:
            det_keep = det_keep.add_prefix("DET_").rename(columns={"DET_FLUIG":"FLUIG"})
            base = base.merge(det_keep, on="FLUIG", how="left")
        if not cond_keep.empty:
            cond_keep = cond_keep.add_prefix("COND_").rename(columns={"COND_FLUIG":"FLUIG"})
            base = base.merge(cond_keep, on="FLUIG", how="left")

        def _pick(row, *cols):
            for c in cols:
                if c in row and str(row[c]).strip():
                    return str(row[c]).strip()
            return ""

        base["U_DATA_INFRACAO"] = base.apply(lambda r: _pick(r, "DATA_INFRACAO","DET_DATA_INFRACAO"), axis=1)
        base["U_AIT"] = base.apply(lambda r: _pick(r, "AIT","DET_AIT","COND_AIT"), axis=1)
        base["U_NOME"] = base.apply(lambda r: _pick(r, "NOME","DET_NOME","COND_NOME_IDENT"), axis=1)
        base["U_PLACA"] = base.apply(lambda r: _pick(r, "PLACA","DET_PLACA"), axis=1)
        base["U_STATUS"] = base.apply(lambda r: _pick(r, "DET_STATUS"), axis=1)
        base["U_INFRACAO"] = base.apply(lambda r: _pick(r, "INFRACAO"), axis=1)
        base["U_VALOR"] = base.apply(lambda r: _pick(r, "VALOR_TOTAL","DET_VALOR_TOTAL"), axis=1)

        base["DT_INF"] = base["U_DATA_INFRACAO"].map(_parse_dt)
        base["VALOR_NUM"] = base["U_VALOR"].map(_parse_money)
        self.df_base = base

        dates = sorted(list(base["DT_INF"].dropna().unique()))
        self._date_index = dates if dates else []
        self._date_min = min(dates) if dates else pd.Timestamp.today()
        self._date_max = max(dates) if dates else pd.Timestamp.today()

    # ----- UI
    def _build_ui(self):
        root = QVBoxLayout(self)

        # Título
        title_card = QFrame(); title_card.setObjectName("glass")
        apply_shadow(title_card, radius=20, blur=60, color=QColor(0, 0, 0, 60))
        tv = QVBoxLayout(title_card); tv.setContentsMargins(24, 24, 24, 24)
        t = QLabel("Cenário Geral"); t.setAlignment(Qt.AlignmentFlag.AlignCenter)
        t.setFont(QFont("Arial", 24, weight=QFont.Weight.Bold))
        tv.addWidget(t)
        root.addWidget(title_card)

        # KPIs + régua de tempo
        top_card = QFrame(); top_card.setObjectName("card"); apply_shadow(top_card, radius=18)
        top = QVBoxLayout(top_card); top.setContentsMargins(16,16,16,16)

        self.lbl_count_det = QLabel(); self.lbl_count_past = QLabel(); self.lbl_count_cond = QLabel()
        for lbl in (self.lbl_count_det, self.lbl_count_past, self.lbl_count_cond):
            lbl.setFont(QFont("Arial", 12, weight=QFont.Weight.Bold))
        row_counts = QHBoxLayout()
        row_counts.addWidget(QLabel("Qtd Detalhamento:")); row_counts.addWidget(self.lbl_count_det); row_counts.addSpacing(16)
        row_counts.addWidget(QLabel("Qtd Fase Pastores:")); row_counts.addWidget(self.lbl_count_past); row_counts.addSpacing(16)
        row_counts.addWidget(QLabel("Qtd Condutor Identificado:")); row_counts.addWidget(self.lbl_count_cond)
        row_counts.addStretch(1)
        top.addLayout(row_counts)

        # Régua de tempo
        self.de_start = QDateEdit(); self.de_end = QDateEdit()
        for de in (self.de_start, self.de_end):
            de.setCalendarPopup(True); de.setDisplayFormat(DATE_FORMAT)
            de.setMinimumDate(QDate(1752,9,14)); de.setSpecialValueText("")

        qmin = QDate(self._date_min.year, self._date_min.month, self._date_min.day) if isinstance(self._date_min, pd.Timestamp) else QDate.currentDate()
        qmax = QDate(self._date_max.year, self._date_max.month, self._date_max.day) if isinstance(self._date_max, pd.Timestamp) else QDate.currentDate()
        self.de_start.setDate(qmin); self.de_end.setDate(qmax)

        from PyQt6.QtWidgets import QSlider
        self.sl_start = QSlider(Qt.Orientation.Horizontal)
        self.sl_end = QSlider(Qt.Orientation.Horizontal)
        n = max(0, len(self._date_index)-1)
        for sl in (self.sl_start, self.sl_end):
            sl.setMinimum(0); sl.setMaximum(n); sl.setTickInterval(1); sl.setSingleStep(1); sl.setPageStep(1)
        self.sl_start.setValue(0); self.sl_end.setValue(n)

        row_time1 = QHBoxLayout()
        row_time1.addWidget(QLabel("Início:")); row_time1.addWidget(self.de_start); row_time1.addSpacing(10)
        row_time1.addWidget(QLabel("Fim:")); row_time1.addWidget(self.de_end); row_time1.addStretch(1)
        row_time2 = QHBoxLayout()
        row_time2.addWidget(self.sl_start); row_time2.addSpacing(8); row_time2.addWidget(self.sl_end)
        top.addLayout(row_time1); top.addLayout(row_time2)

        self.de_start.dateChanged.connect(self._on_dateedit_changed)
        self.de_end.dateChanged.connect(self._on_dateedit_changed)
        self.sl_start.valueChanged.connect(self._on_slider_changed)
        self.sl_end.valueChanged.connect(self._on_slider_changed)

        root.addWidget(top_card)

        # ===== Filtros por coluna (modelo "digitou, filtrou") =====
        filt_card = QFrame(); filt_card.setObjectName("card"); apply_shadow(filt_card, radius=18)
        fv = QVBoxLayout(filt_card); fv.setContentsMargins(12,12,12,12)
        self.filters_scroll = QScrollArea(); self.filters_scroll.setWidgetResizable(True)
        self.filters_host = QWidget(); self.filters_grid = QGridLayout(self.filters_host)
        self.filters_grid.setHorizontalSpacing(14); self.filters_grid.setVerticalSpacing(8)
        self.filters_scroll.setWidget(self.filters_host); fv.addWidget(self.filters_scroll)
        root.addWidget(filt_card)

        # colunas filtráveis
        self.filter_cols = [
            ("U_NOME","Nome"),
            ("U_PLACA","Placa"),
            ("U_INFRACAO","Infração"),
            ("U_STATUS","Status"),
            ("REGIAO","Região"),
            ("IGREJA","Igreja"),
            ("U_AIT","AIT"),
        ]
        self.mode_filtros = {}; self.multi_filtros = {}; self.text_filtros = {}
        self._mount_filters()

        # Abas
        self.tabs = QTabWidget()
        root.addWidget(self.tabs, 1)

        # GERAL
        self.tab_geral = QWidget(); vg = QVBoxLayout(self.tab_geral)
        self.tbl_geral = QTableWidget(); self._prep_table(self.tbl_geral, ["Nome","Qtde Multas","Placas distintas","Valor Total (R$)"])
        vg.addWidget(self.tbl_geral)

        # FLUIG
        self.tab_fluig = QWidget(); vf = QVBoxLayout(self.tab_fluig)
        self.tbl_fluig = QTableWidget(); self._prep_table(self.tbl_fluig, ["Status","Quantidade"])
        vf.addWidget(self.tbl_fluig)

        # DATA
        self.tab_data = QWidget(); vd = QVBoxLayout(self.tab_data)
        self.tbl_data = QTableWidget(); self._prep_table(self.tbl_data, ["Ano-Mês","Total no mês","Abertas (se houver status)"])
        vd.addWidget(self.tbl_data)

        # NOME
        self.tab_nome = QWidget(); vn = QVBoxLayout(self.tab_nome)
        row_nome = QHBoxLayout()
        row_nome.addWidget(QLabel("Nome:"))
        self.cb_nome = QComboBox(); self.cb_nome.currentTextChanged.connect(self._refresh_nome)
        row_nome.addWidget(self.cb_nome); row_nome.addStretch(1)
        vn.addLayout(row_nome)
        self.lbl_nome_metrics = QLabel(""); vn.addWidget(self.lbl_nome_metrics)
        self.tbl_nome = QTableWidget(); self._prep_table(self.tbl_nome, ["FLUIG","Placa","Infração","Data Infração","Status","AIT","Valor (R$)"])
        vn.addWidget(self.tbl_nome)

        # TIPO
        self.tab_tipo = QWidget(); vt = QVBoxLayout(self.tab_tipo)
        row_tipo = QHBoxLayout()
        row_tipo.addWidget(QLabel("Infração:"))
        self.cb_tipo = QComboBox(); self.cb_tipo.currentTextChanged.connect(self._refresh_tipo)
        row_tipo.addWidget(self.cb_tipo); row_tipo.addStretch(1)
        vt.addLayout(row_tipo)
        self.tbl_tipo_top = QTableWidget(); self._prep_table(self.tbl_tipo_top, ["Infração","Quantidade"])
        vt.addWidget(self.tbl_tipo_top)
        self.tbl_tipo_nomes = QTableWidget(); self._prep_table(self.tbl_tipo_nomes, ["Nome","Quantidade"])
        vt.addWidget(self.tbl_tipo_nomes)

        # PLACA
        self.tab_placa = QWidget(); vp = QVBoxLayout(self.tab_placa)
        row_placa = QHBoxLayout()
        row_placa.addWidget(QLabel("Placa:"))
        self.cb_placa = QComboBox(); self.cb_placa.currentTextChanged.connect(self._refresh_placa)
        row_placa.addWidget(self.cb_placa); row_placa.addStretch(1)
        vp.addLayout(row_placa)
        self.tbl_placa_top = QTableWidget(); self._prep_table(self.tbl_placa_top, ["Placa","Quantidade"])
        vp.addWidget(self.tbl_placa_top)
        self.tbl_placa_det = QTableWidget(); self._prep_table(self.tbl_placa_det, ["Nome","Infração","Qtde","Valor Total (R$)"])
        vp.addWidget(self.tbl_placa_det)

        # REGIÃO/IGREJA
        self.tab_reg = QWidget(); vr = QVBoxLayout(self.tab_reg)
        self.tbl_reg = QTableWidget(); self._prep_table(self.tbl_reg, ["Região","Igreja","Quantidade"])
        vr.addWidget(self.tbl_reg)

        self.tabs.addTab(self.tab_geral, "GERAL")
        self.tabs.addTab(self.tab_fluig, "FLUIG")
        self.tabs.addTab(self.tab_data, "DATA")
        self.tabs.addTab(self.tab_nome, "NOME")
        self.tabs.addTab(self.tab_tipo, "TIPO DE INFRAÇÃO")
        self.tabs.addTab(self.tab_placa, "PLACA")
        self.tabs.addTab(self.tab_reg, "REGIÃO/IGREJA")

    def _mount_filters(self):
        # limpa grid
        while self.filters_grid.count():
            item = self.filters_grid.takeAt(0)
            w = item.widget()
            if w: w.setParent(None)

        for i, (col, label) in enumerate(self.filter_cols):
            box = QFrame(); vb = QVBoxLayout(box)
            t = QLabel(label); vb.addWidget(t)
            h1 = QHBoxLayout()
            mode = QComboBox(); mode.addItems(["Todos","Excluir vazios","Somente vazios"])
            ms = CheckableComboBox(self.df_base.get(col, pd.Series([], dtype=str)).astype(str).dropna().unique())
            mode.currentTextChanged.connect(self._apply_filters_and_refresh)
            ms.changed.connect(self._apply_filters_and_refresh)
            h1.addWidget(mode); h1.addWidget(ms)
            vb.addLayout(h1)

            # linha de texto + botão +
            h2 = QHBoxLayout()
            le = QLineEdit(); le.setPlaceholderText(f"Filtrar {label}..."); le.textChanged.connect(self._apply_filters_and_refresh)
            btn = QPushButton("+"); btn.setFixedWidth(28)
            vb.addLayout(h2)
            h2.addWidget(le); h2.addWidget(btn)

            # se clicar +, adiciona outra caixa de texto
            def _add_more(_=None, col_=col, vb_=vb):
                le2 = QLineEdit(); le2.setPlaceholderText(f"Filtrar {label}..."); le2.textChanged.connect(self._apply_filters_and_refresh)
                vb_.addWidget(le2)
                self.text_filtros[col_].append(le2)
            btn.clicked.connect(_add_more)

            self.filters_grid.addWidget(box, i//3, i%3)
            self.mode_filtros[col] = mode
            self.multi_filtros[col] = ms
            self.text_filtros[col] = [le]

    def _prep_table(self, tbl, headers):
        tbl.setAlternatingRowColors(True)
        tbl.setSortingEnabled(True)
        tbl.horizontalHeader().setSortIndicatorShown(True)
        tbl.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        tbl.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        tbl.setColumnCount(len(headers))
        tbl.setHorizontalHeaderLabels(headers)

    # ----- filtros tempo <-> sliders
    def _on_slider_changed(self):
        if not self._date_index:
            return
        a = min(self.sl_start.value(), self.sl_end.value())
        b = max(self.sl_start.value(), self.sl_end.value())
        da = self._date_index[a]; db = self._date_index[b]
        self.de_start.blockSignals(True); self.de_end.blockSignals(True)
        self.de_start.setDate(QDate(da.year, da.month, da.day))
        self.de_end.setDate(QDate(db.year, db.month, db.day))
        self.de_start.blockSignals(False); self.de_end.blockSignals(False)
        self._apply_filters_and_refresh()

    def _on_dateedit_changed(self):
        if not self._date_index:
            self._apply_filters_and_refresh()
            return
        def _nearest_idx(dt):
            ts = pd.Timestamp(dt.year(), dt.month(), dt.day())
            arr = pd.Series(self._date_index)
            return int((arr - ts).abs().argmin())
        i0 = _nearest_idx(self.de_start.date())
        i1 = _nearest_idx(self.de_end.date())
        self.sl_start.blockSignals(True); self.sl_end.blockSignals(True)
        self.sl_start.setValue(min(i0, i1))
        self.sl_end.setValue(max(i0, i1))
        self.sl_start.blockSignals(False); self.sl_end.blockSignals(False)
        self._apply_filters_and_refresh()

    # ----- refresh + aplicação dos filtros
    def _apply_filters_and_refresh(self):
        # contadores brutos (sem filtro)
        self.lbl_count_det.setText(str(len(self.df_det)))
        self.lbl_count_past.setText(str(len(self.df_past)))
        self.lbl_count_cond.setText(str(len(self.df_cond)))

        # período
        q0, q1 = self.de_start.date(), self.de_end.date()
        t0 = pd.Timestamp(q0.year(), q0.month(), q0.day())
        t1 = pd.Timestamp(q1.year(), q1.month(), q1.day())
        a, b = (t0, t1) if t0 <= t1 else (t1, t0)

        df = self.df_base.copy()
        mask = (df["DT_INF"].notna()) & (df["DT_INF"] >= a) & (df["DT_INF"] <= b)
        df = df[mask].reset_index(drop=True)

        # filtros por coluna
        for col, _label in self.filter_cols:
            if col not in df.columns:
                continue
            mode = self.mode_filtros[col].currentText()
            if mode == "Excluir vazios":
                df = df[df[col].astype(str).str.strip()!=""]
            elif mode == "Somente vazios":
                df = df[df[col].astype(str).str.strip()==""]
            sels = [s for s in self.multi_filtros[col].selected_values() if s]
            if sels:
                df = df[df[col].astype(str).isin(sels)]
            # textos (OR entre caixas do mesmo campo)
            termos = [le.text().strip().lower() for le in self.text_filtros[col] if le.text().strip()]
            if termos:
                s = df[col].astype(str).str.lower()
                rgx = "|".join(map(re.escape, termos))
                df = df[s.str.contains(rgx, na=False)]

        # atualiza opções dos combos com o recorte atual (mantendo seleção)
        for col, _label in self.filter_cols:
            ms = self.multi_filtros[col]
            if col not in df.columns: 
                continue
            current_sel = ms.selected_values()
            ms.set_values(sorted([x for x in df[col].astype(str).dropna().unique() if x]))
            if current_sel:
                for i in range(ms.count()):
                    if ms.itemText(i) in current_sel:
                        idx = ms.model().index(i, 0)
                        ms.model().setData(idx, Qt.CheckState.Checked, Qt.ItemDataRole.CheckStateRole)
                ms._update_text()

        self.df_f = df

        # combos dependentes das abas
        nomes = sorted([x for x in self.df_f["U_NOME"].astype(str).unique() if x])
        self.cb_nome.blockSignals(True); self.cb_nome.clear(); self.cb_nome.addItems(nomes); self.cb_nome.blockSignals(False)

        tipos = sorted([x for x in self.df_f["U_INFRACAO"].astype(str).unique() if x])
        self.cb_tipo.blockSignals(True); self.cb_tipo.clear(); self.cb_tipo.addItems(tipos); self.cb_tipo.blockSignals(False)

        placas = sorted([x for x in self.df_f["U_PLACA"].astype(str).unique() if x])
        self.cb_placa.blockSignals(True); self.cb_placa.clear(); self.cb_placa.addItems(placas); self.cb_placa.blockSignals(False)

        # render das abas
        self._refresh_geral()
        self._refresh_fluig()
        self._refresh_data()
        self._refresh_nome()
        self._refresh_tipo()
        self._refresh_placa()
        self._refresh_reg()

    def _refresh_geral(self):
        df = self.df_f.copy()
        if df.empty:
            self._fill_table(self.tbl_geral, [])
            return
        g = df.groupby("U_NOME", dropna=False).agg(
            QT=("FLUIG","count"),
            PLACAS=("U_PLACA", lambda s: len(set([x for x in s if str(x).strip()]))),
            VAL=("VALOR_NUM","sum")
        ).reset_index().rename(columns={"U_NOME":"Nome","QT":"Qtde Multas","PLACAS":"Placas distintas","VAL":"Valor Total (R$)"})
        g = g.sort_values(["Qtde Multas","Valor Total (R$)"], ascending=[False, False]).head(10)
        rows = [[str(r["Nome"]), str(int(r["Qtde Multas"])), str(int(r["Placas distintas"])), f"{r['Valor Total (R$)']:.2f}"] for _, r in g.iterrows()]
        self._fill_table(self.tbl_geral, rows)

    def _refresh_fluig(self):
        df = self.df_f.copy()
        if df.empty or "U_STATUS" not in df.columns:
            self._fill_table(self.tbl_fluig, [])
            return
        g = df["U_STATUS"].fillna("").replace("", "Sem Status").str.upper().value_counts().reset_index()
        g.columns = ["Status","Quantidade"]
        rows = [[str(r["Status"]), str(int(r["Quantidade"]))] for _, r in g.iterrows()]
        self._fill_table(self.tbl_fluig, rows)

    def _refresh_data(self):
        df = self.df_f.copy()
        if df.empty:
            self._fill_table(self.tbl_data, [])
            return
        df["YM"] = df["DT_INF"].dt.to_period("M").astype(str)
        total = df.groupby("YM").size().reset_index(name="Total no mês")
        if "U_STATUS" in df.columns:
            ab = df["U_STATUS"].fillna("").str.upper().eq("ABERTA")
            ab_count = df[ab].groupby("YM").size().reindex(total["YM"]).fillna(0).astype(int).reset_index(drop=True)
            total["Abertas (se houver status)"] = ab_count
        rows = [[r["YM"], str(int(r["Total no mês"])), str(int(r.get("Abertas (se houver status)", 0)))] for _, r in total.iterrows()]
        self._fill_table(self.tbl_data, rows)

    def _refresh_nome(self):
        df = self.df_f.copy()
        nome = self.cb_nome.currentText().strip()
        if not nome:
            self.lbl_nome_metrics.setText("")
            self._fill_table(self.tbl_nome, [])
            return
        d = df[df["U_NOME"].astype(str)==nome]
        qt = len(d)
        valor = d["VALOR_NUM"].sum()
        placas = len(set([x for x in d["U_PLACA"].astype(str) if x]))
        self.lbl_nome_metrics.setText(f"Qtde: {qt} | Valor Total: R$ {valor:.2f} | Placas distintas: {placas}")
        rows = []
        for _, r in d.iterrows():
            rows.append([
                str(r.get("FLUIG","")),
                str(r.get("U_PLACA","")),
                str(r.get("U_INFRACAO","")),
                r["DT_INF"].strftime("%d/%m/%Y") if pd.notna(r["DT_INF"]) else "",
                str(r.get("U_STATUS","")),
                str(r.get("U_AIT","")),
                f"{float(r.get('VALOR_NUM',0.0)):.2f}"
            ])
        self._fill_table(self.tbl_nome, rows)

    def _refresh_tipo(self):
        df = self.df_f.copy()
        if df.empty:
            self._fill_table(self.tbl_tipo_top, [])
            self._fill_table(self.tbl_tipo_nomes, [])
            return
        g = df["U_INFRACAO"].astype(str).replace("", pd.NA).dropna().value_counts().reset_index()
        g.columns = ["Infração","Quantidade"]
        rows = [[str(r["Infração"]), str(int(r["Quantidade"]))] for _, r in g.iterrows()]
        self._fill_table(self.tbl_tipo_top, rows)

        tipo = self.cb_tipo.currentText().strip()
        if not tipo:
            self._fill_table(self.tbl_tipo_nomes, [])
        else:
            dn = df[df["U_INFRACAO"].astype(str)==tipo]
            g2 = dn.groupby("U_NOME").size().reset_index(name="Qtde").sort_values("Qtde", ascending=False)
            rows2 = [[str(r["U_NOME"]), str(int(r["Qtde"]))] for _, r in g2.iterrows()]
            self._fill_table(self.tbl_tipo_nomes, rows2)

    def _refresh_placa(self):
        df = self.df_f.copy()
        if df.empty:
            self._fill_table(self.tbl_placa_top, [])
            self._fill_table(self.tbl_placa_det, [])
            return
        g = df["U_PLACA"].astype(str).replace("", pd.NA).dropna().value_counts().reset_index()
        g.columns = ["Placa","Quantidade"]
        rows = [[str(r["Placa"]), str(int(r["Quantidade"]))] for _, r in g.iterrows()]
        self._fill_table(self.tbl_placa_top, rows)

        placa = self.cb_placa.currentText().strip()
        if not placa:
            self._fill_table(self.tbl_placa_det, [])
        else:
            dp = df[df["U_PLACA"].astype(str)==placa]
            g2 = dp.groupby(["U_NOME","U_INFRACAO"], dropna=False).agg(
                QT=("FLUIG","count"), VAL=("VALOR_NUM","sum")
            ).reset_index().sort_values("QT", ascending=False)
            rows2 = [[str(r["U_NOME"]), str(r["U_INFRACAO"]), str(int(r["QT"])), f"{r['VAL']:.2f}"] for _, r in g2.iterrows()]
            self._fill_table(self.tbl_placa_det, rows2)

    def _refresh_reg(self):
        df = self.df_f.copy()
        if df.empty or ("REGIAO" not in df.columns and "IGREJA" not in df.columns):
            self._fill_table(self.tbl_reg, [])
            return
        reg = df.get("REGIAO", pd.Series([""]*len(df)))
        igr = df.get("IGREJA", pd.Series([""]*len(df)))
        g = pd.DataFrame({"REGIAO":reg.astype(str),"IGREJA":igr.astype(str)})
        g["K"]=1
        g = g.groupby(["REGIAO","IGREJA"]).size().reset_index(name="Quantidade").sort_values("Quantidade", ascending=False)
        rows = [[str(r["REGIAO"]), str(r["IGREJA"]), str(int(r["Quantidade"]))] for _, r in g.iterrows()]
        self._fill_table(self.tbl_reg, rows)

    def _fill_table(self, tbl, rows):
        tbl.setRowCount(len(rows))
        for i, row in enumerate(rows):
            for j, val in enumerate(row):
                it = QTableWidgetItem(str(val))
                it.setFlags(it.flags() & ~Qt.ItemFlag.ItemIsEditable)
                tbl.setItem(i, j, it)
        tbl.resizeColumnsToContents()
        tbl.resizeRowsToContents()


class MultasMenu(QWidget):
    def __init__(self, open_cb):
        super().__init__()
        v = QVBoxLayout(self)
        card = QFrame()
        card.setObjectName("card")
        apply_shadow(card, radius=18)
        gv = QGridLayout(card)
        gv.setContentsMargins(18, 18, 18, 18)
        b1 = QPushButton("Multas em Aberto")
        b2 = QPushButton("Cenário Geral")
        b1.setMinimumHeight(64)
        b2.setMinimumHeight(64)
        b1.setFont(QFont("Arial", 16, weight=QFont.Weight.Bold))
        b2.setFont(QFont("Arial", 16, weight=QFont.Weight.Bold))
        b1.clicked.connect(lambda: open_cb("Multas em Aberto", lambda: InfraMultasWindow()))
        b2.clicked.connect(lambda: open_cb("Cenário Geral", lambda: CenarioGeralWindow()))
        gv.addWidget(b1, 0, 0)
        gv.addWidget(b2, 0, 1)
        v.addWidget(card)


class MainWindow(QMainWindow):

    def __init__(self, user_email: str | None = None):
        super().__init__()
        self.setWindowTitle("GESTÃO DE FROTAS")
        self.resize(1280, 860)

        # ---- Área de abas ----
        self.tab_widget = QTabWidget()
        self.tab_widget.setTabsClosable(True)
        self.tab_widget.setDocumentMode(True)
        self.tab_widget.tabCloseRequested.connect(self.close_tab)
        self.setCentralWidget(self.tab_widget)

        # ---- Home / Início ----
        home = QWidget()
        hv = QVBoxLayout(home)

        # Cabeçalho
        title_card = QFrame(); title_card.setObjectName("glass")
        apply_shadow(title_card, radius=20, blur=60, color=QColor(0, 0, 0, 60))
        tv = QVBoxLayout(title_card); tv.setContentsMargins(24, 24, 24, 24)

        t = QLabel("Gestão de Frota")
        t.setAlignment(Qt.AlignmentFlag.AlignCenter)
        t.setFont(QFont("Arial", 28, QFont.Weight.Bold))
        tv.addWidget(t)

        if user_email:
            tv.addWidget(QLabel(f"Logado como: {user_email}"),
                         alignment=Qt.AlignmentFlag.AlignCenter)
        hv.addWidget(title_card)

        # Cartão com os botões grandes
        grid_card = QFrame(); grid_card.setObjectName("card"); apply_shadow(grid_card, radius=18)
        gv = QGridLayout(grid_card); gv.setContentsMargins(18, 18, 18, 18)

        buttons = [
            ("Base", self.open_base),
            ("Infrações e Multas", self.open_multas),
            ("Combustível", self.open_combustivel),
            ("Relatórios", self.open_relatorios),
            ("Alertas", self.open_alertas),
            ("Condutor", self.open_condutor),   # ✅ aqui o botão existe
        ]


        for i, (label, slot) in enumerate(buttons):
            b = QPushButton(label)
            b.setMinimumHeight(64)
            b.setFont(QFont("Arial", 16, QFont.Weight.Bold))
            b.clicked.connect(slot)
            gv.addWidget(b, i // 2, i % 2)

        hv.addWidget(grid_card)

        # Barra inferior (logout)
        bar = QHBoxLayout()
        out = QPushButton("Sair"); out.setObjectName("danger")
        out.setMinimumHeight(44)
        out.clicked.connect(self.logout)
        bar.addStretch(1); bar.addWidget(out)
        hv.addLayout(bar)

        # Coloca a Home como primeira aba
        self.tab_widget.addTab(home, "Início")

    # ===== Utilidades de abas =====
    def _find_tab_index_by_title(self, title: str) -> int:
        for idx in range(self.tab_widget.count()):
            if self.tab_widget.tabText(idx) == title:
                return idx
        return -1

    def add_or_focus(self, title, factory):
        """Evita duplicar abas com o mesmo título: foca se já existir; senão cria."""
        idx = self._find_tab_index_by_title(title)
        if idx >= 0:
            self.tab_widget.setCurrentIndex(idx)
            return
        w = factory()
        self.tab_widget.addTab(w, title)
        self.tab_widget.setCurrentWidget(w)


    def open_cenarios_gerais(self):

        for i in range(self.tab_widget.count()):
            w = self.tab_widget.widget(i)
            if isinstance(w, CenariosGeraisContainer):
                self.tab_widget.setCurrentIndex(i)
                return

        cont = CenariosGeraisContainer(self, titulo_base="Cenários Gerais")
        idx = self.tab_widget.addTab(cont, "Cenários Gerais")
        self.tab_widget.setCurrentIndex(idx)




from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QTabWidget, QLabel, QMessageBox, QShortcut
)
from PyQt6.QtGui import QKeySequence

class CenariosGeraisContainer(QWidget):
    """
    Contêiner que permite múltiplas abas de 'Cenários Gerais',
    cada uma com uma instância independente de CenariosGeraisWindow.
    """
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

    def close_tab(self, index):
        """Impede fechar a Home (índice 0); fecha as demais."""
        if index == 0:
            return
        w = self.tab_widget.widget(index)
        self.tab_widget.removeTab(index)
        if w is not None:
            w.deleteLater()


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


    def close_tab(self, index):
        """Impede fechar a Home (índice 0); fecha as demais."""
        if index == 0:
            return
        w = self.tab_widget.widget(index)
        self.tab_widget.removeTab(index)
        w.deleteLater()

    # ===== Ações dos botões =====
    def open_base(self):
        try:
            from gestao_frota_single import BaseTab
            self.add_or_focus("Base", lambda: BaseTab())
        except Exception as e:
            QMessageBox.warning(self, "Base", f"Não foi possível abrir a Base.\n{e}")

    def open_multas(self):
        self.add_or_focus("Infrações e Multas", lambda: InfraMultasWindow())

    def open_combustivel(self):
        try:
            self.add_or_focus("Combustível", lambda: CombustivelWindow())
        except Exception as e:
            QMessageBox.warning(self, "Combustível", str(e))

    def open_relatorios(self):
        """
        Permite selecionar UMA OU VÁRIAS planilhas e abre cada uma em sua própria aba:
        'Relatório — <nome_da_planilha>'.
        Se a aba já existir, apenas foca.
        """
        paths, _ = QFileDialog.getOpenFileNames(
            self, "Abrir arquivo(s) de relatório", "",
            "Planilhas (*.xlsx *.xls *.csv)"
        )
        if not paths:
            return

        for p in paths:
            try:
                stem = Path(p).stem
                title = f"Relatório — {stem}"
                self.add_or_focus(title, lambda p_=p: RelatorioWindow(p_))
            except Exception as e:
                QMessageBox.warning(self, "Relatórios", f"Não foi possível abrir '{p}'.\n{e}")

    def open_alertas(self):
        """Abre a aba de Alertas (corrigido; antes o método não existia)."""
        try:
            self.add_or_focus("Alertas", lambda: AlertsTab())
        except Exception as e:
            QMessageBox.warning(self, "Alertas", f"Não foi possível abrir Alertas.\n{e}")

    def open_condutor(self):
        """Abre a tela de Condutor em uma nova aba."""
        try:
            from condutor import CondutorWindow
            self.add_or_focus("Condutor — Busca Integrada", lambda: CondutorWindow())
        except Exception as e:
            QMessageBox.warning(self, "Condutor", f"Não foi possível abrir a tela de Condutor.\n{e}")

    def logout(self):
        self.close()
