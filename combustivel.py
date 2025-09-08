import os
import pandas as pd

from PyQt6.QtCore import Qt, QTimer, QFileSystemWatcher
from PyQt6.QtGui import QColor, QFont
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QFrame, QHBoxLayout, QLabel, QPushButton, QScrollArea,
    QWidget as QW, QGridLayout, QLineEdit, QTableWidget, QTableWidgetItem, QHeaderView,
    QMessageBox, QComboBox, QFileDialog
)

from gestao_frota_single import cfg_get, DATE_FORMAT
from utils import apply_shadow, CheckableComboBox, df_apply_global_texts, ensure_status_cols




class _TabelaComFiltros(QWidget):

    def __init__(self, titulo):
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

        title = QLabel(titulo); title.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        hv.addWidget(title)

        actions = QHBoxLayout()
        btn_limpar = QPushButton("Limpar filtros"); btn_limpar.clicked.connect(self.limpar_filtros)
        btn_export = QPushButton("Exportar Excel"); btn_export.clicked.connect(self.exportar_excel)
        actions.addWidget(btn_limpar); actions.addStretch(1); actions.addWidget(btn_export)
        hv.addLayout(actions)

        # Filtro global com "+"
        row_global = QHBoxLayout()
        row_global.addWidget(QLabel("Filtro global:"))
        def _add_box():
            le = QLineEdit(); le.setPlaceholderText("Filtrar em TODAS as colunas…")
            le.textChanged.connect(self._apply_filters)
            self.global_boxes.append(le)
            row_global.addWidget(le, 1)
        _add_box()
        btn_plus = QPushButton("+"); btn_plus.setFixedWidth(28); btn_plus.clicked.connect(_add_box)
        row_global.addWidget(btn_plus)
        hv.addLayout(row_global)

        # Filtros por coluna
        self.filters_scroll = QScrollArea(); self.filters_scroll.setWidgetResizable(True)
        self.filters_host = QW(); self.filters_grid = QGridLayout(self.filters_host)
        self.filters_grid.setContentsMargins(12,12,12,12); self.filters_grid.setHorizontalSpacing(14); self.filters_grid.setVerticalSpacing(8)
        self.filters_scroll.setWidget(self.filters_host)
        hv.addWidget(self.filters_scroll)

        root.addWidget(header)

        # Tabela
        table_card = QFrame(); table_card.setObjectName("glass"); apply_shadow(table_card, radius=18, blur=60, color=QColor(0,0,0,80))
        tv = QVBoxLayout(table_card)
        self.tabela = QTableWidget()
        self.tabela.setAlternatingRowColors(True)
        self.tabela.setSortingEnabled(True)
        self.tabela.horizontalHeader().setSortIndicatorShown(True)
        self.tabela.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.tabela.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        tv.addWidget(self.tabela)
        root.addWidget(table_card)

    def set_dataframe(self, df: pd.DataFrame):
        self.df_original = df.copy()
        self.df_filtrado = self.df_original.copy()
        self._montar_filtros()
        self._fill_table(self.df_filtrado)

    def _montar_filtros(self):
        while self.filters_grid.count():
            it = self.filters_grid.takeAt(0)
            if it.widget(): it.widget().setParent(None)
        self.mode_filtros.clear(); self.multi_filtros.clear()

        cols = list(self.df_original.columns)
        for i, coluna in enumerate(cols):
            wrap = QFrame(); v = QVBoxLayout(wrap)
            label = QLabel(coluna); label.setObjectName("colTitle"); v.addWidget(label)

            line1 = QHBoxLayout()
            mode = QComboBox(); mode.addItems(["Todos","Excluir vazios","Somente vazios"]); mode.currentTextChanged.connect(self._apply_filters)
            ms = CheckableComboBox(self.df_original[coluna].dropna().astype(str).unique()); ms.changed.connect(self._apply_filters)
            line1.addWidget(mode); line1.addWidget(ms)

            v.addLayout(line1)
            self.filters_grid.addWidget(wrap, i//3, i%3)
            self.mode_filtros[coluna] = mode
            self.multi_filtros[coluna] = ms

    def limpar_filtros(self):
        for le in self.global_boxes:
            le.blockSignals(True); le.clear(); le.blockSignals(False)
        for mode in self.mode_filtros.values():
            mode.blockSignals(True); mode.setCurrentIndex(0); mode.blockSignals(False)
        for ms in self.multi_filtros.values():
            vals = [ms.itemText(i) for i in range(ms.count())]
            ms.set_values(vals)
        self._apply_filters()

    def exportar_excel(self):
        try:
            out = "combustivel_filtrado.xlsx"
            self.df_filtrado.to_excel(out, index=False)
            QMessageBox.information(self, "Exportado", f"{out} criado.")
        except Exception as e:
            QMessageBox.critical(self, "Erro", str(e))

    def _apply_filters(self):
        df = self.df_original.copy()

        texts = [le.text() for le in self.global_boxes if le.text().strip()]
        df = df_apply_global_texts(df, texts)

        for coluna in df.columns:
            mode = self.mode_filtros[coluna].currentText()
            if mode == "Excluir vazios":
                df = df[df[coluna].astype(str).str.strip() != ""]
            elif mode == "Somente vazios":
                df = df[df[coluna].astype(str).str.strip() == ""]
            sels = [s for s in self.multi_filtros[coluna].selected_values() if s]
            if sels:
                df = df[df[coluna].astype(str).isin(sels)]

        self.df_filtrado = df
        self._fill_table(self.df_filtrado)

        # Atualiza listas mantendo seleção
        for coluna in self.df_filtrado.columns:
            ms = self.multi_filtros[coluna]
            current_sel = ms.selected_values()
            ms.set_values(self.df_filtrado[coluna].dropna().astype(str).unique())
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
                self.tabela.setItem(i, j, it)

        self.tabela.resizeColumnsToContents()
        self.tabela.horizontalHeader().setStretchLastSection(True)
        self.tabela.resizeRowsToContents()


class CombustivelWindow(QWidget):
    """
    Duas abas principais:
      - Extrato Geral (cfg: extrato_geral_path)
      - Extrato Simplificado (cfg: extrato_simplificado_path)
    Ambos com a UI de filtros padrão.
    """
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Combustível")
        self.resize(1280, 820)

        self.p_extrato = cfg_get("extrato_geral_path")
        self.p_simpl   = cfg_get("extrato_simplificado_path")

        root = QVBoxLayout(self)

        # Cabeçalho
        head = QFrame(); head.setObjectName("glass"); apply_shadow(head, radius=18, blur=60, color=QColor(0,0,0,60))
        hv = QVBoxLayout(head); hv.setContentsMargins(18,18,18,18)
        t = QLabel("Módulo de Combustível"); t.setAlignment(Qt.AlignmentFlag.AlignCenter)
        t.setFont(QFont("Arial", 22, QFont.Weight.Bold))
        hv.addWidget(t)

        # Ações rápidas
        actions = QHBoxLayout()
        btn_open_extrato = QPushButton("Abrir outro arquivo…"); btn_open_extrato.clicked.connect(self._abrir_arquivo)
        actions.addStretch(1); actions.addWidget(btn_open_extrato)
        hv.addLayout(actions)

        root.addWidget(head)

        # Abas internas
        from PyQt6.QtWidgets import QTabWidget
        self.tabs = QTabWidget()
        root.addWidget(self.tabs, 1)

        self.tab_extrato = _TabelaComFiltros("Extrato Geral")
        self.tab_simpl   = _TabelaComFiltros("Extrato Simplificado")

        self.tabs.addTab(self.tab_extrato, "Extrato Geral")
        self.tabs.addTab(self.tab_simpl, "Extrato Simplificado")

        # Watchers
        self.watcher = QFileSystemWatcher()
        for p in (self.p_extrato, self.p_simpl):
            if p and os.path.exists(p):
                self.watcher.addPath(p)
        self.watcher.fileChanged.connect(lambda _p: QTimer.singleShot(400, self._reload_watched))

        # Carga inicial
        self._carregar_inicial()

    # ---- carga de dados
    def _read_sheet(self, path):
        if not path or not os.path.exists(path):
            return pd.DataFrame()
        try:
            ext = os.path.splitext(path)[1].lower()
            if ext in (".xlsx",".xls"):
                df = pd.read_excel(path, dtype=str).fillna("")
            elif ext == ".csv":
                df = pd.read_csv(path, dtype=str, encoding="utf-8").fillna("")
            else:
                QMessageBox.warning(self, "Combustível", "Formato não suportado.")
                return pd.DataFrame()
        except Exception as e:
            QMessageBox.warning(self, "Combustível", f"Erro ao abrir {os.path.basename(path)}: {e}")
            return pd.DataFrame()

        # padronizações simples comuns ao extrato
        # (mantemos nomes originais; usuário pode filtrar por qualquer coluna)
        return df

    def _carregar_inicial(self):
        de = self._read_sheet(self.p_extrato)
        ds = self._read_sheet(self.p_simpl)
        self.tab_extrato.set_dataframe(de)
        self.tab_simpl.set_dataframe(ds)

    def _reload_watched(self):
        which = self.tabs.currentIndex()
        self._carregar_inicial()
        self.tabs.setCurrentIndex(which)

    # ---- ações
    def _abrir_arquivo(self):
        p, _ = QFileDialog.getOpenFileName(self, "Abrir arquivo de extrato", "", "Planilhas (*.xlsx *.xls *.csv)")
        if not p:
            return
        # Abre o arquivo selecionado na aba atual
        df = self._read_sheet(p)
        tab = self.tab_extrato if self.tabs.currentIndex() == 0 else self.tab_simpl
        tab.set_dataframe(df)


class CombustivelMenu(QWidget):
    """
    Menu simples — se quiser abrir outras visões no futuro (dashboards, mapas, etc.)
    """
    def __init__(self, open_cb=None):
        super().__init__()
        v = QVBoxLayout(self)
        card = QFrame(); card.setObjectName("card"); apply_shadow(card, radius=18)
        gv = QGridLayout(card); gv.setContentsMargins(18,18,18,18)

        b1 = QPushButton("Abrir Extratos")
        b1.setMinimumHeight(64)
        b1.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        b1.clicked.connect(lambda: open_cb("Combustível", lambda: CombustivelWindow()) if open_cb else None)

        gv.addWidget(b1, 0, 0)
        v.addWidget(card)
