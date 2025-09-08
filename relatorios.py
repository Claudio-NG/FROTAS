# relatorios.py
import os
import pandas as pd
from PyQt6.QtCore import Qt, QTimer, QFileSystemWatcher
from PyQt6.QtGui import QColor, QFontMetrics
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QFrame, QHBoxLayout, QLabel, QPushButton, QScrollArea,
    QWidget as QW, QGridLayout, QLineEdit, QTableWidget, QTableWidgetItem, QHeaderView,
    QMessageBox, QComboBox, QSizePolicy, QFileDialog
)

from utils import (
    ensure_status_cols, apply_shadow, CheckableComboBox,
    df_apply_global_texts, STATUS_COLOR
)


class RelatorioWindow(QWidget):
    def __init__(self, caminho_arquivo):
        super().__init__()
        fm = QFontMetrics(self.font())
        self.max_pix = fm.horizontalAdvance("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
        self.setWindowTitle("Relatórios")
        self.resize(1280, 820)

        self.path = caminho_arquivo
        self.df_original = pd.DataFrame()
        self.df_filtrado = pd.DataFrame()

        self.mode_filtros = {}
        self.multi_filtros = {}
        self.global_boxes = []

        root = QVBoxLayout(self)

        # Header / ações
        header = QFrame(); header.setObjectName("card"); apply_shadow(header, radius=18)
        hv = QVBoxLayout(header)

        actions = QHBoxLayout()
        btn_abrir = QPushButton("Abrir…"); btn_abrir.clicked.connect(self._abrir_arquivo)
        btn_recarregar = QPushButton("Recarregar"); btn_recarregar.clicked.connect(self.recarregar)
        btn_limpar = QPushButton("Limpar filtros"); btn_limpar.clicked.connect(self.limpar_filtros)
        btn_export = QPushButton("Exportar Excel"); btn_export.clicked.connect(self.exportar_excel)
        actions.addWidget(btn_abrir); actions.addWidget(btn_recarregar); actions.addWidget(btn_limpar); actions.addStretch(1); actions.addWidget(btn_export)
        hv.addLayout(actions)

        # Filtro global (único) com botão +
        row_global = QHBoxLayout()
        row_global.addWidget(QLabel("Filtro global:"))
        def add_box():
            le = QLineEdit()
            le.setPlaceholderText("Digite para filtrar em TODAS as colunas…")
            le.setMaximumWidth(self.max_pix)
            le.textChanged.connect(self.atualizar_filtro)
            self.global_boxes.append(le)
            row_global.addWidget(le, 1)
        add_box()
        btn_plus = QPushButton("+"); btn_plus.setFixedWidth(28); btn_plus.clicked.connect(add_box)
        row_global.addWidget(btn_plus)
        hv.addLayout(row_global)

        # Filtros por coluna (modo + multiseleção)
        self.scroll = QScrollArea(); self.scroll.setWidgetResizable(True)
        self.filters_host = QW(); self.filters_grid = QGridLayout(self.filters_host)
        self.filters_grid.setContentsMargins(12,12,12,12); self.filters_grid.setHorizontalSpacing(14); self.filters_grid.setVerticalSpacing(8)
        self.scroll.setWidget(self.filters_host)
        hv.addWidget(self.scroll)

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

        # Watcher para recarregar quando editar a planilha
        self.watcher = QFileSystemWatcher()
        if os.path.exists(self.path): self.watcher.addPath(self.path)
        self.watcher.fileChanged.connect(lambda _p: QTimer.singleShot(400, self.recarregar))

        # Carregar dados
        self.carregar_dados(self.path)
        self.showMaximized()

    def _abrir_arquivo(self):
        p, _ = QFileDialog.getOpenFileName(self, "Abrir arquivo", "", "Planilhas (*.xlsx *.xls *.csv)")
        if p:
            # remove o path anterior (se estava sendo observado) e observa o novo
            try:
                if os.path.exists(self.path):
                    self.watcher.removePath(self.path)
            except Exception:
                pass
            self.path = p
            if os.path.exists(self.path):
                self.watcher.addPath(self.path)
            self.carregar_dados(self.path)

    def carregar_dados(self, caminho):
        if not caminho:
            return
        ext = os.path.splitext(caminho)[1].lower()
        try:
            if ext in (".xlsx",".xls"):
                df = pd.read_excel(caminho, dtype=str).fillna("")
            elif ext == ".csv":
                try:
                    df = pd.read_csv(caminho, dtype=str, encoding="utf-8").fillna("")
                except UnicodeDecodeError:
                    df = pd.read_csv(caminho, dtype=str, encoding="latin1").fillna("")
            else:
                QMessageBox.warning(self, "Aviso", "Formato não suportado.")
                return
        except Exception as e:
            QMessageBox.critical(self, "Erro ao carregar", str(e))
            return

        self.df_original = ensure_status_cols(df)
        self.df_filtrado = self.df_original.copy()
        self._montar_filtros()
        self.preencher_tabela(self.df_filtrado)

    def _montar_filtros(self):
        # Limpa grid
        while self.filters_grid.count():
            item = self.filters_grid.takeAt(0)
            w = item.widget()
            if w: w.setParent(None)
        self.mode_filtros.clear()
        self.multi_filtros.clear()

        cols = list(self.df_original.columns)
        for i, coluna in enumerate(cols):
            wrap = QFrame(); v = QVBoxLayout(wrap)
            label = QLabel(coluna); label.setObjectName("colTitle"); label.setWordWrap(True); label.setMaximumWidth(self.max_pix)

            line1 = QHBoxLayout()
            mode = QComboBox(); mode.addItems(["Todos","Excluir vazios","Somente vazios"]); mode.currentTextChanged.connect(self.atualizar_filtro)
            ms = CheckableComboBox(self.df_original[coluna].dropna().astype(str).unique()); ms.changed.connect(self.atualizar_filtro)
            line1.addWidget(mode); line1.addWidget(ms)

            v.addWidget(label); v.addLayout(line1)
            self.filters_grid.addWidget(wrap, i//3, i%3)
            self.mode_filtros[coluna] = mode
            self.multi_filtros[coluna] = ms

    def recarregar(self):
        self.carregar_dados(self.path)

    def limpar_filtros(self):
        for le in self.global_boxes:
            le.blockSignals(True); le.clear(); le.blockSignals(False)
        for mode in self.mode_filtros.values():
            mode.blockSignals(True); mode.setCurrentIndex(0); mode.blockSignals(False)
        for ms in self.multi_filtros.values():
            vals = [ms.itemText(i) for i in range(ms.count())]
            ms.set_values(vals)
        self.atualizar_filtro()

    def atualizar_filtro(self):
        df = self.df_original.copy()

        # Global: todas as colunas, pode ter várias caixas (+)
        texts = [le.text() for le in self.global_boxes if le.text().strip()]
        df = df_apply_global_texts(df, texts)

        # Por coluna: modo + multiseleção
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
        self.preencher_tabela(self.df_filtrado)

        # Atualiza os combos com valores do recorte atual (mantém seleção)
        for coluna in self.df_filtrado.columns:
            ms = self.multi_filtros[coluna]
            current_sel = ms.selected_values()
            ms.set_values(self.df_filtrado[coluna].dropna().astype(str).unique())
            # restaura seleção anterior
            if current_sel:
                for i in range(ms.count()):
                    if ms.itemText(i) in current_sel:
                        idx = ms.model().index(i, 0)
                        ms.model().setData(idx, Qt.CheckState.Checked, Qt.ItemDataRole.CheckStateRole)
                ms._update_text()

    def preencher_tabela(self, df):
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
                # se for coluna STATUS (comum em várias bases), aplicar cor
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
            out = os.path.splitext(os.path.basename(self.path))[0] + "_filtrado.xlsx"
            self.df_filtrado.to_excel(out, index=False)
            QMessageBox.information(self, "Exportado", f"{out} criado.")
        except Exception as e:
            QMessageBox.critical(self, "Erro", str(e))