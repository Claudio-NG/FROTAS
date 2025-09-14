# relatorios.py — versão multi-abas, com ordenação por clique e exportar por aba
import os
import pandas as pd
from PyQt6.QtCore import Qt, QTimer, QFileSystemWatcher
from PyQt6.QtGui import QColor, QFontMetrics
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QFrame, QHBoxLayout, QLabel, QPushButton, QScrollArea,
    QWidget as QW, QGridLayout, QLineEdit, QTableWidget, QTableWidgetItem, QHeaderView,
    QMessageBox, QComboBox, QSizePolicy, QFileDialog, QTabWidget
)

from utils import (
    ensure_status_cols, apply_shadow, CheckableComboBox,
    df_apply_global_texts
)

class _ReportTab(QWidget):
    def __init__(self, path: str):
        super().__init__()
        self.path = path
        self.df_original = pd.DataFrame()
        self.df_filtrado = pd.DataFrame()
        self.mode_filtros = {}
        self.multi_filtros = {}
        self.global_boxes = []

        fm = QFontMetrics(self.font())
        self.max_pix = fm.horizontalAdvance("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")

        root = QVBoxLayout(self)

        # Header com ações da ABA
        header = QFrame(); header.setObjectName("card"); apply_shadow(header, radius=18)
        hv = QVBoxLayout(header)

        actions = QHBoxLayout()
        self.btn_recarregar = QPushButton("Recarregar")
        self.btn_limpar = QPushButton("Limpar filtros")
        self.btn_export = QPushButton("Exportar Excel")
        actions.addWidget(self.btn_recarregar)
        actions.addWidget(self.btn_limpar)
        actions.addStretch(1)
        actions.addWidget(self.btn_export)
        hv.addLayout(actions)

        # Filtro global (múltiplas caixas com +)
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

        # Filtros por coluna
        self.scroll = QScrollArea(); self.scroll.setWidgetResizable(True)
        self.filters_host = QW(); self.filters_grid = QGridLayout(self.filters_host)
        self.filters_grid.setContentsMargins(12,12,12,12)
        self.filters_grid.setHorizontalSpacing(14); self.filters_grid.setVerticalSpacing(8)
        self.scroll.setWidget(self.filters_host)
        hv.addWidget(self.scroll)

        root.addWidget(header)

        # Tabela
        table_card = QFrame(); table_card.setObjectName("glass")
        apply_shadow(table_card, radius=18, blur=60, color=QColor(0,0,0,80))
        tv = QVBoxLayout(table_card)
        self.tabela = QTableWidget()
        self.tabela.setAlternatingRowColors(True)
        self.tabela.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.tabela.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        # Ordenação por clique no cabeçalho
        self.tabela.setSortingEnabled(True)
        self.tabela.horizontalHeader().setSortIndicatorShown(True)
        tv.addWidget(self.tabela)
        root.addWidget(table_card)

        # sinais dos botões
        self.btn_recarregar.clicked.connect(self.recarregar)
        self.btn_limpar.clicked.connect(self.limpar_filtros)
        self.btn_export.clicked.connect(self.exportar_excel)

        # watcher desta aba
        self.watcher = QFileSystemWatcher()
        if os.path.exists(self.path):
            self.watcher.addPath(self.path)
        self.watcher.fileChanged.connect(lambda _p: QTimer.singleShot(400, self.recarregar))

        # carrega
        self.carregar_dados(self.path)

    # ------- carregamento / filtros / export -------
    def carregar_dados(self, caminho):
        if not caminho:
            return
        ext = os.path.splitext(caminho)[1].lower()
        try:
            if ext in (".xlsx",".xls"):
                df = pd.read_excel(caminho, dtype=str).fillna("")
            elif ext == ".csv":
                try:
                    # tenta UTF-8; fallback Latin-1, separador inferido
                    df = pd.read_csv(caminho, dtype=str, sep=None, engine="python", encoding="utf-8").fillna("")
                except UnicodeDecodeError:
                    df = pd.read_csv(caminho, dtype=str, sep=None, engine="python", encoding="latin1").fillna("")
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
        # limpa grid
        while self.filters_grid.count():
            item = self.filters_grid.takeAt(0)
            w = item.widget()
            if w: w.setParent(None)
        self.mode_filtros.clear()
        self.multi_filtros.clear()

        cols = list(self.df_original.columns)
        for i, coluna in enumerate(cols):
            wrap = QFrame(); v = QVBoxLayout(wrap)
            label = QLabel(coluna); label.setObjectName("colTitle"); label.setWordWrap(True)
            v.addWidget(label)

            line = QHBoxLayout()
            mode = QComboBox(); mode.addItems(["Todos", "Excluir vazios", "Somente vazios"])
            mode.currentTextChanged.connect(self.atualizar_filtro)
            ms = CheckableComboBox(self.df_original[coluna].dropna().astype(str).unique())
            ms.changed.connect(self.atualizar_filtro)
            line.addWidget(mode); line.addWidget(ms)
            v.addLayout(line)

            r, c = divmod(i, 3)  # 3 colunas de filtros por linha
            self.filters_grid.addWidget(wrap, r, c)

            self.mode_filtros[coluna] = mode
            self.multi_filtros[coluna] = ms

    def atualizar_filtro(self):
        df = self.df_original.copy()
        texts = [b.text() for b in self.global_boxes]
        df = df_apply_global_texts(df, texts)
        # modos e multiseleção
        for coluna, mode in self.mode_filtros.items():
            m = mode.currentText()
            if m == "Excluir vazios":
                df = df[df[coluna].astype(str).str.strip() != ""]
            elif m == "Somente vazios":
                df = df[df[coluna].astype(str).str.strip() == ""]
            sels = [s for s in self.multi_filtros[coluna].selected_values() if s]
            if sels:
                df = df[df[coluna].astype(str).isin(sels)]
        self.df_filtrado = df
        self.preencher_tabela(self.df_filtrado)

    def limpar_filtros(self):
        for b in self.global_boxes:
            b.blockSignals(True); b.clear(); b.blockSignals(False)
        for mode in self.mode_filtros.values():
            mode.blockSignals(True); mode.setCurrentIndex(0); mode.blockSignals(False)
        for ms in self.multi_filtros.values():
            vals = [ms.itemText(i) for i in range(ms.count())]
            ms.set_values(vals)
        self.atualizar_filtro()

    def preencher_tabela(self, df):
        # manter ordenação clicável: desliga antes de preencher e religa depois
        self.tabela.setSortingEnabled(False)
        if df is None or df.empty:
            self.tabela.clear()
            self.tabela.setRowCount(0); self.tabela.setColumnCount(0)
            self.tabela.setSortingEnabled(True)
            return

        headers = [str(c) for c in df.columns]
        self.tabela.clear()
        self.tabela.setColumnCount(len(headers))
        self.tabela.setHorizontalHeaderLabels(headers)
        self.tabela.setRowCount(len(df))

        for i, (_, row) in enumerate(df.iterrows()):
            for j, col in enumerate(headers):
                it = QTableWidgetItem(str(row.get(col, "")))
                it.setFlags(it.flags() & ~Qt.ItemFlag.ItemIsEditable)
                self.tabela.setItem(i, j, it)

        self.tabela.resizeColumnsToContents()
        self.tabela.horizontalHeader().setStretchLastSection(True)
        # RELIGA ordenação — agora o clique no título alterna crescente/decrescente
        self.tabela.setSortingEnabled(True)

    def exportar_excel(self):
        try:
            base = os.path.splitext(os.path.basename(self.path))[0]
            out = f"{base}_filtrado.xlsx"
            self.df_filtrado.to_excel(out, index=False)
            QMessageBox.information(self, "Exportado", f"{out} criado.")
        except Exception as e:
            QMessageBox.critical(self, "Erro", str(e))

    def recarregar(self):
        # reanexa watcher (alguns editores trocam o arquivo por outro inode)
        try:
            if os.path.exists(self.path):
                self.watcher.removePath(self.path)
        except Exception:
            pass
        if os.path.exists(self.path):
            self.watcher.addPath(self.path)
        self.carregar_dados(self.path)


# ---------- Janela principal de Relatórios (multi-abas) ----------
class RelatorioWindow(QWidget):
    """
    • Botão “Abrir…” adiciona uma NOVA ABA com o arquivo escolhido (sem fechar as outras).
    • Cada aba tem filtros, ordenação clicável no cabeçalho e botão de exportar.
    • “Recarregar” e “Limpar filtros” atuam apenas na aba ativa.
    """
    def __init__(self, caminho_inicial: str | None = None):
        super().__init__()
        self.setWindowTitle("Relatórios")
        self.resize(1280, 820)

        root = QVBoxLayout(self)

        # Barra superior: abrir nova planilha como nova aba
        top = QFrame(); top.setObjectName("card"); apply_shadow(top, radius=18)
        th = QHBoxLayout(top)
        btn_abrir = QPushButton("Abrir…"); btn_abrir.clicked.connect(self._abrir_arquivo)
        th.addWidget(btn_abrir); th.addStretch(1)
        root.addWidget(top)

        # Abas
        self.tabs = QTabWidget()
        root.addWidget(self.tabs, 1)

        # Se vier um caminho pronto, já cria a primeira aba
        if caminho_inicial:
            self._add_tab(caminho_inicial)

        self.showMaximized()

    def _add_tab(self, path: str):
        if not path: return
        tab = _ReportTab(path)
        title = os.path.splitext(os.path.basename(path))[0] or "Relatório"
        self.tabs.addTab(tab, title)
        self.tabs.setCurrentWidget(tab)

    def _abrir_arquivo(self):
        p, _ = QFileDialog.getOpenFileName(self, "Abrir arquivo", "", "Planilhas (*.xlsx *.xls *.csv)")
        if p:
            self._add_tab(p)
