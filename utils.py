import os, ast, re, shutil, unicodedata, base64
from glob import glob
from pathlib import Path
import pandas as pd

from PyQt6.QtCore import QDate, Qt, pyqtSignal, QUrl
from PyQt6.QtGui import QColor, QDesktopServices
from PyQt6.QtWidgets import (
    QGraphicsDropShadowEffect, QMessageBox, QComboBox, QDialog, QVBoxLayout, QHBoxLayout,
    QLabel, QTableWidget, QTableWidgetItem, QHeaderView, QPushButton, QFrame, QLineEdit,
    QSplitter, QGroupBox, QWidget, QFileDialog, QScrollArea, QGridLayout
)


BASE_DIR = os.path.expanduser("~")
APP_DIR  = os.path.join(BASE_DIR, "Documentos", "GestaoFrotas")
CFG_PATH = str(Path(__file__).resolve().parent / "base.json")

_DEFAULTS_LOCAL = {
    "multas_root": os.path.join(APP_DIR, "Multas"),
    "geral_multas_csv": os.path.join(APP_DIR, "Multas", "geral_multas.csv"),
    "pastores_dir": os.path.join(APP_DIR, "Pastores"),
    "condutores_root": os.path.join(APP_DIR, "Condutores"),
}

def _cfg_load() -> dict:
    try:
        if os.path.exists(CFG_PATH):
            import json
            with open(CFG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
        else:
            data = {}
    except Exception:
        data = {}
    # garante defaults
    for k, v in _DEFAULTS_LOCAL.items():
        data.setdefault(k, v)
    return data


def _cfg_save(data: dict) -> None:
    try:
        import json
        with open(CFG_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _cfg_get(key: str, default=None):
    data = _cfg_load()
    return data.get(key, _DEFAULTS_LOCAL.get(key, default))


def _cfg_set(key: str, value):
    data = _cfg_load()
    data[key] = value
    _cfg_save(data)


DATE_COLS = ["DATA INDICAÇÃO", "BOLETO", "SGU"]  # datas oficiais
STATUS_COLOR = {
    "Pago": QColor("#2ecc71"),
    "Pendente": QColor("#ffd166"),
    "Vencido": QColor("#ef5350"),
    "": QColor("#BDBDBD"),
}

import os, csv
import pandas as pd

def read_table_any(path: str) -> pd.DataFrame:

    if not path or not os.path.exists(path):
        return pd.DataFrame()
    try:
        lower = path.lower()
        if lower.endswith(".csv"):
            # Detecta encoding
            with open(path, "rb") as fh:
                head = fh.read(4096)
            encoding = None
            for enc in ("utf-8-sig", "utf-8"):
                try:
                    head.decode(enc)
                    encoding = enc
                    break
                except Exception:
                    pass
            if not encoding:
                encoding = "latin1"

            try:
                sample = head.decode(encoding, errors="ignore")
                dialect = csv.Sniffer().sniff(sample, delimiters=";,")
                sep = dialect.delimiter
            except Exception:
                txt = sample if "sample" in locals() else ""
                sep = ";" if (txt.count(";") >= txt.count(",")) else ","

            df = pd.read_csv(path, sep=sep, dtype=str, encoding=encoding).fillna("")
        else:
            df = pd.read_excel(path, dtype=str).fillna("")
        df.columns = [str(c).strip() for c in df.columns]
        return df
    except Exception:
        return pd.DataFrame()


def _norm(s: str) -> str:
    s = ''.join(ch for ch in unicodedata.normalize('NFKD', str(s or "")) if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", s.strip()).lower()


def _only_digits(s: str) -> str:
    return re.sub(r"\D+", "", str(s or ""))



def df_apply_global_texts(df: pd.DataFrame, texts: list[str]) -> pd.DataFrame:
    """
    Aplica filtro 'contém' em TODAS as colunas (case-insensitive).
    - texts: lista com 1+ caixas (pode haver várias).
    - Para cada caixa: TODOS os tokens precisam aparecer (AND) em ALGUMA coluna (OR entre colunas).
    """
    if df is None or df.empty:
        return df
    s_df = df.fillna("").astype(str).apply(lambda col: col.str.lower())
    mask_total = pd.Series(True, index=df.index)
    for text in texts:
        q = (text or "").strip().lower()
        if not q:
            continue
        tokens = [t for t in re.split(r"\s+", q) if t]
        if not tokens:
            continue
        mask_box = pd.Series(True, index=df.index)
        for tok in tokens:
            m_tok = pd.Series(False, index=df.index)
            pat = re.escape(tok)
            for c in s_df.columns:
                m_tok |= s_df[c].str.contains(pat, na=False)
            mask_box &= m_tok
        mask_total &= mask_box
    return df[mask_total].copy()


class CheckableComboBox(QComboBox):
    changed = pyqtSignal()

    def __init__(self, values):
        super().__init__()
        self.set_values(values)
        self.view().pressed.connect(self._toggle)
        self._update_text()

    def set_values(self, values):
        self.blockSignals(True)
        self.clear()
        vals = sorted({str(v) for v in values if str(v).strip()})
        if not vals:
            self.addItem("(vazio)")
            idx = self.model().index(0, 0)
            self.model().setData(idx, Qt.CheckState.Unchecked, Qt.ItemDataRole.CheckStateRole)
        else:
            for i, v in enumerate(vals):
                self.addItem(v)
                idx = self.model().index(i, 0)
                self.model().setData(idx, Qt.CheckState.Unchecked, Qt.ItemDataRole.CheckStateRole)
        self.blockSignals(False)
        self._update_text()

    def _toggle(self, index):
        st = self.model().data(index, Qt.ItemDataRole.CheckStateRole)
        ns = Qt.CheckState.Unchecked if st == Qt.CheckState.Checked else Qt.CheckState.Checked
        self.model().setData(index, ns, Qt.ItemDataRole.CheckStateRole)
        self._update_text()
        self.changed.emit()

    def selected_values(self):
        out = []
        for i in range(self.count()):
            idx = self.model().index(i, 0)
            st = self.model().data(idx, Qt.ItemDataRole.CheckStateRole)
            if st == Qt.CheckState.Checked:
                out.append(self.itemText(i))
        return out

    def _update_text(self):
        n = len(self.selected_values())
        self.setEditable(True)
        self.lineEdit().setReadOnly(True)
        self.lineEdit().setText("Todos" if n == 0 else f"{n} selecionados")
        self.setEditable(False)


# =============================================================================
# Helpers de data / parsing
# =============================================================================

def _parse_dt_any(s):
    s = str(s).strip()
    if not s:
        return QDate()
    for fmt in ["dd/MM/yyyy", "dd-MM-yyyy", "yyyy-MM-dd", "yyyy/MM/dd"]:
        q = QDate.fromString(s, fmt)
        if q.isValid():
            return q
    try:
        dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.notna(dt):
            return QDate(int(dt.year), int(dt.month), int(dt.day))
    except Exception:
        pass
    return QDate()


def to_qdate_flexible(val):
    if not isinstance(val, str) or not val.strip():
        return QDate()
    for fmt in ["dd-MM-yyyy", "dd/MM/yyyy", "yyyy-MM-dd", "yyyy/MM/dd"]:
        qd = QDate.fromString(val.strip(), fmt)
        if qd.isValid():
            return qd
    return QDate()


# =============================================================================
# Pastores (carregamento flexível)
# =============================================================================

def _pick_fase_pastores():
    pastores_dir = _cfg_get("pastores_dir")
    base = os.path.join(pastores_dir, "Notificações de Multas - Fase Pastores.xlsx")
    if os.path.exists(base):
        return base
    cands = []
    for p in ("*Fase*Pastor*.xls*", "*fase*pastor*.xls*"):
        cands += glob(os.path.join(pastores_dir, p))
    cands = [p for p in cands if os.path.isfile(p)]
    if not cands:
        return ""
    return max(cands, key=lambda p: os.path.getmtime(p))


def load_fase_pastores():
    path = _pick_fase_pastores()
    return load_fase_pastores_from(path)


def load_fase_pastores_from(path):
    if not path or not os.path.exists(path):
        return pd.DataFrame(columns=["FLUIG", "DATA_PASTORES", "TIPO"])
    try:
        df = pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")
    except Exception as e:
        QMessageBox.critical(None, "Erro", str(e))
        return pd.DataFrame(columns=["FLUIG", "DATA_PASTORES", "TIPO"])
    fcol = next((c for c in df.columns if "fluig" in c.lower()), None)
    dcol = next((c for c in df.columns if "data pastores" in c.lower() or ("pastores" in c.lower() and "data" in c.lower())), None)
    tcol = next((c for c in df.columns if "tipo" in c.lower()), None)
    if not fcol or not dcol or not tcol:
        return pd.DataFrame(columns=["FLUIG", "DATA_PASTORES", "TIPO"])
    out = df[[fcol, dcol, tcol]].copy()
    out.columns = ["FLUIG", "DATA_PASTORES", "TIPO"]
    for c in out.columns:
        out[c] = out[c].astype(str).str.strip()
    return out


# =============================================================================
# UI helpers
# =============================================================================

def _paint_status(item, status):
    if status:
        bg = STATUS_COLOR.get(status)
        if bg:
            item.setBackground(bg)
            yiq = (bg.red() * 299 + bg.green() * 587 + bg.blue() * 114) / 1000
            item.setForeground(QColor("#000000" if yiq >= 160 else "#FFFFFF"))


def parse_permissions(perms):
    if isinstance(perms, list):
        return perms
    if isinstance(perms, str):
        s = perms.strip()
        if s.lower() in ("todos", "all", "*", ""):
            return 'todos'
        try:
            return ast.literal_eval(s)
        except Exception:
            return [p.strip() for p in s.split(",") if p.strip()]
    return 'todos'


def apply_shadow(w, radius=20, blur=40, color=QColor(0, 0, 0, 100)):
    eff = QGraphicsDropShadowEffect()
    eff.setBlurRadius(blur)
    eff.setXOffset(0)
    eff.setYOffset(8)
    eff.setColor(color)
    w.setGraphicsEffect(eff)
    w.setStyleSheet(f"border-radius:{radius}px;")


# >>> Abrir pasta no SO (Explorer/Finder/Linux)

def open_folder(path: str) -> None:
    """
    Abre 'path' no gerenciador de arquivos do sistema.
    Tenta: os.startfile (Windows), xdg-open (Linux), open (macOS) e, por fim, QDesktopServices.
    """
    try:
        p = os.path.abspath(path)
        if not os.path.exists(p):
            os.makedirs(p, exist_ok=True)
        if os.name == "nt":
            os.startfile(p)  # type: ignore[attr-defined]
            return
        # POSIX
        if shutil.which("xdg-open"):
            os.system(f'xdg-open "{p}" >/dev/null 2>&1 &')
            return
        if shutil.which("open"):
            os.system(f'open "{p}" >/dev/null 2>&1 &')
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(p))
    except Exception:
        try:
            QDesktopServices.openUrl(QUrl.fromLocalFile(path))
        except Exception:
            pass


# =============================================================================
# CSV / Multas helpers
# =============================================================================

def ensure_status_cols(df, csv_path=None):
    """
    Garante que existam, para cada coluna de data oficial em DATE_COLS,
    a própria coluna e a coluna de status correspondente (*_STATUS).
    Se csv_path for fornecido, persiste o CSV atualizado.
    """
    changed = False

    # ✅ nunca use "or" com DataFrame
    if isinstance(df, pd.DataFrame):
        df = df.copy()
    else:
        df = pd.DataFrame()

    for c in DATE_COLS:
        if c not in df.columns:
            df[c] = ""
            changed = True
        sc = f"{c}_STATUS"
        if sc not in df.columns:
            df[sc] = ""
            changed = True

    if changed and csv_path:
        try:
            df.to_csv(csv_path, index=False)
        except Exception:
            pass

    return df


def _multas_root():
    return _cfg_get("multas_root")


def _geral_multas_csv():
    return _cfg_get("geral_multas_csv")


def build_multa_dir(infrator, ano, mes, placa, notificacao, fluig):
    # sanitização leve para nome de pasta
    def _safe(s):
        s = str(s or "").strip()
        return re.sub(r'[\\/:*?"<>|]+', "_", s)
    sub = f"{_safe(placa)}_{_safe(notificacao)}_FLUIG({_safe(fluig)})"
    return os.path.join(_multas_root(), _safe(infrator), _safe(ano), _safe(mes), sub)


def gerar_geral_multas_csv(root=None, output=None):
    root = root or _multas_root()
    output = output or _geral_multas_csv()
    rows = []
    if os.path.isdir(root):
        for infrator in os.listdir(root):
            infrator_dir = os.path.join(root, infrator)
            if not os.path.isdir(infrator_dir):
                continue
            for ano in os.listdir(infrator_dir):
                ano_dir = os.path.join(infrator_dir, ano)
                if not os.path.isdir(ano_dir):
                    continue
                for mes in os.listdir(ano_dir):
                    mes_dir = os.path.join(ano_dir, mes)
                    if not os.path.isdir(mes_dir):
                        continue
                    for folder in os.listdir(mes_dir):
                        fdir = os.path.join(mes_dir, folder)
                        if not os.path.isdir(fdir):
                            continue
                        parts = folder.split('_')
                        placa = parts[0] if len(parts) > 0 else ""
                        notificacao = parts[1] if len(parts) > 1 else ""
                        fluig = ""
                        if len(parts) > 2 and '(' in parts[2] and ')' in parts[2]:
                            fluig = parts[2].split('(')[1].split(')')[0]
                        rows.append({
                            "INFRATOR": infrator,
                            "ANO": ano,
                            "MES": mes,
                            "PLACA": placa,
                            "NOTIFICACAO": notificacao,
                            "FLUIG": fluig,
                            "ORGÃO": "",
                            "DATA INDICAÇÃO": "",
                            "BOLETO": "",
                            "LANÇAMENTO NFF": "",
                            "VALIDACAO NFF": "",
                            "CONCLUSAO": "",
                            "SGU": "",
                        })
    df = pd.DataFrame(rows)
    df = ensure_status_cols(df)
    os.makedirs(os.path.dirname(output), exist_ok=True)
    df.to_csv(output, index=False)


def ensure_base_csv():
    out = _geral_multas_csv()
    if not os.path.exists(out):
        os.makedirs(os.path.dirname(out), exist_ok=True)
        gerar_geral_multas_csv()


def build_condutor_dir(nome: str, cpf: str) -> str:
    cond_root = _cfg_get("condutores_root", "")
    if not cond_root:
        return ""
    key = _only_digits(cpf) or _norm(nome) or "sem_identificacao"
    path = os.path.join(cond_root, key)
    return path


def link_multa_em_condutor(nome: str, cpf: str, path_da_multa: str):
    """
    Cria/atualiza o índice do condutor com um 'atalho' (arquivo .txt apontando para a pasta real da multa).
    Evita duplicação de PDFs.
    """
    cond_dir = build_condutor_dir(nome, cpf)
    if not cond_dir:
        return
    os.makedirs(cond_dir, exist_ok=True)
    base = os.path.basename(os.path.normpath(path_da_multa))
    atalho = os.path.join(cond_dir, f"{base} - LINK.txt")
    try:
        with open(atalho, "w", encoding="utf-8") as f:
            f.write(path_da_multa)
    except Exception:
        pass


# =============================================================================
# DIÁLOGOS (antes em dialogs.py)
# =============================================================================

class SummaryDialog(QDialog):
    def __init__(self, df):
        super().__init__()
        self.setWindowTitle("Visão Geral")
        self.resize(640, 480)
        v = QVBoxLayout(self)
        card = QFrame()
        card.setObjectName("card")
        apply_shadow(card, radius=16)
        cv = QVBoxLayout(card)
        t = QTableWidget()
        t.setColumnCount(2)
        t.setHorizontalHeaderLabels(["Coluna", "Resumo"])
        resumo = []
        for col in df.columns:
            try:
                s = pd.to_numeric(df[col], errors="coerce").sum()
                resumo.append((col, f"{s}"))
            except Exception:
                resumo.append((col, f"{df[col].nunique()}"))
        t.setRowCount(len(resumo))
        for i, (c, val) in enumerate(resumo):
            t.setItem(i, 0, QTableWidgetItem(c))
            t.setItem(i, 1, QTableWidgetItem(val))
        t.resizeColumnsToContents()
        cv.addWidget(t)
        v.addWidget(card)
        close = QPushButton("Fechar")
        v.addWidget(close)
        close.clicked.connect(self.accept)


class ConferirFluigDialog(QDialog):
    def __init__(self, parent, df_left, df_right):
        super().__init__(parent)
        self.setWindowTitle("Conferir FLUIG")
        self.resize(1200, 680)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)
        wrap = QVBoxLayout(self)

        top = QHBoxLayout()
        l = QLabel(f"Abertas no Detalhamento e faltando no CSV: {len(df_left)}")
        r = QLabel(f"No CSV e não no Detalhamento (abertas): {len(df_right)}")
        l.setObjectName("headline"); r.setObjectName("headline")
        top.addWidget(l); top.addStretch(1); top.addWidget(r)
        wrap.addLayout(top)

        tools = QHBoxLayout()
        self.left_search = QLineEdit(); self.left_search.setPlaceholderText("Filtrar esquerda...")
        self.right_search = QLineEdit(); self.right_search.setPlaceholderText("Filtrar direita...")
        tools.addWidget(self.left_search); tools.addStretch(1); tools.addWidget(self.right_search)
        wrap.addLayout(tools)

        split = QSplitter()
        gb_l = QGroupBox("Detalhamento → faltando no CSV")
        gb_r = QGroupBox("CSV → faltando no Detalhamento")
        apply_shadow(gb_l, radius=14); apply_shadow(gb_r, radius=14)
        lv = QVBoxLayout(gb_l); rv = QVBoxLayout(gb_r)

        self.tbl_left = QTableWidget(); self.tbl_right = QTableWidget()
        for tbl in (self.tbl_left, self.tbl_right):
            tbl.setAlternatingRowColors(True)
            tbl.setSortingEnabled(True)
            tbl.horizontalHeader().setSortIndicatorShown(True)
            tbl.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
            tbl.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
            tbl.setSelectionMode(QTableWidget.SelectionMode.ExtendedSelection)
        lv.addWidget(self.tbl_left); rv.addWidget(self.tbl_right)
        split.addWidget(gb_l); split.addWidget(gb_r)
        wrap.addWidget(split)

        actions = QHBoxLayout()
        self.btn_copy_left = QPushButton("Copiar FLUIG (esquerda)")
        self.btn_insert_left = QPushButton("Inserir selecionados")
        self.btn_copy_right = QPushButton("Copiar FLUIG (direita)")
        actions.addWidget(self.btn_copy_left)
        actions.addWidget(self.btn_insert_left)
        actions.addStretch(1)
        actions.addWidget(self.btn_copy_right)
        wrap.addLayout(actions)

        self.df_left = df_left.reset_index(drop=True)
        self.df_right = df_right.reset_index(drop=True)
        self._fill(self.tbl_left, self.df_left, actions_col=True)
        self._fill(self.tbl_right, self.df_right, actions_col=False)

        self.left_search.textChanged.connect(lambda: self._filter(self.tbl_left, self.df_left, self.left_search.text(), actions_col=True))
        self.right_search.textChanged.connect(lambda: self._filter(self.tbl_right, self.df_right, self.right_search.text(), actions_col=False))
        self.btn_copy_left.clicked.connect(lambda: self._copy(self.tbl_left))
        self.btn_copy_right.clicked.connect(lambda: self._copy(self.tbl_right))
        self.btn_insert_left.clicked.connect(self._insert_selected)

    def _fill(self, tbl, df, actions_col=False):
        tbl.clear()
        cols = [str(c) for c in df.columns]
        if actions_col:
            cols = cols + ["Ações"]
        tbl.setColumnCount(len(cols))
        tbl.setRowCount(len(df))
        tbl.setHorizontalHeaderLabels(cols)

        fcol = None
        for c in df.columns:
            cl = str(c).lower()
            if cl.startswith("nº fluig") or cl == "fluig":
                fcol = c
                break

        for i in range(len(df)):
            for j, c in enumerate(df.columns):
                it = QTableWidgetItem(str(df.iat[i, j]))
                it.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
                tbl.setItem(i, j, it)
            if actions_col and fcol is not None:
                code = str(df.at[i, fcol]).strip()
                btn = QPushButton("INSERIR")
                btn.clicked.connect(lambda _, k=code: self._insert_one(k))
                tbl.setCellWidget(i, len(cols) - 1, btn)

        tbl.resizeColumnsToContents()
        tbl.resizeRowsToContents()

    def _filter(self, tbl, base, text, actions_col=False):
        tx = text.strip().lower()
        if not tx:
            df = base
        else:
            mask = base.apply(lambda r: any(tx in str(x).lower() for x in r.values), axis=1)
            df = base[mask]
        self._fill(tbl, df.reset_index(drop=True), actions_col=actions_col)

    def _copy(self, tbl):
        from PyQt6.QtWidgets import QApplication
        if tbl.rowCount() == 0:
            QApplication.clipboard().setText("")
            return
        col = None
        for j in range(tbl.columnCount()):
            name = tbl.horizontalHeaderItem(j).text().lower()
            if name.startswith("nº fluig") or name == "fluig":
                col = j
                break
        if col is None:
            QApplication.clipboard().setText("")
            return
        rows = tbl.selectionModel().selectedRows()
        vals = [tbl.item(r.row(), col).text() for r in rows] if rows else [tbl.item(r, col).text() for r in range(tbl.rowCount())]
        QApplication.clipboard().setText("\n".join(vals))

    def _insert_one(self, code):
        if not code:
            return
        try:
            self.parent().inserir(code)
        except Exception:
            pass
        try:
            fcol = next((c for c in self.df_left.columns if str(c).lower().startswith("nº fluig") or str(c).lower() == "fluig"), None)
            if fcol:
                self.df_left = self.df_left[self.df_left[fcol].astype(str).str.strip() != str(code).strip()].reset_index(drop=True)
                self._fill(self.tbl_left, self.df_left, actions_col=True)
        except Exception:
            pass

    def _insert_selected(self):
        col = None
        for j in range(self.tbl_left.columnCount()):
            name = self.tbl_left.horizontalHeaderItem(j).text().lower()
            if name.startswith("nº fluig") or name == "fluig":
                col = j; break
        if col is None:
            return
        rows = self.tbl_left.selectionModel().selectedRows()
        if not rows:
            return
        codes = [self.tbl_left.item(r.row(), col).text() for r in rows]
        for code in codes:
            self._insert_one(code)


class AlertasDialog(QDialog):
    def __init__(self, parent, df_alertas):
        super().__init__(parent)
        self.setWindowTitle("Alertas de Datas")
        self.resize(960, 560)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)

        v = QVBoxLayout(self)
        card = QFrame(); card.setObjectName("glass"); apply_shadow(card, radius=18, blur=60, color=QColor(0, 0, 0, 60))
        cv = QVBoxLayout(card)

        t = QTableWidget()
        t.setAlternatingRowColors(True)
        t.setSortingEnabled(True)
        t.horizontalHeader().setSortIndicatorShown(True)
        t.setColumnCount(7)
        t.setHorizontalHeaderLabels(["FLUIG", "INFRATOR", "PLACA", "ORGÃO", "ETAPA", "DATA", "STATUS"])
        t.setRowCount(len(df_alertas))
        for r, row in enumerate(df_alertas):
            for c, val in enumerate(row):
                it = QTableWidgetItem(val)
                if c == 6 and val in STATUS_COLOR:
                    _paint_status(it, val)
                t.setItem(r, c, it)
        t.resizeColumnsToContents(); t.resizeRowsToContents()
        cv.addWidget(t)
        v.addWidget(card)

        close = QPushButton("Fechar"); close.clicked.connect(self.accept)
        v.addWidget(close)

class GlobalFilterBar(QFrame):
    changed = pyqtSignal()

    def __init__(self, label_text="Filtro global:"):
        super().__init__()
        self.setObjectName("card")
        apply_shadow(self, radius=16)
        self._edits = []

        lay = QHBoxLayout(self)
        lay.setContentsMargins(8, 8, 8, 8)

        lab = QLabel(label_text)
        lay.addWidget(lab)

        self._add_edit(lay)

        btn_plus = QPushButton("+")
        btn_plus.setFixedWidth(28)
        btn_plus.clicked.connect(lambda: self._add_edit(lay))
        lay.addWidget(btn_plus)

    def _add_edit(self, lay):
        le = QLineEdit()
        le.setPlaceholderText("Digite para filtrar em TODAS as colunas…")
        le.textChanged.connect(self.changed.emit)
        self._edits.append(le)
        lay.addWidget(le, 1)

    def values(self):
        return [e.text() for e in self._edits if e.text().strip()]