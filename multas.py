# multas.py — COMPLETO (Cenário Geral pelas planilhas; CSV apenas p/ lógica operacional)
import os, re, shutil, json
import pandas as pd
from PyQt6.QtCore import Qt, QDate, QTimer, QFileSystemWatcher, QUrl
from PyQt6.QtGui import QColor, QFont, QFontMetrics, QDesktopServices
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QFrame, QHBoxLayout, QLabel, QComboBox, QLineEdit,
    QTableWidget, QTableWidgetItem, QHeaderView, QPushButton, QMessageBox,
    QDialog, QFormLayout, QFileDialog, QSizePolicy, QScrollArea, QInputDialog,
    QDateEdit, QCompleter, QTabWidget
)

import sys, glob

from gestao_frota_single import (
    DATE_FORMAT, DATE_COLS, STATUS_COLOR,
    GERAL_MULTAS_CSV, MULTAS_ROOT, PASTORES_DIR, ORGAOS,
    PORTUGUESE_MONTHS,
    cfg_get, cfg_set, cfg_all
)
from utils import (
    ensure_status_cols, apply_shadow, _paint_status, to_qdate_flexible,
    build_multa_dir, _parse_dt_any, CheckableComboBox, SummaryDialog, ConferirFluigDialog,
    link_multa_em_condutor
)

# ====== Configs locais ======
DATE_COLS_MUL = ["DATA INDICAÇÃO", "BOLETO", "SGU"]
IGNORED_COLS = {"LANÇAMENTOS DE NFF", "VALIDAÇÃO", "CONCLUSÃO", "LANÇAMENTO NFF", "VALIDACAO NFF", "CONCLUSAO"}

# -------- Utilidades locais --------
def _num(s):
    s = str(s or "").strip()
    if not s:
        return 0.0
    s = re.sub(r"[^\d,.-]", "", s)
    s = s.replace(".", "").replace(",", ".") if ("," in s and "." in s) else s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def _to_date(s):
    s = str(s or "").strip()
    if not s:
        return pd.NaT
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
        if s:
            return s
    return ""

def _most_frequent_placa(series: pd.Series) -> str:
    vals = series.fillna("").astype(str).tolist()
    if not vals:
        return ""
    norm_map = {}
    for v in vals:
        n = _norm_placa(v)
        norm_map.setdefault(n, []).append(v.strip())
    if not norm_map:
        return ""
    key = max(norm_map.keys(), key=lambda k: len(norm_map[k]))
    formas = norm_map[key]
    with_hyphen = [f for f in formas if "-" in f]
    return with_hyphen[0] if with_hyphen else formas[0]

def _fmt_money(x):
    return f"{float(x or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# ============================================================
# ==============  ENGINE (PLANILHAS → CONSOLIDADO) ===========
# ============================================================
class PlanilhasEngine:
    """
    Lê as planilhas de multas, consolida por FLUIG (1 linha por multa),
    marca DESCONTADA a partir da planilha Fase Pastores,
    e calcula KPIs. Não usa CSV.
    """

    # nomes usados no cfg_get / cfg_set (mantém com o que você usar na aba Base)
    KEY_DETALHAMENTO   = "detalhamento_path"
    KEY_DETALHAMENTO2  = "detalhamento2_path"
    KEY_DETALHAMENTO1  = "detalhamento1_path"
    KEY_FASE_PASTORES  = "pastores_file"
    KEY_COND_IDENT     = "condutor_ident_path"

    # rótulos amigáveis
    LABELS = {
        KEY_DETALHAMENTO:  "Notificações de Multas - Detalhamento.xlsx",
        KEY_DETALHAMENTO2: "Notificações de Multas - Detalhamento-2.xlsx",
        KEY_DETALHAMENTO1: "Notificações de Multas - Detalhamento (1).xlsx",
        KEY_FASE_PASTORES: "Notificações de Multas - Fase Pastores.xlsx",
        KEY_COND_IDENT:    "Notificações de Multas - Condutor Identificado.xlsx",
    }

    # alias curto exibido na coluna "Fontes"
    ALIAS = {
        KEY_DETALHAMENTO:  "Detalhamento",
        KEY_DETALHAMENTO2: "Detalhamento-2",
        KEY_DETALHAMENTO1: "Detalhamento(1)",
        KEY_FASE_PASTORES: "Fase Pastores",
        KEY_COND_IDENT:    "Condutor Identificado",
    }

    def __init__(self, parent=None):
        self.parent = parent

    # dentro de PlanilhasEngine



    def _ensure_path(self, key_cfg: str) -> str:
        # 1) tenta caminho salvo
        p = cfg_get(key_cfg)
        if p and os.path.exists(p):
            return p
        
        
        def _app_root():
            # pasta do executável/py principal (mais estável que cwd)
            try:
                base = os.path.dirname(os.path.abspath(sys.argv[0]))
            except Exception:
                base = os.getcwd()
            return base

        # 2) tenta achar automaticamente na RAIZ pelos nomes esperados
        root = _app_root()
        # nomes "sugeridos" (o LABELS já tem o nome padrão)
        prefer = self.LABELS.get(key_cfg, "")
        candidates = []

        # (a) exatamente o nome preferido na raiz
        if prefer:
            candidates.append(os.path.join(root, prefer))

        # (b) variações na raiz (xls/xlsx/xlsm) com “palavras-chave”
        # ex.: "Detalhamento", "Fase Pastores", "Condutor Identificado"
        kw = self.ALIAS.get(key_cfg, key_cfg)  # usa o alias como palavra-chave
        patterns = [
            f"*{kw}*.xlsx", f"*{kw}*.xls", f"*{kw}*.xlsm",
            f"*{kw.replace(' ', '*')}*.xlsx",  # tolera espaços/variações
        ]
        seen = set()
        for pat in patterns:
            for f in glob.glob(os.path.join(root, pat)):
                if f not in seen and os.path.isfile(f):
                    candidates.append(f); seen.add(f)

        # 3) primeiro candidato válido
        for c in candidates:
            if os.path.exists(c):
                cfg_set(key_cfg, c)  # grava para as próximas vezes
                return c

        # 4) por fim, pergunta via diálogo
        sel, _ = QFileDialog.getOpenFileName(
            self.parent,
            f"Selecione a planilha — {self.LABELS.get(key_cfg, key_cfg)}",
            root,
            "Arquivos Excel (*.xlsx *.xls *.xlsm)"
        )
        if sel:
            cfg_set(key_cfg, sel)
            return sel
        return ""


    # ---------- Lê 1 fonte e normaliza ----------
    def _read_one_source(self, key_cfg: str, filtro_nome: str = "") -> pd.DataFrame:
        path = self._ensure_path(key_cfg)
        if not path or not os.path.exists(path):
            return pd.DataFrame()
        try:
            df = pd.read_excel(path, dtype=str).fillna("")
        except Exception as e:
            QMessageBox.warning(self.parent, "Planilhas", f"Erro ao ler {os.path.basename(path)}:\n{e}")
            return pd.DataFrame()

        # filtro por nome, quando houver coluna compatível
        if filtro_nome.strip():
            col_nome = None
            for c in ("Nome", "NOME", "INFRATOR", "Infrator"):
                if c in df.columns:
                    col_nome = c; break
            if col_nome:
                df = df[df[col_nome].astype(str).str.contains(re.escape(filtro_nome), case=False, na=False)]

        # ignora CANCELADA (se houver)
        col_status = next((c for c in df.columns if c.strip().lower() == "status"), None)
        if col_status:
            df = df[df[col_status].astype(str).str.upper() != "CANCELADA"]

        # colunas chave detectadas dinamicamente
        col_fluig = next((c for c in df.columns if "FLUIG" in c.upper()), None)
        col_data  = next((c for c in df.columns if "DATA INFRA" in c.upper()), None)
        col_valor = next((c for c in df.columns if "VALOR TOTAL" in c.upper()), None)
        col_inf   = next((c for c in df.columns if c.upper() in ("INFRAÇÃO", "INFRACAO")), None)
        col_placa = next((c for c in df.columns if c.strip().upper() == "PLACA"), None)

        if col_fluig is None:
            return pd.DataFrame()

        out = pd.DataFrame()
        out["FLUIG"]    = df[col_fluig].astype(str)
        out["Status"]   = df.get(col_status, "")
        out["Data"]     = df.get(col_data, "")
        out["Placa"]    = df.get(col_placa, "")
        out["Infração"] = df.get(col_inf, "")
        out["Valor"]    = df.get(col_valor, df.get("Valor", ""))

        out["VALOR_NUM"] = out["Valor"].map(_num)
        out["DT_M"]      = out["Data"].map(_to_date)
        out["FONTE"]     = self.ALIAS.get(key_cfg, os.path.basename(path))
        out["PLACA_N"]   = out["Placa"].map(_norm_placa)

        # marcação de DESCONTADA pela Fase Pastores
        if key_cfg == self.KEY_FASE_PASTORES and len(out):
            col_tipo = next((c for c in df.columns if c.strip().upper() == "TIPO"), None)
            col_data_past = next((c for c in df.columns if "DATA" in c.upper() and "PASTOR" in c.upper()), None)
            tipo = df.get(col_tipo, "")
            data_p = df.get(col_data_past, "")
            disc = (tipo.astype(str).str.upper() == "MULTAS PASTORES") & (data_p.astype(str).str.strip() != "")
            disc = disc.reindex(df.index).fillna(False)
            out["DESCONTADA"] = disc.astype(bool).values[:len(out)]
        else:
            out["DESCONTADA"] = False

        return out

    # ---------- Consolidação por FLUIG ----------
    def load_consolidated(self, filtro_nome: str = "", data_ini=None, data_fim=None):
        all_keys = [
            self.KEY_DETALHAMENTO,
            self.KEY_DETALHAMENTO2,
            self.KEY_DETALHAMENTO1,
            self.KEY_FASE_PASTORES,
            self.KEY_COND_IDENT,
        ]

        frames = []
        presentes = {}  # fonte -> set(FLUIG)
        for key in all_keys:
            df = self._read_one_source(key, filtro_nome=filtro_nome)
            if df is None or df.empty:
                continue
            frames.append(df)
            presentes[self.ALIAS.get(key, key)] = set(df["FLUIG"].astype(str).unique())

        if not frames:
            base = pd.DataFrame(columns=["FONTE","FLUIG","Status","Data","Placa","Infração","Valor","VALOR_NUM","DT_M","DESCONTADA","PLACA_N"])
        else:
            base = pd.concat(frames, ignore_index=True)

        # 1 linha por FLUIG
        if base.empty:
            consolidated = base.copy()
        else:
            def agg_fontes(s):    return ", ".join(sorted(set(x for x in s if str(x).strip())))
            def agg_valornum(s):  return max([float(x) for x in s if pd.notna(x)] or [0.0])
            def agg_data(s):
                ss = [x for x in s if pd.notna(x)]
                return min(ss) if ss else pd.NaT

            grp = base.groupby(base["FLUIG"].astype(str), dropna=False)
            consolidated = pd.DataFrame({
                "FLUIG": grp.apply(lambda g: str(g.name)),
                "Fontes": grp["FONTE"].apply(agg_fontes),
                "Status": grp["Status"].apply(_first_nonempty),
                "DT_M": grp["DT_M"].apply(agg_data),
                "Data": grp["Data"].apply(_first_nonempty),
                "Placa": grp["Placa"].apply(_most_frequent_placa),
                "Infração": grp["Infração"].apply(_first_nonempty),
                "Valor": grp["Valor"].apply(_first_nonempty),
                "VALOR_NUM": grp["VALOR_NUM"].apply(agg_valornum),
                "DESCONTADA": grp["DESCONTADA"].apply(lambda s: bool(s.astype(bool).any())),
            }).reset_index(drop=True)

        # recorte de período por DT_M (opcional)
        if data_ini is not None and data_fim is not None and not consolidated.empty:
            a = pd.to_datetime(data_ini).normalize()
            b = pd.to_datetime(data_fim).normalize()
            if a > b: a, b = b, a
            consolidated = consolidated[(consolidated["DT_M"].notna()) & (consolidated["DT_M"] >= a) & (consolidated["DT_M"] <= b)]

        # matriz de presença (FLUIG × Fonte)
        presence = pd.DataFrame()
        if not base.empty:
            sources = sorted(base["FONTE"].unique().tolist())
            presence = pd.DataFrame(index=sorted(base["FLUIG"].astype(str).unique()), columns=sources)
            presence[:] = ""
            for src, vals in presentes.items():
                for fl in vals:
                    if str(fl) in presence.index:
                        presence.loc[str(fl), src] = "✓"
            presence.index.name = "FLUIG"

        # KPIs (pós-período)
        if consolidated.empty:
            kpis = {"descontado": 0.0, "pendente": 0.0, "qtd": 0}
        else:
            desc = float(consolidated.loc[consolidated["DESCONTADA"], "VALOR_NUM"].sum() or 0.0)
            pend = float(consolidated.loc[~consolidated["DESCONTADA"], "VALOR_NUM"].sum() or 0.0)
            kpis = {"descontado": desc, "pendente": pend, "qtd": int(len(consolidated))}
        return consolidated, presence, kpis

# ============================================================
# =================  DIÁLOGOS CSV (operacional) ==============
# ============================================================
class InserirDialog(QDialog):
    def __init__(self, parent, prefill_fluig=None):
        super().__init__(parent)
        self.setWindowTitle("Inserir Multa")
        self.resize(720, 560)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)

        self._csv = cfg_get("geral_multas_csv")
        os.makedirs(os.path.dirname(self._csv), exist_ok=True)
        self.df = ensure_status_cols(pd.read_csv(self._csv, dtype=str).fillna(""), csv_path=self._csv)

        if "COMENTARIO" not in self.df.columns:
            self.df["COMENTARIO"] = ""
            self.df.to_csv(self._csv, index=False)

        form = QFormLayout(self)
        self.widgets = {}

        base_fields = [c for c in self.df.columns if not c.endswith("_STATUS") and c not in IGNORED_COLS and c != "FLUIG"]
        fields = ["FLUIG"] + base_fields

        for c in fields:
            if c in DATE_COLS_MUL:
                d = QDateEdit(); d.setCalendarPopup(True); d.setDisplayFormat(DATE_FORMAT)
                d.setMinimumDate(QDate(1752, 9, 14)); d.setSpecialValueText("")
                d.setDate(d.minimumDate())
                s = QComboBox(); s.addItems(["", "Pendente", "Pago", "Vencido"])
                box = QWidget(); hb = QHBoxLayout(box); hb.setContentsMargins(0, 0, 0, 0); hb.addWidget(d); hb.addWidget(s)
                form.addRow(c, box); self.widgets[c] = (d, s)
            elif c == "ORGÃO":
                cb = QComboBox(); cb.addItems(ORGAOS); form.addRow(c, cb); self.widgets[c] = cb
            else:
                w = QLineEdit()
                if c == "FLUIG":
                    comp = QCompleter(sorted(self.df["FLUIG"].dropna().astype(str).unique()))
                    comp.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
                    w.setCompleter(comp)
                    w.editingFinished.connect(lambda le=w: self.on_fluig_leave(le))
                form.addRow(c, w); self.widgets[c] = w

        bar = QHBoxLayout()
        btn_save = QPushButton("Salvar"); btn_pdf = QPushButton("Anexar PDF"); btn_close = QPushButton("Fechar")
        bar.addWidget(btn_save); bar.addStretch(1); bar.addWidget(btn_pdf); bar.addWidget(btn_close)
        form.addRow(bar)
        btn_save.clicked.connect(self.salvar)
        btn_pdf.clicked.connect(self.anexar_pdf)
        btn_close.clicked.connect(self.reject)

        if prefill_fluig:
            self.widgets["FLUIG"].setText(str(prefill_fluig).strip())
            self.on_fluig_leave(self.widgets["FLUIG"])

    def _apply_fase_pastores(self, code):
        path = cfg_get(PlanilhasEngine.KEY_FASE_PASTORES)
        try:
            dfp = pd.read_excel(path, dtype=str).fillna("")
        except Exception:
            return
        fcol = next((c for c in dfp.columns if "fluig" in c.lower()), None)
        dcol = next((c for c in dfp.columns if "data" in c.lower() and "pastor" in c.lower()), None)
        tcol = next((c for c in dfp.columns if "tipo" in c.lower()), None)
        if not fcol or not dcol or not tcol:
            return
        row = dfp[dfp[fcol].astype(str).str.strip().eq(str(code).strip())]
        if row.empty:
            return
        tipo = str(row[tcol].iloc[0]).upper()
        data = str(row[dcol].iloc[0]).strip()
        if ("PASTOR" in tipo) and data and "SGU" in self.widgets:
            de, se = self.widgets["SGU"]
            qd = _parse_dt_any(data)
            if qd.isValid():
                de.setDate(qd)
                se.setCurrentText("Pago")

    def on_fluig_leave(self, le: QLineEdit):
        code = str(le.text()).strip()
        if code and code in self.df["FLUIG"].astype(str).tolist():
            QMessageBox.warning(self, "Erro", "FLUIG já existe"); le.clear(); return
        # tenta preencher a partir da planilha de Detalhamento
        try:
            det = cfg_get(PlanilhasEngine.KEY_DETALHAMENTO)
            if not det or not os.path.exists(det):
                det, _ = QFileDialog.getOpenFileName(self, "Selecione Detalhamento", "", "Arquivos Excel (*.xlsx *.xls)")
                if det:
                    cfg_set(PlanilhasEngine.KEY_DETALHAMENTO, det)
            if det:
                x = pd.read_excel(
                    det,
                    usecols=["Nº Fluig", "Placa", "Nome", "AIT", "Data Infração", "Data Limite", "Status"],
                    dtype=str
                ).fillna("")
            else:
                x = pd.DataFrame()
        except Exception:
            x = pd.DataFrame()

        if x.empty:
            self._apply_fase_pastores(code)
            return

        row = x[x["Nº Fluig"].astype(str).str.strip() == code]
        if row.empty:
            self._apply_fase_pastores(code)
            return

        if "PLACA" in self.widgets:
            self.widgets["PLACA"].setText(row["Placa"].iloc[0])
        if "INFRATOR" in self.widgets:
            self.widgets["INFRATOR"].setText(row["Nome"].iloc[0])
        if "NOTIFICACAO" in self.widgets:
            self.widgets["NOTIFICACAO"].setText(row["AIT"].iloc[0])

        try:
            dt = pd.to_datetime(row["Data Infração"].iloc[0], dayfirst=False, errors="coerce")
            if pd.notna(dt):
                if "MES" in self.widgets:
                    self.widgets["MES"].setText(PORTUGUESE_MONTHS.get(dt.month, ""))
                if "ANO" in self.widgets:
                    self.widgets["ANO"].setText(str(dt.year))
        except Exception:
            pass

        try:
            d2 = pd.to_datetime(row["Data Limite"].iloc[0], dayfirst=False, errors="coerce")
            if pd.notna(d2) and "DATA INDICAÇÃO" in self.widgets and isinstance(self.widgets["DATA INDICAÇÃO"], tuple):
                de, _ = self.widgets["DATA INDICAÇÃO"]
                de.setDate(QDate(d2.year, d2.month, d2.day))
        except Exception:
            pass

        self._apply_fase_pastores(code)

    def salvar(self):
        new = {}
        for c, w in self.widgets.items():
            if isinstance(w, tuple):
                d, s = w
                new[c] = "" if d.date() == d.minimumDate() else d.date().toString(DATE_FORMAT)
                new[f"{c}_STATUS"] = s.currentText()
            else:
                new[c] = w.currentText() if isinstance(w, QComboBox) else w.text().strip()

        if new.get("FLUIG", "") in self.df["FLUIG"].astype(str).tolist():
            QMessageBox.warning(self, "Erro", "FLUIG já existe"); return

        self.df.loc[len(self.df)] = new
        csv = cfg_get("geral_multas_csv")
        os.makedirs(os.path.dirname(csv), exist_ok=True)
        if "COMENTARIO" not in self.df.columns:
            self.df["COMENTARIO"] = ""
        self.df.to_csv(csv, index=False)

        try:
            infr, ano, mes = new.get("INFRATOR", ""), new.get("ANO", ""), new.get("MES", "")
            placa, notificacao, fluig = new.get("PLACA", ""), new.get("NOTIFICACAO", ""), new.get("FLUIG", "")
            dest = build_multa_dir(infr, ano, mes, placa, notificacao, fluig)
            os.makedirs(dest, exist_ok=True)
            cpf = new.get("CPF", "")
            link_multa_em_condutor(infr, cpf, dest)
        except Exception as e:
            QMessageBox.warning(self, "Aviso", f"Falha ao criar a pasta da multa: {e}")

        self.anexar_pdf()
        QMessageBox.information(self, "Sucesso", "Multa inserida.")
        self.accept()

    def anexar_pdf(self):
        try:
            infr = self.widgets.get("INFRATOR")
            ano  = self.widgets.get("ANO")
            mes  = self.widgets.get("MES")
            placa = self.widgets.get("PLACA")
            notif = self.widgets.get("NOTIFICACAO")
            fluig = self.widgets.get("FLUIG")

            if not (infr and ano and mes and placa and notif and fluig):
                return

            infr, ano, mes = infr.text().strip(), ano.text().strip(), mes.text().strip()
            placa, notif, fluig = placa.text().strip(), notif.text().strip(), fluig.text().strip()
            if not all([infr, ano, mes, placa, notif, fluig]):
                return

            dest = build_multa_dir(infr, ano, mes, placa, notif, fluig)
            os.makedirs(dest, exist_ok=True)
            pdf, _ = QFileDialog.getOpenFileName(self, "Selecione PDF", "", "PDF Files (*.pdf)")
            if pdf:
                shutil.copy(pdf, os.path.join(dest, os.path.basename(pdf)))
        except Exception:
            pass

class EditarDialog(QDialog):
    def __init__(self, parent):
        super().__init__(parent)
        self.setWindowTitle("Editar Multa")
        self.resize(720, 560)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)

        v = QVBoxLayout(self)
        top = QHBoxLayout()

        csv = cfg_get("geral_multas_csv")
        os.makedirs(os.path.dirname(csv), exist_ok=True)
        self.df = ensure_status_cols(pd.read_csv(csv, dtype=str).fillna(""), csv_path=csv)
        if "COMENTARIO" not in self.df.columns:
            self.df["COMENTARIO"] = ""
            self.df.to_csv(csv, index=False)

        self.le_key = QLineEdit(); self.le_key.setPlaceholderText("Digite FLUIG para carregar")
        comp = QCompleter(sorted(self.df["FLUIG"].dropna().astype(str).unique()))
        comp.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.le_key.setCompleter(comp)

        btn_load = QPushButton("Carregar")
        top.addWidget(self.le_key); top.addWidget(btn_load)
        v.addLayout(top)

        self.formw = QWidget(); self.form = QFormLayout(self.formw)
        self.widgets = {}
        v.addWidget(self.formw)

        bar = QHBoxLayout()
        btn_save = QPushButton("Salvar"); btn_close = QPushButton("Fechar")
        bar.addWidget(btn_save); bar.addStretch(1); bar.addWidget(btn_close)
        v.addLayout(bar)

        btn_load.clicked.connect(self.load_record)
        btn_save.clicked.connect(self.save_record)
        btn_close.clicked.connect(self.reject)

    def load_record(self):
        key = self.le_key.text().strip()
        if not key:
            return

        csv = cfg_get("geral_multas_csv")
        self.df = ensure_status_cols(pd.read_csv(csv, dtype=str).fillna(""), csv_path=csv)
        if "COMENTARIO" not in self.df.columns:
            self.df["COMENTARIO"] = ""

        rows = self.df.index[self.df.get("FLUIG", pd.Series([], dtype=str)).astype(str) == key].tolist()
        if not rows:
            QMessageBox.warning(self, "Aviso", "FLUIG não encontrado")
            return
        i = rows[0]

        while self.form.count():
            item = self.form.takeAt(0)
            w = item.widget()
            if w:
                w.deleteLater()
        self.widgets.clear()

        cols_sem_status = [col for col in self.df.columns if not str(col).endswith("_STATUS") and col not in IGNORED_COLS]

        for col in cols_sem_status:
            if col in self.widgets:
                continue
            if col in DATE_COLS_MUL:
                d = QDateEdit()
                d.setCalendarPopup(True)
                d.setDisplayFormat(DATE_FORMAT)
                d.setMinimumDate(QDate(1752, 9, 14))
                d.setSpecialValueText("")
                qd = to_qdate_flexible(self.df.at[i, col])
                d.setDate(qd if qd.isValid() else d.minimumDate())
                s = QComboBox(); s.addItems(["", "Pendente", "Pago", "Vencido"])
                s.setCurrentText(self.df.at[i, f"{col}_STATUS"] if f"{col}_STATUS" in self.df.columns else "")
                box = QWidget(); hb = QHBoxLayout(box); hb.setContentsMargins(0, 0, 0, 0); hb.addWidget(d); hb.addWidget(s)
                self.form.addRow(col, box)
                self.widgets[col] = (d, s)
            elif col == "ORGÃO":
                cb = QComboBox(); cb.addItems(ORGAOS); cb.setCurrentText(self.df.at[i, col])
                self.form.addRow(col, cb)
                self.widgets[col] = cb
            else:
                w = QLineEdit(str(self.df.at[i, col]))
                self.form.addRow(col, w)
                self.widgets[col] = w

        self.current_index = i

    def save_record(self):
        if not hasattr(self, "current_index"):
            return
        i = self.current_index
        for c, w in self.widgets.items():
            if isinstance(w, tuple):
                d, s = w
                self.df.at[i, c] = "" if d.date() == d.minimumDate() else d.date().toString(DATE_FORMAT)
                self.df.at[i, f"{c}_STATUS"] = s.currentText()
            else:
                self.df.at[i, c] = w.currentText() if isinstance(w, QComboBox) else w.text().strip()
        self.df.to_csv(cfg_get("geral_multas_csv"), index=False)
        QMessageBox.information(self, "Sucesso", "Multa editada.")
        self.accept()

class ExcluirDialog(QDialog):
    def __init__(self, parent):
        super().__init__(parent)
        self.setWindowTitle("Excluir Multa")
        self.resize(520, 160)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)

        v = QVBoxLayout(self)
        top = QHBoxLayout()

        csv = cfg_get("geral_multas_csv")
        self.df = ensure_status_cols(pd.read_csv(csv, dtype=str).fillna(""), csv_path=csv)
        if "COMENTARIO" not in self.df.columns:
            self.df["COMENTARIO"] = ""
            self.df.to_csv(csv, index=False)

        self.le_key = QLineEdit(); self.le_key.setPlaceholderText("Digite FLUIG para excluir")
        comp = QCompleter(sorted(self.df["FLUIG"].dropna().astype(str).unique()))
        comp.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.le_key.setCompleter(comp)

        btn_delete = QPushButton("Excluir")
        top.addWidget(self.le_key); top.addWidget(btn_delete)
        v.addLayout(top)

        bar = QHBoxLayout()
        btn_close = QPushButton("Fechar"); bar.addStretch(1); bar.addWidget(btn_close)
        v.addLayout(bar)

        btn_delete.clicked.connect(self.do_delete)
        btn_close.clicked.connect(self.reject)

    def do_delete(self):
        key = self.le_key.text().strip()
        if not key:
            return
        csv = cfg_get("geral_multas_csv")
        self.df = ensure_status_cols(pd.read_csv(csv, dtype=str).fillna(""), csv_path=csv)
        rows = self.df.index[self.df["FLUIG"].astype(str) == key].tolist()
        if not rows:
            QMessageBox.warning(self, "Aviso", "FLUIG não encontrado"); return
        i = rows[0]
        try:
            infr  = str(self.df.at[i, "INFRATOR"]) if "INFRATOR" in self.df.columns else ""
            ano   = str(self.df.at[i, "ANO"]) if "ANO" in self.df.columns else ""
            mes   = str(self.df.at[i, "MES"]) if "MES" in self.df.columns else ""
            placa = str(self.df.at[i, "PLACA"]) if "PLACA" in self.df.columns else ""
            notif = str(self.df.at[i, "NOTIFICACAO"]) if "NOTIFICACAO" in self.df.columns else ""
            fluig = str(self.df.at[i, "FLUIG"]) if "FLUIG" in self.df.columns else ""
            root = cfg_get("multas_root")
            sub = f"{placa}_{notif}_FLUIG({fluig})"
            path = os.path.join(root, infr.strip(), str(ano).strip(), str(mes).strip(), sub)
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
                p = os.path.dirname(path)
                root_abs = os.path.abspath(root)
                for _ in range(3):
                    if not p:
                        break
                    p_abs = os.path.abspath(p)
                    if os.path.isdir(p) and not os.listdir(p) and os.path.commonpath([p_abs, root_abs]) == root_abs:
                        try:
                            os.rmdir(p)
                        except Exception:
                            break
                        p = os.path.dirname(p)
                    else:
                        break
        except Exception:
            pass

        self.df = self.df.drop(i).reset_index(drop=True)
        self.df.to_csv(csv, index=False)
        QMessageBox.information(self, "Sucesso", "Multa excluída.")
        self.accept()

# ============================================================
# ======== CENÁRIO GERAL (PLANILHAS, sem CSV aqui) ==========
# ============================================================
class MultasGeneralDialog(QDialog):
    """
    Cenário geral baseado nas PLANILHAS (Detalhamento, Fase Pastores, Condutor Identificado, etc.),
    sem uso do CSV. Consolidação por FLUIG + KPIs + TOP10.
    """
    def __init__(self, parent=None, filtro_nome:str="", data_ini=None, data_fim=None):
        super().__init__(parent)
        self.setWindowTitle("Cenário Geral — Multas (Planilhas)")
        self.resize(1200, 760)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)

        engine = PlanilhasEngine(parent=self)
        df, presence, kpis = engine.load_consolidated(filtro_nome=filtro_nome, data_ini=data_ini, data_fim=data_fim)
        df = (df if isinstance(df, pd.DataFrame) else pd.DataFrame()).fillna("")

        # Estimativa de pontos (mesma regra do condutor)
        def _guess_points(v: float) -> int:
            v = round(float(v or 0), 2)
            if abs(v - 88.38) <= 0.5:   return 3
            if abs(v - 130.16) <= 0.8:  return 4
            if abs(v - 195.23) <= 1.0:  return 5
            if abs(v - 293.47) <= 1.5:  return 7
            if v <= 100:   return 3
            if v <= 160:   return 4
            if v <= 230:   return 5
            return 7
        df["PTS"] = df.get("VALOR_NUM", 0).map(_guess_points)

        # KPIs gerais
        total = float(df["VALOR_NUM"].sum() or 0.0) if not df.empty else 0.0
        descont = float(df.loc[df["DESCONTADA"], "VALOR_NUM"].sum() or 0.0) if not df.empty else 0.0
        pend = float(df.loc[~df["DESCONTADA"], "VALOR_NUM"].sum() or 0.0) if not df.empty else 0.0

        # Agrupar por placa ou infrator (quando existir na consolidação original não há NOME; então usamos Placa como chave secundária de análise)
        key_col = "Placa"
        if key_col not in df.columns:
            df[key_col] = "(sem placa)"

        g = df.groupby(df[key_col].astype(str))
        dash = pd.DataFrame({
            "Qtde": g.size(),
            "ValorTotal": g["VALOR_NUM"].sum(),
            "Descontado_R$": g.apply(lambda x: x.loc[x["DESCONTADA"], "VALOR_NUM"].sum()),
            "Pendente_R$": g.apply(lambda x: x.loc[~x["DESCONTADA"], "VALOR_NUM"].sum()),
            "Pontos": g["PTS"].sum(),
        }).reset_index().rename(columns={key_col: "Chave"})
        dash["% Descontado"] = dash.apply(lambda r: (r["Descontado_R$"] / r["ValorTotal"]) * 100 if r["ValorTotal"] else 0.0, axis=1)

        # ===== UI =====
        root = QVBoxLayout(self)
        card = QFrame(); card.setObjectName("glass"); apply_shadow(card, radius=18, blur=60)
        cv = QVBoxLayout(card)
        title = QLabel("Cenário Geral de Multas (Planilhas)")
        title.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        cv.addWidget(title)

        # KPIs
        kpi = QHBoxLayout()
        def _k(lab, val):
            box = QFrame(); box.setObjectName("card"); apply_shadow(box, radius=12, blur=30)
            v = QVBoxLayout(box)
            L = QLabel(lab); V = QLabel(val)
            L.setStyleSheet("font-weight:600"); V.setStyleSheet("font-weight:800; font-size:18px")
            v.addWidget(L); v.addWidget(V)
            return box
        kpi.addWidget(_k("Multas (qtde)", str(int(len(df)))))
        kpi.addWidget(_k("Valor total (R$)", _fmt_money(total)))
        kpi.addWidget(_k("Descontado (R$)", _fmt_money(descont)))
        kpi.addWidget(_k("Pendente (R$)", _fmt_money(pend)))
        kpi.addWidget(_k("Fontes", ", ".join(sorted(df["Fontes"].str.split(", ").explode().dropna().unique())) if not df.empty else "-"))
        cv.addLayout(kpi)

        tabs = QTabWidget()
        cv.addWidget(tabs)

        # tabela auxiliar
        def top_table(df_top: pd.DataFrame, cols_show: list[str]):
            t = QTableWidget(); t.setAlternatingRowColors(True); t.setSortingEnabled(True)
            t.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
            t.setColumnCount(len(cols_show)); t.setHorizontalHeaderLabels(cols_show)
            t.setRowCount(len(df_top))
            for i, (_, r) in enumerate(df_top.iterrows()):
                for j, c in enumerate(cols_show):
                    val = r[c]
                    if isinstance(val, float):
                        if "R$" in c or "Valor" in c or c.endswith("_R$"):
                            val = _fmt_money(val)
                        elif "Pontos" in c:
                            val = f"{int(val)}"
                        elif "%" in c:
                            val = f"{val:.1f}%"
                    t.setItem(i, j, QTableWidgetItem(str(val)))
            t.resizeColumnsToContents(); t.horizontalHeader().setStretchLastSection(True)
            return t

        # TOP10s
        def _mk_top(tab_name, order_col, cols):
            top = dash.sort_values(order_col, ascending=False).head(10).reset_index(drop=True)
            tabs.addTab(top_table(top, cols), tab_name)

        _mk_top("Mais Multas (Qtde)", "Qtde", ["Chave","Qtde","ValorTotal","Descontado_R$","Pendente_R$","Pontos"])
        _mk_top("Maior Valor", "ValorTotal", ["Chave","ValorTotal","Qtde","Descontado_R$","Pendente_R$","Pontos"])
        _mk_top("Mais Descontado R$", "Descontado_R$", ["Chave","Descontado_R$","ValorTotal","Qtde","Pontos"])
        _mk_top("Menor % Descontado", "% Descontado", ["Chave","% Descontado","ValorTotal","Qtde","Pontos"])
        _mk_top("Maiores Pontos", "Pontos", ["Chave","Pontos","ValorTotal","Qtde","Descontado_R$"])

        # Tabela consolidada por FLUIG (para exploração)
        if not df.empty:
            cols_fluig = ["FLUIG","Fontes","Status","Data","Placa","Infração","Valor","VALOR_NUM","DESCONTADA"]
            cols_fluig = [c for c in cols_fluig if c in df.columns]
            t2 = top_table(df.sort_values(["DT_M","FLUIG"]), cols_fluig)
            tabs.addTab(t2, "Consolidado (por FLUIG)")

        root.addWidget(card)

# ============================================================
# ===================== VIEW PRINCIPAL =======================
# ============================================================
class GeralMultasView(QWidget):
    """
    Tabela principal (operacional) baseada no CSV (em aberto),
    com botão extra de Cenário Geral (planilhas).
    """
    def __init__(self, parent_for_edit=None):
        super().__init__()
        self.parent_for_edit = parent_for_edit

        fm = QFontMetrics(self.font())
        self.max_pix = fm.horizontalAdvance("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")

        df = pd.read_csv(cfg_get("geral_multas_csv"), dtype=str).fillna("")
        self.df_original = ensure_status_cols(df, csv_path=cfg_get("geral_multas_csv"))
        if "COMENTARIO" not in self.df_original.columns:
            self.df_original["COMENTARIO"] = ""
            self.df_original.to_csv(cfg_get("geral_multas_csv"), index=False)
        self.df_filtrado = self.df_original.copy()
        self.cols_show = [c for c in self.df_original.columns if not c.endswith("_STATUS") and c not in IGNORED_COLS]

        root = QVBoxLayout(self)

        header_card = QFrame(); header_card.setObjectName("card"); apply_shadow(header_card, radius=18)
        hv = QVBoxLayout(header_card)

        title = QLabel("Infrações e Multas")
        title.setFont(QFont("Arial", 18, weight=QFont.Weight.Bold))
        hv.addWidget(title)

        sc_global = QScrollArea(); sc_global.setWidgetResizable(True)
        wrap_g = QWidget(); rowg = QHBoxLayout(wrap_g)
        rowg.addWidget(QLabel("Filtro global:"))
        self.global_box = QLineEdit()
        self.global_box.setPlaceholderText("Digite aqui para filtrar em TODAS as colunas…")
        self.global_box.setMaximumWidth(self.max_pix)
        self.global_box.textChanged.connect(self.atualizar_filtro)
        rowg.addWidget(self.global_box, 1)
        sc_global.setWidget(wrap_g)
        hv.addWidget(sc_global)

        sc_seg = QScrollArea(); sc_seg.setWidgetResizable(True)
        inner = QWidget(); hl = QHBoxLayout(inner); hl.setContentsMargins(0,0,0,0); hl.setSpacing(8)
        self.mode_filtros = {}
        self.multi_filtros = {}
        for coluna in self.cols_show:
            box = QVBoxLayout()
            lbl = QLabel(coluna); lbl.setObjectName("colTitle"); lbl.setWordWrap(True); lbl.setMaximumWidth(self.max_pix)
            box.addWidget(lbl)
            line = QHBoxLayout()
            mode = QComboBox(); mode.addItems(["Todos", "Excluir vazios", "Somente vazios"])
            mode.currentTextChanged.connect(self.atualizar_filtro)
            ms = CheckableComboBox(self.df_original[coluna].dropna().astype(str).unique()); ms.changed.connect(self.atualizar_filtro)
            line.addWidget(mode); line.addWidget(ms)
            box.addLayout(line)
            hl.addLayout(box)
            self.mode_filtros[coluna] = mode
            self.multi_filtros[coluna] = ms
        sc_seg.setWidget(inner)
        hv.addWidget(sc_seg)
        root.addWidget(header_card)

        table_card = QFrame(); table_card.setObjectName("glass"); apply_shadow(table_card, radius=18, blur=60, color=QColor(0, 0, 0, 80))
        tv = QVBoxLayout(table_card)

        self.tabela = QTableWidget()
        self.tabela.setAlternatingRowColors(True)
        self.tabela.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.tabela.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.tabela.setSortingEnabled(True)
        self.tabela.horizontalHeader().setSortIndicatorShown(True)
        self.tabela.cellDoubleClicked.connect(self.on_double_click)
        tv.addWidget(self.tabela)

        buttons = QHBoxLayout()
        btn_visao = QPushButton("Visão Geral…"); btn_visao.clicked.connect(self.mostrar_visao)
        btn_cenario = QPushButton("Cenário Geral…"); btn_cenario.clicked.connect(self.mostrar_cenario_geral_planilhas)
        btn_limpar = QPushButton("Limpar filtros"); btn_limpar.clicked.connect(self.limpar_filtros)
        btn_inserir = QPushButton("Inserir"); btn_inserir.clicked.connect(lambda: self.parent_for_edit.inserir())
        btn_editar = QPushButton("Editar"); btn_editar.clicked.connect(lambda: self.parent_for_edit.editar())
        btn_excluir = QPushButton("Excluir"); btn_excluir.setObjectName("danger"); btn_excluir.clicked.connect(lambda: self.parent_for_edit.excluir())
        btn_fluig = QPushButton("CONFERIR FLUIG"); btn_fluig.clicked.connect(lambda: self.parent_for_edit.conferir_fluig())
        btn_past = QPushButton("FASE PASTORES"); btn_past.clicked.connect(lambda: self.parent_for_edit.fase_pastores())
        btn_export = QPushButton("Exportar Excel"); btn_export.clicked.connect(self.exportar_excel)
        buttons.addWidget(btn_visao); buttons.addWidget(btn_cenario); buttons.addWidget(btn_limpar); buttons.addWidget(btn_inserir); buttons.addWidget(btn_editar); buttons.addWidget(btn_excluir); buttons.addWidget(btn_fluig); buttons.addWidget(btn_past); buttons.addStretch(1); buttons.addWidget(btn_export)
        tv.addLayout(buttons)
        root.addWidget(table_card)

        self.atualizar_filtro()

    def recarregar(self):
        df = pd.read_csv(cfg_get("geral_multas_csv"), dtype=str).fillna("")
        self.df_original = ensure_status_cols(df, csv_path=cfg_get("geral_multas_csv"))
        if "COMENTARIO" not in self.df_original.columns:
            self.df_original["COMENTARIO"] = ""
            self.df_original.to_csv(cfg_get("geral_multas_csv"), index=False)
        self.df_filtrado = self.df_original.copy()
        self.cols_show = [c for c in self.df_original.columns if not c.endswith("_STATUS") and c not in IGNORED_COLS]
        self.atualizar_filtro()

    def mostrar_visao(self):
        try:
            dlg = SummaryDialog(self.df_filtrado[self.cols_show])
            dlg.exec()
        except Exception:
            QMessageBox.information(self, "Visão Geral", "Resumo indisponível.")

    def mostrar_cenario_geral_planilhas(self):
        # usa as datas do CSV se existirem como contexto
        try:
            data_ini = None; data_fim = None
            if "DATA INDICAÇÃO" in self.df_filtrado.columns:
                # Apenas como sugestão visual; o motor de planilhas usa DT_M (Data da infração) das fontes
                pass
            dlg = MultasGeneralDialog(self, filtro_nome="", data_ini=data_ini, data_fim=data_fim)
            dlg.exec()
        except Exception as e:
            QMessageBox.critical(self, "Cenário Geral", f"Não foi possível abrir: {e}")

    def limpar_filtros(self):
        self.global_box.blockSignals(True); self.global_box.clear(); self.global_box.blockSignals(False)
        for mode in self.mode_filtros.values():
            mode.blockSignals(True); mode.setCurrentIndex(0); mode.blockSignals(False)
        for ms in self.multi_filtros.values():
            vals = [ms.itemText(i) for i in range(ms.count())]
            ms.set_values(vals)
        self.atualizar_filtro()

    def atualizar_filtro(self):
        df = self.df_original.copy()
        from utils import df_apply_global_texts
        texts = [self.global_box.text()]
        df = df_apply_global_texts(df, texts)
        for coluna in self.cols_show:
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

    def _abrir_pasta_da_multa(self, infrator, ano, mes, placa, notificacao, fluig):
        try:
            dest = build_multa_dir(infrator, ano, mes, placa, notificacao, fluig)
            if not os.path.exists(dest):
                os.makedirs(dest, exist_ok=True)
            QDesktopServices.openUrl(QUrl.fromLocalFile(dest))
        except Exception as e:
            QMessageBox.warning(self, "Pasta da Multa", f"Não foi possível abrir a pasta.\n{e}")

    def preencher_tabela(self, df):
        if "COMENTARIO" not in df.columns:
            df = df.copy(); df["COMENTARIO"] = ""
        show = df[self.cols_show].reset_index(drop=True)
        df_idx = df.reset_index(drop=True)

        self.tabela.clear()
        self.tabela.setColumnCount(len(show.columns) + 1)
        self.tabela.setRowCount(len(show))
        headers = [str(c) for c in show.columns] + ["Ações"]
        self.tabela.setHorizontalHeaderLabels(headers)

        for i in range(len(show)):
            for j, col in enumerate(show.columns):
                val = "" if pd.isna(show.iat[i, j]) else str(show.iat[i, j])
                it = QTableWidgetItem(val)
                it.setFlags(it.flags() & ~Qt.ItemFlag.ItemIsEditable)
                it.setTextAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
                if col in DATE_COLS_MUL:
                    st = str(df_idx.iloc[i].get(f"{col}_STATUS", ""))
                    _paint_status(it, st)
                self.tabela.setItem(i, j, it)

            key = str(df_idx.iloc[i].get("FLUIG", "")).strip()
            infr  = str(df_idx.iloc[i].get("INFRATOR", "")).strip()
            ano   = str(df_idx.iloc[i].get("ANO", "")).strip()
            mes   = str(df_idx.iloc[i].get("MES", "")).strip()
            placa = str(df_idx.iloc[i].get("PLACA", "")).strip()
            notif = str(df_idx.iloc[i].get("NOTIFICACAO", "")).strip()

            wrap = QWidget()
            h = QHBoxLayout(wrap); h.setContentsMargins(0, 0, 0, 0); h.setSpacing(6)

            btn_pasta = QPushButton("Pasta")
            btn_pasta.setToolTip("Abrir a pasta desta multa")
            btn_pasta.clicked.connect(
                lambda _, a=infr, b=ano, c=mes, d=placa, e=notif, f=key: self._abrir_pasta_da_multa(a, b, c, d, e, f)
            )
            h.addWidget(btn_pasta)

            btn_comment = QPushButton("Comentar")
            if "COMENTARIO" in df_idx.columns:
                btn_comment.setToolTip(str(df_idx.iloc[i].get("COMENTARIO", "")).strip())
            if self.parent_for_edit:
                btn_comment.clicked.connect(lambda _, k=key: self.parent_for_edit.comentar_with_key(k))
            h.addWidget(btn_comment)

            h.addStretch(1)
            self.tabela.setCellWidget(i, len(show.columns), wrap)

        self.tabela.resizeColumnsToContents()
        self.tabela.resizeRowsToContents()

    def exportar_excel(self):
        try:
            self.df_filtrado[self.cols_show].to_excel("geral_multas_filtrado.xlsx", index=False)
            QMessageBox.information(self, "Exportado", "geral_multas_filtrado.xlsx criado.")
        except Exception as e:
            QMessageBox.critical(self, "Erro", str(e))

    def on_double_click(self, row, col):
        if self.parent_for_edit is None:
            return
        dfv = self.df_filtrado.reset_index(drop=True)
        key = dfv.iloc[row].get("FLUIG", "")
        if not key:
            return
        self.parent_for_edit.editar_with_key(key)

# ============================================================
# ================== JANELA (CONTÊINER) ======================
# ============================================================
class InfraMultasWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Infrações e Multas")
        self.resize(1240, 820)

        lay = QVBoxLayout(self)
        self.view_geral = GeralMultasView(self)
        lay.addWidget(self.view_geral)

        btn_row = QHBoxLayout()
        btn_det = QPushButton("Detalhamento")
        btn_det.clicked.connect(self.abrir_detalhamento)
        btn_row.addStretch(1)
        btn_row.addWidget(btn_det)
        lay.addLayout(btn_row)

        self.watcher = QFileSystemWatcher()
        csv = cfg_get("geral_multas_csv")
        if os.path.exists(csv):
            self.watcher.addPath(csv)
        self.watcher.fileChanged.connect(self._csv_changed)

    def _csv_changed(self, path):
        if not os.path.exists(path):
            QTimer.singleShot(500, lambda: self._readd_watch(path))
            return
        QTimer.singleShot(500, self.reload_geral)

    def _readd_watch(self, path):
        if os.path.exists(path):
            self.watcher.addPath(path)
        self.reload_geral()

    def reload_geral(self):
        self.view_geral.recarregar()

    def abrir_detalhamento(self):
        try:
            from relatorios import RelatorioWindow
        except Exception:
            QMessageBox.information(self, "Relatórios", "Módulo de relatórios não encontrado.")
            return
        path = cfg_get(PlanilhasEngine.KEY_DETALHAMENTO)
        if not path or not os.path.exists(path):
            QMessageBox.information(self, "Detalhamento", "Configure a planilha de Detalhamento na Base.")
            return
        w = RelatorioWindow(path)
        w.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
        w.show()

    def conferir_fluig(self):
        """Compara FLUIG 'aberta' no Detalhamento (planilha) vs registros no CSV (em aberto)."""
        try:
            detalhamento_path = cfg_get(PlanilhasEngine.KEY_DETALHAMENTO)
            if not detalhamento_path or not os.path.exists(detalhamento_path):
                detalhamento_path, _ = QFileDialog.getOpenFileName(self, "Selecione Detalhamento", "", "Arquivos Excel (*.xlsx *.xls)")
                if not detalhamento_path:
                    return
                cfg_set(PlanilhasEngine.KEY_DETALHAMENTO, detalhamento_path)

            df_det = pd.read_excel(detalhamento_path, dtype=str).fillna("")
            if df_det.empty or len(df_det.columns) < 2:
                QMessageBox.warning(self, "Aviso", "Planilha inválida."); return
            status_col = next((c for c in df_det.columns if c.strip().lower() == "status"), df_det.columns[1])
            mask_aberta = df_det[status_col].astype(str).str.strip().str.lower().eq("aberta")
            df_open = df_det[mask_aberta].copy()
            if "Nº Fluig" in df_open.columns:
                fcol = "Nº Fluig"
            else:
                fcol = next((c for c in df_open.columns if "fluig" in c.lower()), None)
            if not fcol:
                QMessageBox.warning(self, "Aviso", "Coluna de Fluig não encontrada."); return

            df_csv = ensure_status_cols(pd.read_csv(cfg_get("geral_multas_csv"), dtype=str).fillna(""), csv_path=cfg_get("geral_multas_csv"))
            if "COMENTARIO" not in df_csv.columns:
                df_csv["COMENTARIO"] = ""
                df_csv.to_csv(cfg_get("geral_multas_csv"), index=False)

            fluig_det = set(df_open[fcol].astype(str).str.strip())
            fluig_csv = set(df_csv["FLUIG"].astype(str).str.strip()) if "FLUIG" in df_csv.columns else set()
            no_csv_codes = sorted([c for c in fluig_det if c and c not in fluig_csv])
            no_det_codes = sorted([c for c in fluig_csv if c and c not in fluig_det])

            left_cols = [fcol] + [c for c in ["Placa", "Nome", "AIT", "Data Limite", "Data Infração", "Status"] if c in df_open.columns]
            df_left = df_open[df_open[fcol].astype(str).str.strip().isin(no_csv_codes)][left_cols].copy()
            df_left.rename(columns={fcol: "Nº Fluig"}, inplace=True)

            right_cols = [c for c in ["FLUIG", "PLACA", "INFRATOR", "NOTIFICACAO", "ANO", "MES", "COMENTARIO"] if c in df_csv.columns]
            df_right = df_csv[df_csv["FLUIG"].astype(str).str.strip().isin(no_det_codes)][right_cols].copy()

            dlg = ConferirFluigDialog(self, df_left, df_right)
            dlg.exec()
        except Exception as e:
            QMessageBox.critical(self, "Erro", str(e))

    def inserir(self, prefill_fluig=None):
        dlg = InserirDialog(self, prefill_fluig)
        dlg.exec()
        self.reload_geral()

    def editar(self):
        dlg = EditarDialog(self)
        dlg.exec()
        self.reload_geral()

    def editar_with_key(self, key):
        dlg = EditarDialog(self)
        dlg.le_key.setText(str(key))
        dlg.load_record()
        dlg.exec()
        self.reload_geral()

    def excluir(self):
        dlg = ExcluirDialog(self)
        dlg.exec()
        self.reload_geral()

    def fase_pastores(self):
        """Atualiza CSV (SGU/STATUS) com base na planilha Fase Pastores (pagos)."""
        try:
            path = cfg_get(PlanilhasEngine.KEY_FASE_PASTORES)
            if not path or not os.path.exists(path):
                path, _ = QFileDialog.getOpenFileName(self, "Selecione Fase Pastores", "", "Arquivos Excel (*.xlsx *.xls)")
                if not path:
                    QMessageBox.warning(self, "Aviso", "Planilha Fase Pastores não selecionada.")
                    return
                cfg_set(PlanilhasEngine.KEY_FASE_PASTORES, path)

            dfp = pd.read_excel(path, dtype=str).fillna("")
            fcol = next((c for c in dfp.columns if "fluig" in c.lower()), None)
            dcol = next((c for c in dfp.columns if "data" in c.lower() and "pastor" in c.lower()), None)
            tcol = next((c for c in dfp.columns if "tipo" in c.lower()), None)
            if not fcol or not dcol or not tcol:
                QMessageBox.warning(self, "Aviso", "Colunas inválidas em Fase Pastores.")
                return

            df = ensure_status_cols(pd.read_csv(cfg_get("geral_multas_csv"), dtype=str).fillna(""), csv_path=None)
            if "COMENTARIO" not in df.columns:
                df["COMENTARIO"] = ""
            idx = {str(f).strip(): i for i, f in enumerate(df.get("FLUIG", pd.Series([], dtype=str)).astype(str))}
            changed = False
            for _, r in dfp.iterrows():
                f = str(r[fcol]).strip()
                tipo = str(r[tcol]).upper()
                data = str(r[dcol]).strip()
                if not f or f not in idx:
                    continue
                if "PASTOR" not in tipo or not data:
                    continue
                qd = _parse_dt_any(data)
                if not qd.isValid():
                    continue
                i = idx[f]
                df.at[i, "SGU"] = qd.toString(DATE_FORMAT)
                df.at[i, "SGU_STATUS"] = "Pago"
                changed = True
            if changed:
                df.to_csv(cfg_get("geral_multas_csv"), index=False)
                QMessageBox.information(self, "Sucesso", "Atualizado.")
            else:
                QMessageBox.information(self, "Sucesso", "Nada para atualizar.")
        except Exception as e:
            QMessageBox.critical(self, "Erro", str(e))
        self.reload_geral()
