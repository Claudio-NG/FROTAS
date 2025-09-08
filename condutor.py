import os, re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from PyQt6.QtCore import Qt, QDate, pyqtSignal, QObject
from PyQt6.QtGui import QFont, QColor, QStandardItemModel, QStandardItem
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QFrame, QHBoxLayout, QLabel, QLineEdit, QPushButton,
    QCompleter, QDateEdit, QTableWidget, QHeaderView, QTableWidgetItem, QGridLayout, QMessageBox
)

from gestao_frota_single import cfg_get, DATE_FORMAT
from utils import apply_shadow, GlobalFilterBar, df_apply_global_texts

class _Sig(QObject):
    ready = pyqtSignal(str, pd.DataFrame)
    error = pyqtSignal(str)



class CondutorWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Condutor — Busca Integrada")
        self.resize(1200, 820)
        self.sig = _Sig()
        self.sig.ready.connect(self._on_chunk_ready)
        self.sig.error.connect(self._on_error)
        self.p_multas = cfg_get("geral_multas_csv")
        self.p_extrato = cfg_get("extrato_geral_path")
        self.p_simpl = cfg_get("extrato_simplificado_path")
        self.p_det = cfg_get("detalhamento_path")
        self.names_model = QStandardItemModel(self)
        self._df_m = pd.DataFrame()
        self._df_e = pd.DataFrame()
        self._df_d = pd.DataFrame()
        self._build_ui()
        self._build_completer_source()

    def _build_ui(self):
        root = QVBoxLayout(self)
        head = QFrame(); head.setObjectName("glass"); apply_shadow(head, radius=18, blur=60, color=QColor(0,0,0,60))
        hv = QVBoxLayout(head); hv.setContentsMargins(18,18,18,18)
        t = QLabel("Condutor — Busca Integrada"); t.setAlignment(Qt.AlignmentFlag.AlignCenter)
        t.setFont(QFont("Arial", 22, QFont.Weight.Bold))
        hv.addWidget(t)
        root.addWidget(head)

        bar = QFrame(); bar.setObjectName("card"); apply_shadow(bar, radius=16)
        bl = QGridLayout(bar)
        self.ed_nome = QLineEdit(); self.ed_nome.setPlaceholderText("Digite um nome (ou escolha uma sugestão)…")
        self.btn_carregar = QPushButton("Carregar informações")
        self.btn_carregar.setMinimumHeight(40)
        self.de_ini = QDateEdit(); self.de_fim = QDateEdit()
        for de in (self.de_ini, self.de_fim):
            de.setCalendarPopup(True); de.setDisplayFormat(DATE_FORMAT)
        from pandas import Timestamp
        today = Timestamp.today().normalize()
        self.de_ini.setDate(QDate(today.year, today.month, 1))
        self.de_fim.setDate(QDate(today.year, today.month, today.day))
        self.de_ini.dateChanged.connect(self._apply_filters)
        self.de_fim.dateChanged.connect(self._apply_filters)
        self.global_bar = GlobalFilterBar("Filtro global:")
        self.global_bar.changed.connect(self._apply_filters)
        bl.addWidget(QLabel("Nome do Condutor/Responsável:"), 0, 0)
        bl.addWidget(self.ed_nome, 0, 1, 1, 2)
        bl.addWidget(self.btn_carregar, 0, 3)
        bl.addWidget(QLabel("Início:"), 1, 0); bl.addWidget(self.de_ini, 1, 1)
        bl.addWidget(QLabel("Fim:"), 1, 2); bl.addWidget(self.de_fim, 1, 3)
        bl.addWidget(self.global_bar, 2, 0, 1, 4)
        root.addWidget(bar)

        cards = QFrame(); cards.setObjectName("glass"); apply_shadow(cards, radius=16, blur=60, color=QColor(0,0,0,60))
        cg = QGridLayout(cards)
        self.k_multas = QLabel("0"); self.k_valor = QLabel("0,00")
        self.k_abast  = QLabel("0"); self.k_litros = QLabel("0,00"); self.k_custo = QLabel("0,00")
        for i, (lab, val) in enumerate([("Multas", self.k_multas), ("Valor Multas (R$)", self.k_valor),
                                        ("Abastecimentos", self.k_abast), ("Litros", self.k_litros),
                                        ("Custo Combustível (R$)", self.k_custo)]):
            cg.addWidget(QLabel(lab), 0, i)
            val.setFont(QFont("Arial", 14, QFont.Weight.Bold)); val.setAlignment(Qt.AlignmentFlag.AlignCenter)
            cg.addWidget(val, 1, i)
        root.addWidget(cards)

        self.tbl_m = self._mk_table(["FLUIG","Data","Placa","Órgão","Infração","Valor (R$)"])
        self.tbl_e = self._mk_table(["Data","Placa","Motorista","Combustível","Litros","R$/L","R$","Estabelecimento","Cidade/UF"])
        wrap = QHBoxLayout()
        wrap.addWidget(self.tbl_m, 1)
        wrap.addWidget(self.tbl_e, 1)
        root.addLayout(wrap)
        self.btn_carregar.clicked.connect(self._start_load_for_name)

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

    def _build_completer_source(self):
        names = set()
        if self.p_simpl and os.path.exists(self.p_simpl):
            try:
                ds = pd.read_excel(self.p_simpl, dtype=str).fillna("")
                for cand in ["Nome Responsável","RESPONSÁVEL","RESPONSAVEL","Responsável","Responsavel"]:
                    if cand in ds.columns:
                        names |= set([x for x in ds[cand].astype(str) if x.strip()])
                        break
            except Exception:
                pass
        if self.p_multas and os.path.exists(self.p_multas):
            try:
                dm = pd.read_csv(self.p_multas, dtype=str).fillna("")
                for cand in ["INFRATOR","NOME","CONDUTOR"]:
                    if cand in dm.columns:
                        names |= set([x for x in dm[cand].astype(str) if x.strip()])
            except Exception:
                pass
        if self.p_extrato and os.path.exists(self.p_extrato):
            try:
                de = pd.read_excel(self.p_extrato, dtype=str).fillna("")
                for cand in ["NOME MOTORISTA","Motorista","MOTORISTA"]:
                    if cand in de.columns:
                        names |= set([x for x in de[cand].astype(str) if x.strip()])
                        break
            except Exception:
                pass
        self.names_model.clear()
        for n in sorted(names):
            self.names_model.appendRow(QStandardItem(n))
        comp = QCompleter(self.names_model, self)
        comp.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        comp.setFilterMode(Qt.MatchFlag.MatchContains)
        self.ed_nome.setCompleter(comp)

    def _start_load_for_name(self):
        target = self.ed_nome.text().strip()
        if not target:
            QMessageBox.information(self, "Condutor", "Informe um nome ou escolha uma sugestão.")
            return
        self._df_m = pd.DataFrame()
        self._df_e = pd.DataFrame()
        self._df_d = pd.DataFrame()
        tasks = []
        with ThreadPoolExecutor(max_workers=3) as ex:
            tasks.append(ex.submit(self._load_multas_for, target))
            tasks.append(ex.submit(self._load_extrato_for, target))
            tasks.append(ex.submit(self._load_detalhamento_for, target))
            for fut in as_completed(tasks):
                try:
                    tag, df = fut.result()
                    if tag == "M": self._df_m = df
                    if tag == "E": self._df_e = df
                    if tag == "D": self._df_d = df
                    self.sig.ready.emit(tag, df)
                except Exception as e:
                    self.sig.error.emit(str(e))
        self._apply_filters()

    def _load_multas_for(self, name: str):
        if not self.p_multas or not os.path.exists(self.p_multas):
            return "M", pd.DataFrame()
        df = pd.read_csv(self.p_multas, dtype=str).fillna("")
        cond = None
        for c in ("INFRATOR","NOME","CONDUTOR"):
            if c in df.columns: cond = c; break
        if cond:
            df = df[df[cond].astype(str).str.contains(re.escape(name), case=False, na=False)]
        def _num(s):
            s = str(s).strip()
            if not s: return 0.0
            s = re.sub(r"[^\d,.-]", "", s)
            if "," in s and "." in s:
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", ".")
            try: return float(s)
            except: return 0.0
        if "VALOR" not in df.columns:
            alt = [c for c in df.columns if c.upper().strip() in ("VALOR MULTA","VALOR_MULTA","VALOR DA MULTA")]
            df["VALOR"] = df[alt[0]] if alt else ""
        df["VALOR_NUM"] = df["VALOR"].map(_num)
        df["DT_M"] = pd.to_datetime(df.get("DATA", df.get("DATA DA INFRACAO", df.get("DATA INFRAÇÃO",""))),
                                    dayfirst=True, errors="coerce")
        return "M", df

    def _load_extrato_for(self, name: str):
        if not self.p_extrato or not os.path.exists(self.p_extrato):
            return "E", pd.DataFrame()
        df = pd.read_excel(self.p_extrato, dtype=str).fillna("")
        hit = False
        for c in ("NOME MOTORISTA","Motorista","MOTORISTA"):
            if c in df.columns:
                df = df[df[c].astype(str).str.contains(re.escape(name), case=False, na=False)]
                hit = True
                break
        if not hit:
            for c in ("RESPONSAVEL","Responsável","RESPONSÁVEL","Nome Responsável"):
                if c in df.columns:
                    df = df[df[c].astype(str).str.contains(re.escape(name), case=False, na=False)]
                    break
        m = {
            "DATA TRANSACAO":"DATA_TRANSACAO","PLACA":"PLACA","NOME MOTORISTA":"MOTORISTA",
            "TIPO COMBUSTIVEL":"COMBUSTIVEL","LITROS":"LITROS","VL/LITRO":"VL_LITRO",
            "VALOR EMISSAO":"VALOR","NOME ESTABELECIMENTO":"ESTABELECIMENTO",
            "CIDADE":"CIDADE","UF":"UF","CIDADE/UF":"CIDADE_UF"
        }
        use = {k:v for k,v in m.items() if k in df.columns}
        df = df.rename(columns=use)
        if "CIDADE_UF" not in df.columns:
            df["CIDADE_UF"] = df.get("CIDADE","").astype(str).str.strip()+"/"+df.get("UF","").astype(str).str.strip()
        def _dt(s): return pd.to_datetime(str(s), dayfirst=True, errors="coerce")
        def _num(s):
            s = str(s).strip()
            if not s: return 0.0
            s = re.sub(r"[^\d,.-]", "", s)
            if "," in s and "." in s:
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", ".")
            try: return float(s)
            except: return 0.0
        df["DT_C"] = df.get("DATA_TRANSACAO","").map(_dt)
        for c_src, c_num in [("LITROS","LITROS_NUM"),("VL_LITRO","VL_LITRO_NUM"),("VALOR","VALOR_NUM")]:
            df[c_num] = df.get(c_src, "").map(_num)
        return "E", df

    def _load_detalhamento_for(self, name: str):
        if not self.p_det or not os.path.exists(self.p_det):
            return "D", pd.DataFrame()
        try:
            df = pd.read_excel(self.p_det, dtype=str).fillna("")
        except Exception:
            return "D", pd.DataFrame()
        cols = [c for c in df.columns if any(k in c.upper() for k in ("RESPONS", "CONDUTOR", "MOTORISTA", "INFRATOR", "NOME"))]
        if cols:
            m = pd.Series(False, index=df.index)
            for c in cols:
                m |= df[c].astype(str).str.contains(re.escape(name), case=False, na=False)
            df = df[m]
        return "D", df

    def _apply_filters(self):
        import pandas as pd
        q0, q1 = self.de_ini.date(), self.de_fim.date()
        t0 = pd.Timestamp(q0.year(), q0.month(), q0.day())
        t1 = pd.Timestamp(q1.year(), q1.month(), q1.day())
        a, b = (t0, t1) if t0 <= t1 else (t1, t0)
        dm = self._df_m.copy()
        de = self._df_e.copy()
        if not dm.empty and "DT_M" in dm:
            dm = dm[(dm["DT_M"].notna()) & (dm["DT_M"] >= a) & (dm["DT_M"] <= b)]
            dm = df_apply_global_texts(dm, self.global_bar.values())
        if not de.empty and "DT_C" in de:
            de = de[(de["DT_C"].notna()) & (de["DT_C"] >= a) & (de["DT_C"] <= b)]
            de = df_apply_global_texts(de, self.global_bar.values())
        vm = float(dm["VALOR_NUM"].sum()) if not dm.empty else 0.0
        self.k_multas.setText(str(len(dm)))
        self.k_valor.setText(f"{vm:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
        self.k_abast.setText(str(len(de)))
        self.k_litros.setText(f"{float(de.get('LITROS_NUM', pd.Series()).sum() or 0.0):,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
        self.k_custo.setText(f"{float(de.get('VALOR_NUM', pd.Series()).sum() or 0.0):,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
        self._fill_multas(dm)
        self._fill_extrato(de)

    def _fill_multas(self, dm: pd.DataFrame):
        headers = ["FLUIG","Data","Placa","Órgão","Infração","Valor (R$)"]
        rows = []
        if not dm.empty:
            org = None
            for c in ("ÓRGÃO","ORGÃO","ORGAO","ORG"):
                if c in dm.columns: org = c; break
            inf = None
            for c in ("TIPO INFRACAO","TIPO INFRAÇÃO","INFRACAO","INFRAÇÃO","NOTIFICACAO","NOTIFICAÇÃO"):
                if c in dm.columns: inf = c; break
            for _, r in dm.sort_values("VALOR_NUM", ascending=False).iterrows():
                rows.append([
                    r.get("FLUIG",""),
                    r["DT_M"].strftime("%d/%m/%Y") if pd.notna(r["DT_M"]) else "",
                    r.get("PLACA",""),
                    r.get(org,"") if org else "",
                    r.get(inf,"") if inf else "",
                    f"{float(r.get('VALOR_NUM',0)):.2f}",
                ])
        self._fill(self.tbl_m, rows, headers)

    def _fill_extrato(self, de: pd.DataFrame):
        headers = ["Data","Placa","Motorista","Combustível","Litros","R$/L","R$","Estabelecimento","Cidade/UF"]
        rows = []
        if not de.empty:
            for _, r in de.sort_values("DT_C").iterrows():
                rows.append([
                    r["DT_C"].strftime("%d/%m/%Y %H:%M") if pd.notna(r["DT_C"]) else "",
                    r.get("PLACA",""),
                    r.get("MOTORISTA",""),
                    r.get("COMBUSTIVEL",""),
                    f"{float(r.get('LITROS_NUM',0)):.2f}",
                    f"{float(r.get('VL_LITRO_NUM',0)):.2f}",
                    f"{float(r.get('VALOR_NUM',0)):.2f}",
                    r.get("ESTABELECIMENTO",""),
                    r.get("CIDADE_UF",""),
                ])
        self._fill(self.tbl_e, rows, headers)

    def _fill(self, tbl: QTableWidget, rows, headers):
        tbl.setSortingEnabled(False)
        tbl.clear()
        tbl.setColumnCount(len(headers))
        tbl.setHorizontalHeaderLabels(headers)
        tbl.setRowCount(len(rows))
        for i, r in enumerate(rows):
            for j, v in enumerate(r):
                it = QTableWidgetItem(str(v))
                it.setFlags(it.flags() & ~Qt.ItemFlag.ItemIsEditable)
                tbl.setItem(i, j, it)
        tbl.resizeColumnsToContents()
        tbl.horizontalHeader().setStretchLastSection(True)
        tbl.setSortingEnabled(True)

    def _on_chunk_ready(self, tag: str, df: pd.DataFrame):
        pass

    def _on_error(self, msg: str):
        QMessageBox.warning(self, "Condutor", msg)
