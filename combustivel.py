# combustivel.py
import os, re
import pandas as pd
from PyQt6.QtCore import Qt, QDate
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QFrame, QHBoxLayout, QLabel, QPushButton,
    QTableWidget, QTableWidgetItem, QHeaderView, QDateEdit, QTabWidget,
    QMessageBox
)

from utils import GlobalFilterBar, df_apply_global_texts
from gestao_frota_single import cfg_get, DATE_FORMAT

# ---------- helpers de parsing ----------
def _num(s):
    s = str(s or '').strip()
    if not s: return 0.0
    s = re.sub(r"[^\d,.-]", "", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def _to_date(s):
    s = str(s or '').strip()
    if not s: return pd.NaT
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M", "%d/%m/%Y"):
        try:
            return pd.to_datetime(s, format=fmt, dayfirst=True, errors="raise")
        except Exception:
            pass
    return pd.to_datetime(s, dayfirst=True, errors="coerce")

def _norm_placa(x: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(x or '').upper())

def _series(df: pd.DataFrame, col: str, fill: str = "") -> pd.Series:
    if col in df.columns: return df[col]
    return pd.Series([fill]*len(df), index=df.index, dtype=object)

# ---------- leitura flexível CSV/XLSX ----------
def _resolve_candidates(base: str) -> list[str]:
    if not base: return []
    if os.path.exists(base): return [base]
    folder = os.path.dirname(base) or "."
    stem   = os.path.basename(base)
    pats = [f"{stem}.csv", f"{stem}.xlsx", f"{stem}.xls", f"{stem}*.csv", f"{stem}*.xlsx", f"{stem}*.xls"]
    out = []
    try:
        items = os.listdir(folder)
    except Exception:
        items = []
    for p in pats:
        rx = re.compile("^" + re.escape(p).replace("\\*", ".*") + "$", re.IGNORECASE)
        for x in items:
            if rx.fullmatch(x):
                cand = os.path.join(folder, x)
                if os.path.isfile(cand): out.append(cand)
    out.sort(key=lambda p: (0 if p.lower().endswith(".csv") else 1, p))
    return list(dict.fromkeys(out))

def _resolve_path(base: str) -> str:
    c = _resolve_candidates(base)
    return c[0] if c else ""

def _read_table_flex(base: str) -> pd.DataFrame:
    path = _resolve_path(base)
    if not path: return pd.DataFrame()
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext == ".csv":
            try:
                df = pd.read_csv(path, dtype=str, sep=None, engine="python", encoding="utf-8")
            except UnicodeDecodeError:
                df = pd.read_csv(path, dtype=str, sep=None, engine="python", encoding="latin1")
        elif ext in (".xlsx", ".xls"):
            df = pd.read_excel(path, dtype=str)
        else:
            return pd.DataFrame()
        return df.fillna("")
    except Exception:
        return pd.DataFrame()

# ======================== LOADERS ========================
def _load_responsavel() -> pd.DataFrame:
    """
    Responsavel → mapa PLACA_N → RESPONSAVEL (preferindo registro ativo).
    Aceita CSV/XLSX/XLS e também base sem extensão.
    """
    base = cfg_get('responsavel_path') or 'Responsavel'
    df = _read_table_flex(base)
    if df.empty:
        return pd.DataFrame(columns=['PLACA_N','RESPONSAVEL'])

    c_nome   = next((c for c in df.columns if c.strip().upper() in ('NOME','RESPONSAVEL','RESPONSÁVEL','NOME RESPONSÁVEL')), None)
    c_placa  = next((c for c in df.columns if c.strip().upper() == 'PLACA'), None)
    c_status = next((c for c in df.columns if c.strip().upper() == 'STATUS'), None)
    c_fim    = next((c for c in df.columns if 'DATA' in c.upper() and 'FIM' in c.upper()), None)
    c_ini    = next((c for c in df.columns if 'DATA' in c.upper() and ('INÍC' in c.upper() or 'INIC' in c.upper())), None)
    if not c_nome or not c_placa:
        return pd.DataFrame(columns=['PLACA_N','RESPONSAVEL'])

    df['_PL']  = _series(df, c_placa).map(_norm_placa)
    df['_INI'] = pd.to_datetime(_series(df, c_ini), dayfirst=True, errors='coerce') if c_ini else pd.NaT
    df['_FIM'] = pd.to_datetime(_series(df, c_fim), dayfirst=True, errors='coerce') if c_fim else pd.NaT

    if c_status:
        stat = _series(df, c_status).astype(str).str.upper()
        act = df[(stat!='VENDIDO') & (df['_FIM'].isna())].copy()
    else:
        act = df[df['_FIM'].isna()].copy()
    if act.empty:
        act = df.copy()

    m = (act
         .dropna(subset=['_PL'])
         .sort_values(['_INI'], ascending=[False])
         .groupby('_PL')
         .first()
         .reset_index())
    out = m[['_PL', c_nome]].copy()
    out.columns = ['PLACA_N','RESPONSAVEL']
    return out

def _load_extrato_simplificado() -> pd.DataFrame:
    """
    ExtratoSimplificado → nivelado por PLACA. Aceita CSV/XLSX/XLS.
    """
    base = cfg_get('extrato_simplificado_path') or 'ExtratoSimplificado'
    df = _read_table_flex(base)
    if df.empty:
        return pd.DataFrame()

    ren = {}
    for a,b in {
        'Placa':'Placa','Nome Responsável':'Responsavel','RESPONSÁVEL':'Responsavel','RESPONSAVEL':'Responsavel',
        'Limite Atual':'LimiteAtual','Limite':'Limite','Compras (utilizado)':'Compras','Saldo':'Saldo',
        'Limite Próximo Período':'LimiteProximo','Cidade/UF':'CidadeUF','Modelo':'Modelo','Fabricante':'Fabricante',
        'Família':'Familia','Tipo Frota':'TipoFrota'
    }.items():
        if a in df.columns: ren[a] = b
    df = df.rename(columns=ren).fillna("")

    for c in ['LimiteAtual','Limite','Compras','Saldo','LimiteProximo']:
        if c in df.columns: df[c] = df[c].map(_num)

    df['PLACA_N'] = _series(df, 'Placa').map(_norm_placa)

    # merge com responsável atual por placa
    mapa = _load_responsavel()
    if not mapa.empty:
        df = df.merge(mapa, on='PLACA_N', how='left')
        # prioriza SEMPRE o mapa do Responsavel
        src = df.get('Responsavel', pd.Series(index=df.index, dtype=str))
        df['Responsavel'] = df['RESPONSAVEL'] if 'RESPONSAVEL' in df.columns else src
        if 'RESPONSAVEL' in df.columns:
            df['Responsavel'] = df['RESPONSAVEL'].where(df['RESPONSAVEL'].astype(str).str.strip()!='', src)
            df.drop(columns=['RESPONSAVEL'], inplace=True, errors='ignore')
    else:
        if 'Responsavel' not in df.columns:
            df['Responsavel'] = ''
    return df

def _load_extrato_geral() -> pd.DataFrame:
    """
    ExtratoGeral → enriquece com RESPONSÁVEL do mapa por PLACA. Aceita CSV/XLSX/XLS.
    """
    base = cfg_get('extrato_geral_path') or 'ExtratoGeral'
    df = _read_table_flex(base)
    if df.empty:
        return pd.DataFrame()

    ren = {}
    for a,b in {
        'DATA TRANSACAO':'DATA','PLACA':'PLACA',
        'NOME MOTORISTA':'MOTORISTA','Motorista':'MOTORISTA','MOTORISTA':'MOTORISTA',
        'TIPO COMBUSTIVEL':'COMBUSTIVEL','LITROS':'LITROS','VL/LITRO':'VL_LITRO','VALOR EMISSAO':'VALOR',
        'NOME ESTABELECIMENTO':'ESTABELECIMENTO','CIDADE':'CIDADE','UF':'UF','CIDADE/UF':'CIDADE_UF',
        'RESPONSAVEL':'RESPONSAVEL'
    }.items():
        if a in df.columns: ren[a] = b
    df = df.rename(columns=ren).fillna("")

    if 'CIDADE_UF' not in df.columns:
        df['CIDADE_UF'] = _series(df, 'CIDADE').astype(str).str.strip()+"/"+_series(df,'UF').astype(str).str.strip()

    df['DT']           = _series(df, 'DATA').map(_to_date)
    df['LITROS_NUM']   = _series(df, 'LITROS').map(_num)
    df['VL_LITRO_NUM'] = _series(df, 'VL_LITRO').map(_num)
    df['VALOR_NUM']    = _series(df, 'VALOR').map(_num)
    df['PLACA_N']      = _series(df, 'PLACA').map(_norm_placa)

    mapa = _load_responsavel()
    if not mapa.empty:
        df = df.merge(mapa, on='PLACA_N', how='left', suffixes=('', '_MAP'))
        base = df.get('RESPONSAVEL', pd.Series(index=df.index, dtype=str))
        alt  = df.get('Responsavel', pd.Series(index=df.index, dtype=str))
        df['Responsavel'] = df['RESPONSAVEL'].where(base.astype(str).str.strip()!='', alt)
        if 'RESPONSAVEL' in df.columns:
            df.drop(columns=['RESPONSAVEL'], inplace=True, errors='ignore')
    else:
        if 'Responsavel' not in df.columns:
            alt = next((c for c in df.columns if c.strip().upper() in ('RESPONSAVEL','RESPONSÁVEL','NOME RESPONSÁVEL')), None)
            df['Responsavel'] = df[alt] if alt else ''
    return df

# ======================== UI / JANELA ========================
class CombustivelWindow(QWidget):
    """
    - ATUAL (ExtratoSimplificado): por PLACA + responsável do Responsavel
    - HISTÓRICO (ExtratoGeral): Linhas | por Responsável | por Placa
    - Suporta CSV e Excel
    """
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Combustível — Visão Administrativa Geral")
        self.resize(1280, 880)

        self.df_simpl = pd.DataFrame()
        self.df_hist  = pd.DataFrame()

        root = QVBoxLayout(self)

        # Período + reload
        bar = QFrame(); bv = QHBoxLayout(bar)
        self.de_ini = QDateEdit(); self.de_fim = QDateEdit()
        for de in (self.de_ini, self.de_fim):
            de.setCalendarPopup(True); de.setDisplayFormat(DATE_FORMAT)
        today = pd.Timestamp.today().normalize()
        self.de_ini.setDate(QDate(today.year, today.month, 1))
        self.de_fim.setDate(QDate(today.year, today.month, today.day))
        btn_reload = QPushButton("Recarregar fontes"); btn_reload.clicked.connect(self._load)
        bv.addWidget(QLabel("Início:")); bv.addWidget(self.de_ini)
        bv.addWidget(QLabel("Fim:"));    bv.addWidget(self.de_fim)
        bv.addStretch(1);                bv.addWidget(btn_reload)
        root.addWidget(bar)

        # Filtro global (histórico)
        self.global_bar = GlobalFilterBar("Filtro global (aplica ao histórico):")
        self.global_bar.changed.connect(self._refresh)
        root.addWidget(self.global_bar)

        # KPIs atuais
        kcard = QFrame(); kv = QHBoxLayout(kcard)
        self.k_lim_atual = QLabel('0,00'); self.k_compras = QLabel('0,00')
        self.k_saldo = QLabel('0,00');    self.k_lim_next = QLabel('0,00')
        for lab in (self.k_lim_atual, self.k_compras, self.k_saldo, self.k_lim_next):
            lab.setFont(QFont('Arial', 14, QFont.Weight.Bold))
        kv.addWidget(QLabel('Σ Limite Atual (R$):')); kv.addWidget(self.k_lim_atual)
        kv.addWidget(QLabel('Σ Compras (R$):'));      kv.addWidget(self.k_compras)
        kv.addWidget(QLabel('Σ Saldo (R$):'));        kv.addWidget(self.k_saldo)
        kv.addWidget(QLabel('Σ Limite Próx. (R$):')); kv.addWidget(self.k_lim_next)
        root.addWidget(kcard)

        # Abas e tabelas
        self.tabs = QTabWidget(); root.addWidget(self.tabs, 1)
        self.tbl_atual_por_placa = self._mk_tbl()
        self.tbl_hist_linhas     = self._mk_tbl()
        self.tbl_hist_por_resp   = self._mk_tbl()
        self.tbl_hist_por_placa  = self._mk_tbl()
        self._add_tab(self.tbl_atual_por_placa, "Atual — por Placa")
        self._add_tab(self.tbl_hist_linhas,     "Histórico — Linhas (todas)")
        self._add_tab(self.tbl_hist_por_resp,   "Histórico — por Responsável")
        self._add_tab(self.tbl_hist_por_placa,  "Histórico — por Placa")

        self.de_ini.dateChanged.connect(self._refresh)
        self.de_fim.dateChanged.connect(self._refresh)

        self._load()

    def _mk_tbl(self):
        t = QTableWidget()
        t.setAlternatingRowColors(True)
        t.setSortingEnabled(True)
        t.horizontalHeader().setSortIndicatorShown(True)
        try:
            t.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        except AttributeError:
            t.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        try:
            t.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        except AttributeError:
            t.verticalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        return t

    def _add_tab(self, table, title):
        w = QFrame(); v = QVBoxLayout(w); v.addWidget(table)
        self.tabs.addTab(w, title)

    def _fmt_money(self, x):
        try:
            return f"{float(x or 0):,.2f}".replace(",","X").replace(".",",").replace("X",".")
        except Exception:
            return str(x)

    # ================== Load/Refresh ==================
    def _load(self):
        try:
            self.df_simpl = _load_extrato_simplificado()
            self.df_hist  = _load_extrato_geral()

            # KPIs atuais
            if not self.df_simpl.empty:
                self.k_lim_atual.setText(self._fmt_money(self.df_simpl.get('LimiteAtual', pd.Series()).sum()))
                self.k_compras.setText(self._fmt_money(self.df_simpl.get('Compras', pd.Series()).sum()))
                self.k_saldo.setText(self._fmt_money(self.df_simpl.get('Saldo', pd.Series()).sum()))
                self.k_lim_next.setText(self._fmt_money(self.df_simpl.get('LimiteProximo', pd.Series()).sum()))
            else:
                for lab in (self.k_lim_atual, self.k_compras, self.k_saldo, self.k_lim_next):
                    lab.setText('0,00')

            self._refresh()
        except Exception as e:
            QMessageBox.critical(self, 'Combustível', f'Falha ao carregar fontes.\n{e}')

    def _refresh(self):
        a = pd.Timestamp(self.de_ini.date().year(), self.de_ini.date().month(), self.de_ini.date().day())
        b = pd.Timestamp(self.de_fim.date().year(), self.de_fim.date().month(), self.de_fim.date().day())
        if a > b: a, b = b, a

        # HISTÓRICO (linhas)
        hist = self.df_hist.copy()
        if not hist.empty and 'DT' in hist.columns:
            hist = hist[(hist['DT'].notna()) & (hist['DT'] >= a) & (hist['DT'] <= b)]
            hist = df_apply_global_texts(hist, self.global_bar.values())
            if not isinstance(hist, pd.DataFrame):
                hist = pd.DataFrame()

        # ATUAL — por PLACA
        s = self.df_simpl.copy()
        if not s.empty:
            for c in ['LimiteAtual','Compras','Saldo','LimiteProximo']:
                if c not in s.columns: s[c] = 0.0
            atual_cols = ['Responsavel','Placa','Modelo','Fabricante','CidadeUF','LimiteAtual','Compras','Saldo','LimiteProximo']
            atual_cols = [c for c in atual_cols if c in s.columns]
            df_atual = (s.sort_values(['Saldo','LimiteAtual'], ascending=[False, False])[atual_cols]
                        .rename(columns={'Responsavel':'Responsável','CidadeUF':'Cidade/UF','LimiteProximo':'Limite Próximo'}))
            self._fill(self.tbl_atual_por_placa, df_atual, headers=list(df_atual.columns), money_cols={'Limite Atual','Compras','Saldo','Limite Próximo'})
        else:
            self._fill(self.tbl_atual_por_placa, pd.DataFrame(), [])

        # HISTÓRICO — Linhas
        if not hist.empty:
            cols = ['DT','PLACA','Responsavel','MOTORISTA','COMBUSTIVEL','LITROS_NUM','VL_LITRO_NUM','VALOR_NUM','ESTABELECIMENTO','CIDADE_UF']
            cols = [c for c in cols if c in hist.columns]
            df_rows = hist[cols].copy().rename(columns={
                'DT':'Data','PLACA':'Placa','Responsavel':'Responsável','MOTORISTA':'Motorista',
                'COMBUSTIVEL':'Combustível','LITROS_NUM':'Litros','VL_LITRO_NUM':'R$/L',
                'VALOR_NUM':'R$','ESTABELECIMENTO':'Estabelecimento','CIDADE_UF':'Cidade/UF'
            })
            if 'Data' in df_rows.columns:
                df_rows['Data'] = df_rows['Data'].dt.strftime('%d/%m/%Y %H:%M')
            self._fill(self.tbl_hist_linhas, df_rows, headers=list(df_rows.columns), money_cols={'R$','R$/L'})
        else:
            self._fill(self.tbl_hist_linhas, pd.DataFrame(), [])

        # HISTÓRICO — por RESPONSÁVEL
        if not hist.empty:
            g = hist.groupby(hist['Responsavel'].astype(str).str.strip())
            agg_resp = g.agg(Abastecimentos=('VALOR_NUM','count'), Litros=('LITROS_NUM','sum'), Custo=('VALOR_NUM','sum')).reset_index().rename(columns={'Responsavel':'Responsável'})
            agg_resp['R$/L'] = (agg_resp['Custo'] / agg_resp['Litros']).replace([pd.NA, pd.NaT, float('inf')], 0.0)
            self._fill(self.tbl_hist_por_resp, agg_resp.sort_values(['Custo','Abastecimentos'], ascending=[False, False]),
                       headers=['Responsável','Abastecimentos','Litros','Custo','R$/L'], money_cols={'Custo','R$/L'})
        else:
            self._fill(self.tbl_hist_por_resp, pd.DataFrame(), [])

        # HISTÓRICO — por PLACA
        if not hist.empty:
            g2 = hist.groupby(hist['PLACA'].astype(str).str.strip())
            agg_placa = g2.agg(Abastecimentos=('VALOR_NUM','count'), Litros=('LITROS_NUM','sum'), Custo=('VALOR_NUM','sum')).reset_index().rename(columns={'PLACA':'Placa'})
            agg_placa['R$/L'] = (agg_placa['Custo'] / agg_placa['Litros']).replace([pd.NA, pd.NaT, float('inf')], 0.0)
            self._fill(self.tbl_hist_por_placa, agg_placa.sort_values(['Custo','Abastecimentos'], ascending=[False, False]),
                       headers=['Placa','Abastecimentos','Litros','Custo','R$/L'], money_cols={'Custo','R$/L'})
        else:
            self._fill(self.tbl_hist_por_placa, pd.DataFrame(), [])

    # --------- utilitário para preencher QTableWidget ---------
    def _fill(self, tbl: QTableWidget, df: pd.DataFrame, headers=None, money_cols=None):
        money_cols = money_cols or set()
        if headers is None:
            headers = list(df.columns) if isinstance(df, pd.DataFrame) else []
        tbl.setSortingEnabled(False)
        tbl.clear()
        tbl.setColumnCount(len(headers))
        tbl.setHorizontalHeaderLabels(headers)
        if df is None or df.empty:
            tbl.setRowCount(0)
            tbl.setSortingEnabled(True)
            return
        tbl.setRowCount(len(df))
        for i, (_, r) in enumerate(df.iterrows()):
            for j, c in enumerate(headers):
                val = r.get(c, '')
                if c in money_cols:
                    try: val = f"{float(val or 0):,.2f}".replace(",","X").replace(".",",").replace("X",".")
                    except Exception: pass
                it = QTableWidgetItem(str(val))
                it.setFlags(it.flags() & ~Qt.ItemFlag.ItemIsEditable)
                tbl.setItem(i, j, it)
        tbl.resizeColumnsToContents()
        tbl.horizontalHeader().setStretchLastSection(True)
        tbl.setSortingEnabled(True)
