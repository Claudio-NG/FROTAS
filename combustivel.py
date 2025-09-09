import os, re
import pandas as pd
from PyQt6.QtCore import Qt, QDate
from PyQt6.QtGui import QColor, QFont
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QFrame, QHBoxLayout, QLabel, QPushButton,
    QTableWidget, QTableWidgetItem, QHeaderView, QDateEdit, QTabWidget,
    QMessageBox
)

from utils import GlobalFilterBar, df_apply_global_texts
from gestao_frota_single import cfg_get, DATE_FORMAT

# ================= Helpers =================

def _num(s):
    s = str(s or '').strip()
    if not s:
        return 0.0
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
    if not s:
        return pd.NaT
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return pd.to_datetime(s, format=fmt, errors="raise")
        except Exception:
            pass
    return pd.to_datetime(s, dayfirst=True, errors="coerce")


def _norm_placa(x: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(x or '').upper())


# ============== Carregamentos base ==============

def _load_responsavel() -> pd.DataFrame:
    """Carrega Responsavel.xlsx e retorna mapeamento placa→responsável (ativo)."""
    p = cfg_get('responsavel_path') or 'Responsavel.xlsx'
    if not os.path.exists(p):
        return pd.DataFrame(columns=['PLACA','RESPONSAVEL'])
    try:
        df = pd.read_excel(p, dtype=str).fillna("")
    except Exception:
        return pd.DataFrame(columns=['PLACA','RESPONSAVEL'])
    # colunas prováveis
    c_nome = 'NOME' if 'NOME' in df.columns else next((c for c in df.columns if c.strip().upper() in ('NOME','RESPONSAVEL','RESPONSÁVEL','NOME RESPONSÁVEL')), None)
    c_placa = 'PLACA' if 'PLACA' in df.columns else next((c for c in df.columns if c.strip().upper()== 'PLACA'), None)
    c_status = next((c for c in df.columns if c.strip().upper()=='STATUS'), None)
    c_fim = next((c for c in df.columns if 'DATA' in c.upper() and 'FIM' in c.upper()), None)
    c_ini = next((c for c in df.columns if 'DATA' in c.upper() and 'INÍC' in c.upper() or 'INIC' in c.upper()), None)
    if not c_nome or not c_placa:
        return pd.DataFrame(columns=['PLACA','RESPONSAVEL'])

    df['_PL'] = df[c_placa].map(_norm_placa)
    df['_INI'] = pd.to_datetime(df[c_ini], dayfirst=True, errors='coerce') if c_ini else pd.NaT
    df['_FIM'] = pd.to_datetime(df[c_fim], dayfirst=True, errors='coerce') if c_fim else pd.NaT

    # ativos: STATUS!=VENDIDO e sem DATA FIM
    if c_status:
        act = df[(df[c_status].astype(str).str.upper()!='VENDIDO') & (df['_FIM'].isna())].copy()
    else:
        act = df[df['_FIM'].isna()].copy()
    if act.empty:
        act = df.copy()
    act = act.sort_values(['_INI'], ascending=[False])

    # mantém primeiro por placa
    act = act.dropna(subset=['_PL'])
    m = act.groupby('_PL').first().reset_index()
    out = m[['_PL', c_nome]].copy()
    out.columns = ['PLACA_N','RESPONSAVEL']
    return out


def _load_extrato_simplificado() -> pd.DataFrame:
    p = cfg_get('extrato_simplificado_path')
    if not p or not os.path.exists(p):
        return pd.DataFrame()
    try:
        df = pd.read_excel(p, dtype=str).fillna("")
    except Exception:
        return pd.DataFrame()

    # renomeia colunas principais
    rename = {}
    for a,b in {
        'Placa':'Placa','Nome Responsável':'Responsavel','RESPONSÁVEL':'Responsavel','RESPONSAVEL':'Responsavel',
        'Limite Atual':'LimiteAtual','Limite':'Limite','Compras (utilizado)':'Compras','Saldo':'Saldo',
        'Limite Próximo Período':'LimiteProximo','Cidade/UF':'CidadeUF','Modelo':'Modelo','Fabricante':'Fabricante',
        'Família':'Familia','Tipo Frota':'TipoFrota'
    }.items():
        if a in df.columns:
            rename[a] = b
    df = df.rename(columns=rename)

    # normaliza numéricos
    for src, dst in [('LimiteAtual','LimiteAtual'),('Limite','Limite'),('Compras','Compras'),('Saldo','Saldo'),('LimiteProximo','LimiteProximo')]:
        if src in df.columns:
            df[dst] = df[src].map(_num)
    df['PLACA_N'] = df.get('Placa','').map(_norm_placa)

    # vincula responsável por placa se vazio
    resp = _load_responsavel()
    if not resp.empty:
        df = df.merge(resp, on='PLACA_N', how='left')
        # prioriza nome existente na planilha quando houver
        if 'Responsavel' in df.columns:
            df['Responsavel'] = df['Responsavel'].where(df['Responsavel'].astype(str).str.strip()!='', df['RESPONSAVEL'])
        else:
            df['Responsavel'] = df['RESPONSAVEL']
        if 'RESPONSAVEL' in df.columns:
            df.drop(columns=['RESPONSAVEL'], inplace=True)
    else:
        if 'Responsavel' not in df.columns:
            df['Responsavel'] = ''
    return df


def _load_extrato_geral() -> pd.DataFrame:
    p = cfg_get('extrato_geral_path')
    if not p or not os.path.exists(p):
        return pd.DataFrame()
    try:
        df = pd.read_excel(p, dtype=str).fillna("")
    except Exception:
        return pd.DataFrame()

    rename = {}
    for a,b in {
        'DATA TRANSACAO':'DATA','PLACA':'PLACA','NOME MOTORISTA':'MOTORISTA','Motorista':'MOTORISTA','MOTORISTA':'MOTORISTA',
        'TIPO COMBUSTIVEL':'COMBUSTIVEL','LITROS':'LITROS','VL/LITRO':'VL_LITRO','VALOR EMISSAO':'VALOR','NOME ESTABELECIMENTO':'ESTABELECIMENTO',
        'CIDADE':'CIDADE','UF':'UF','CIDADE/UF':'CIDADE_UF'
    }.items():
        if a in df.columns:
            rename[a] = b
    df = df.rename(columns=rename)

    if 'CIDADE_UF' not in df.columns:
        df['CIDADE_UF'] = df.get('CIDADE','').astype(str).str.strip()+"/"+df.get('UF','').astype(str).str.strip()

    df['DT'] = df.get('DATA','').map(_to_date)
    df['LITROS_NUM'] = df.get('LITROS','').map(_num)
    df['VL_LITRO_NUM'] = df.get('VL_LITRO','').map(_num)
    df['VALOR_NUM'] = df.get('VALOR','').map(_num)
    df['PLACA_N'] = df.get('PLACA','').map(_norm_placa)

    # Responsável por placa (mapa ativo)
    resp = _load_responsavel()
    if not resp.empty:
        df = df.merge(resp, on='PLACA_N', how='left')
        df.rename(columns={'RESPONSAVEL':'Responsavel'}, inplace=True)
        df['Responsavel'] = df['Responsavel'].fillna('')
    else:
        df['Responsavel'] = ''
    return df


# ================== UI: Combustível (10 cenários) ==================
class CombustivelWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Combustível — Visão Administrativa Geral")
        self.resize(1280, 860)

        self.df_simpl = pd.DataFrame()   # estado atual
        self.df_hist  = pd.DataFrame()   # histórico (extrato geral)

        root = QVBoxLayout(self)

        # Barra de período (histórico) + filtro global
        bar = QFrame(); bar.setObjectName('card')
        bv = QHBoxLayout(bar)
        self.de_ini = QDateEdit(); self.de_fim = QDateEdit()
        for de in (self.de_ini, self.de_fim):
            de.setCalendarPopup(True); de.setDisplayFormat(DATE_FORMAT)
        today = pd.Timestamp.today().normalize()
        self.de_ini.setDate(QDate(today.year, today.month, 1))
        self.de_fim.setDate(QDate(today.year, today.month, today.day))
        btn_reload = QPushButton("Recarregar fontes"); btn_reload.clicked.connect(self._load)
        bv.addWidget(QLabel("Início:")); bv.addWidget(self.de_ini)
        bv.addWidget(QLabel("Fim:")); bv.addWidget(self.de_fim)
        bv.addStretch(1); bv.addWidget(btn_reload)
        root.addWidget(bar)

        self.global_bar = GlobalFilterBar("Filtro global (aplica às listas históricas):")
        self.global_bar.changed.connect(self._refresh)
        root.addWidget(self.global_bar)

        # KPIs atuais (simplificado)
        kcard = QFrame(); kcard.setObjectName('glass')
        kv = QHBoxLayout(kcard)
        self.k_lim_atual = QLabel('0,00'); self.k_compras = QLabel('0,00'); self.k_saldo = QLabel('0,00'); self.k_lim_next = QLabel('0,00')
        for lab in (self.k_lim_atual, self.k_compras, self.k_saldo, self.k_lim_next):
            lab.setFont(QFont('Arial', 14, QFont.Weight.Bold))
        kv.addWidget(QLabel('Σ Limite Atual (R$):')); kv.addWidget(self.k_lim_atual)
        kv.addWidget(QLabel('Σ Compras (R$):')); kv.addWidget(self.k_compras)
        kv.addWidget(QLabel('Σ Saldo (R$):')); kv.addWidget(self.k_saldo)
        kv.addWidget(QLabel('Σ Limite Próx. (R$):')); kv.addWidget(self.k_lim_next)
        root.addWidget(kcard)

        # sub-abas (10)
        self.tabs = QTabWidget(); root.addWidget(self.tabs, 1)

        # tabelas
        self.tbl_top_saldo = self._mk_tbl()
        self.tbl_top_lim = self._mk_tbl()
        self.tbl_top_lim_next = self._mk_tbl()
        self.tbl_top_compras = self._mk_tbl()
        self.tbl_top_saldo_pct = self._mk_tbl()

        self.tbl_hist_custo_resp = self._mk_tbl()
        self.tbl_hist_litros_resp = self._mk_tbl()
        self.tbl_hist_preco_resp = self._mk_tbl()
        self.tbl_hist_estab_caro = self._mk_tbl()
        self.tbl_hist_cidade = self._mk_tbl()

        self._add_tab(self.tbl_top_saldo, "TOP10 • Maiores saldos (atual)")
        self._add_tab(self.tbl_top_lim, "TOP10 • Maiores limites atuais")
        self._add_tab(self.tbl_top_lim_next, "TOP10 • Maiores limites próximos")
        self._add_tab(self.tbl_top_compras, "TOP10 • Maiores compras (atual)")
        self._add_tab(self.tbl_top_saldo_pct, "TOP10 • Maior % saldo sobre limite atual")

        self._add_tab(self.tbl_hist_custo_resp, "TOP10 • Responsáveis por custo (histórico)")
        self._add_tab(self.tbl_hist_litros_resp, "TOP10 • Responsáveis por litros (histórico)")
        self._add_tab(self.tbl_hist_preco_resp, "TOP10 • Responsáveis por R$/L médio (mín. 10 abastec.)")
        self._add_tab(self.tbl_hist_estab_caro, "TOP10 • Estabelecimentos por R$/L médio (mín. 30 abastec.)")
        self._add_tab(self.tbl_hist_cidade, "TOP10 • Cidades/UF por custo (histórico)")

        # sinais
        self.de_ini.dateChanged.connect(self._refresh)
        self.de_fim.dateChanged.connect(self._refresh)

        self._load()

    def _mk_tbl(self):
        t = QTableWidget(); t.setAlternatingRowColors(True)
        t.setSortingEnabled(True)
        t.horizontalHeader().setSortIndicatorShown(True)
        t.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        t.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
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
            self.df_hist = _load_extrato_geral()
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
        # período histórico
        a = pd.Timestamp(self.de_ini.date().year(), self.de_ini.date().month(), self.de_ini.date().day())
        b = pd.Timestamp(self.de_fim.date().year(), self.de_fim.date().month(), self.de_fim.date().day())
        if a > b:
            a, b = b, a

        hist = self.df_hist.copy()
        if not hist.empty and 'DT' in hist.columns:
            hist = hist[(hist['DT'].notna()) & (hist['DT'] >= a) & (hist['DT'] <= b)]
            hist = df_apply_global_texts(hist, self.global_bar.values())
            if not isinstance(hist, pd.DataFrame):
                hist = pd.DataFrame()
        
        # ========= Cenários ATUAIS =========
        s = self.df_simpl.copy()
        if not s.empty:
            s['LimiteAtual'] = s.get('LimiteAtual', 0.0)
            s['Compras'] = s.get('Compras', 0.0)
            s['Saldo'] = s.get('Saldo', 0.0)
            s['LimiteProximo'] = s.get('LimiteProximo', 0.0)
            s['pctSaldo'] = (100.0 * s['Saldo'] / s['LimiteAtual']).replace([pd.NA, pd.NaT, float('inf')], 0.0)
        else:
            s = pd.DataFrame(columns=['Responsavel','Placa','LimiteAtual','Compras','Saldo','LimiteProximo','pctSaldo'])

        top = lambda df, n=10: df.head(n) if not df.empty else df
        
        top_saldo = s.sort_values(['Saldo','LimiteAtual'], ascending=[False, False])[['Responsavel','Placa','Saldo','LimiteAtual','Compras','LimiteProximo','CidadeUF']].rename(columns={'Responsavel':'Responsável'})
        top_lim = s.sort_values('LimiteAtual', ascending=False)[['Responsavel','Placa','LimiteAtual','Saldo','Compras','LimiteProximo']].rename(columns={'Responsavel':'Responsável'})
        top_lim_next = s.sort_values('LimiteProximo', ascending=False)[['Responsavel','Placa','LimiteProximo','LimiteAtual','Saldo']].rename(columns={'Responsavel':'Responsável'})
        top_comp = s.sort_values('Compras', ascending=False)[['Responsavel','Placa','Compras','LimiteAtual','Saldo']].rename(columns={'Responsavel':'Responsável'})
        top_ps = s.sort_values('pctSaldo', ascending=False)[['Responsavel','Placa','pctSaldo','Saldo','LimiteAtual']].rename(columns={'Responsavel':'Responsável'})

        self._fill(self.tbl_top_saldo, top(top_saldo), ['Responsável','Placa','Saldo','LimiteAtual','Compras','LimiteProximo','CidadeUF'], money_cols={'Saldo','LimiteAtual','Compras','LimiteProximo'})
        self._fill(self.tbl_top_lim, top(top_lim), ['Responsável','Placa','LimiteAtual','Saldo','Compras','LimiteProximo'], money_cols={'Saldo','LimiteAtual','Compras','LimiteProximo'})
        self._fill(self.tbl_top_lim_next, top(top_lim_next), ['Responsável','Placa','LimiteProximo','LimiteAtual','Saldo'], money_cols={'Saldo','LimiteAtual','LimiteProximo'})
        self._fill(self.tbl_top_compras, top(top_comp), ['Responsável','Placa','Compras','LimiteAtual','Saldo'], money_cols={'Saldo','LimiteAtual','Compras'})
        self._fill(self.tbl_top_saldo_pct, top(top_ps), ['Responsável','Placa','pctSaldo','Saldo','LimiteAtual'], money_cols={'Saldo','LimiteAtual'}, pct_cols={'pctSaldo'})

        # ========= Cenários HISTÓRICOS =========
        if hist.empty:
            agg_resp = pd.DataFrame(columns=['Responsável','Abastecimentos','Litros','Custo','R$/L'])
            agg_estab = pd.DataFrame(columns=['Estabelecimento','Abastecimentos','R$/L'])
            agg_city = pd.DataFrame(columns=['Cidade/UF','Abastecimentos','Custo'])
        else:
            g = hist.groupby(hist['Responsavel'].astype(str).str.strip())
            agg_resp = g.agg(
                Abastecimentos=('VALOR_NUM','count'),
                Litros=('LITROS_NUM','sum'),
                Custo=('VALOR_NUM','sum')
            ).reset_index().rename(columns={'Responsavel':'Responsável'})
            agg_resp['R$/L'] = (agg_resp['Custo'] / agg_resp['Litros']).replace([pd.NA, pd.NaT, float('inf')], 0.0)

            g_e = hist.groupby(hist['ESTABELECIMENTO'].astype(str).str.strip())
            agg_estab = g_e.agg(
                Abastecimentos=('VALOR_NUM','count'),
                Custo=('VALOR_NUM','sum'),
                Litros=('LITROS_NUM','sum')
            ).reset_index().rename(columns={'ESTABELECIMENTO':'Estabelecimento'})
            agg_estab['R$/L'] = (agg_estab['Custo']/agg_estab['Litros']).replace([pd.NA, pd.NaT, float('inf')], 0.0)

            g_c = hist.groupby(hist['CIDADE_UF'].astype(str).str.strip())
            agg_city = g_c.agg(
                Abastecimentos=('VALOR_NUM','count'),
                Custo=('VALOR_NUM','sum')
            ).reset_index().rename(columns={'CIDADE_UF':'Cidade/UF'})

        # tops
        self._fill(self.tbl_hist_custo_resp, top(agg_resp.sort_values(['Custo','Abastecimentos'], ascending=[False, False])),
                   ['Responsável','Custo','Abastecimentos','Litros','R$/L'], money_cols={'Custo'})
        self._fill(self.tbl_hist_litros_resp, top(agg_resp.sort_values(['Litros','Abastecimentos'], ascending=[False, False])),
                   ['Responsável','Litros','Abastecimentos','Custo','R$/L'], money_cols={'Custo'})

        # preço médio por responsável (mín. 10 abast.)
        resp_preco = agg_resp[agg_resp['Abastecimentos']>=10].sort_values('R$/L', ascending=False)
        self._fill(self.tbl_hist_preco_resp, top(resp_preco), ['Responsável','R$/L','Abastecimentos','Custo','Litros'], money_cols={'Custo'})

        estab_caro = agg_estab[agg_estab['Abastecimentos']>=30].sort_values('R$/L', ascending=False)
        self._fill(self.tbl_hist_estab_caro, top(estab_caro), ['Estabelecimento','R$/L','Abastecimentos','Custo','Litros'], money_cols={'Custo'})

        self._fill(self.tbl_hist_cidade, top(agg_city.sort_values(['Custo','Abastecimentos'], ascending=[False, False])),
                   ['Cidade/UF','Custo','Abastecimentos'], money_cols={'Custo'})

    def _fill(self, tbl: QTableWidget, df: pd.DataFrame, headers, money_cols=None, pct_cols=None):
        money_cols = money_cols or set(); pct_cols = pct_cols or set()
        tbl.setSortingEnabled(False)
        tbl.clear(); tbl.setColumnCount(len(headers)); tbl.setHorizontalHeaderLabels(headers)
        tbl.setRowCount(0 if df is None else len(df))
        if df is None or df.empty:
            tbl.setSortingEnabled(True); return
        for i, (_, r) in enumerate(df.iterrows()):
            for j, c in enumerate(headers):
                val = r.get(c, '')
                if c in money_cols:
                    val = self._fmt_money(val)
                elif c in pct_cols:
                    try:
                        val = f"{float(val or 0):.1f}%"
                    except Exception:
                        pass
                it = QTableWidgetItem(str(val))
                it.setFlags(it.flags() & ~Qt.ItemFlag.ItemIsEditable)
                tbl.setItem(i, j, it)
        tbl.resizeColumnsToContents(); tbl.horizontalHeader().setStretchLastSection(True)
        tbl.setSortingEnabled(True)