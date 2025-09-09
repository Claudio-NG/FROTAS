import os, re
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import GlobalFilterBar, df_apply_global_texts
from PyQt6.QtCore import Qt, QDate, pyqtSignal, QObject
from PyQt6.QtGui import QFont, QColor, QStandardItemModel, QStandardItem
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QFrame, QHBoxLayout, QLabel, QLineEdit, QPushButton,
    QCompleter, QDateEdit, QTableWidget, QHeaderView, QTableWidgetItem, QGridLayout,
    QMessageBox, QComboBox
)

DATE_FORMAT = "dd/MM/yyyy"


def _num(s):
    s = str(s or "").strip()
    if not s: return 0.0
    s = re.sub(r"[^\d,.-]", "", s)
    s = s.replace(".", "").replace(",", ".") if ("," in s and "." in s) else s.replace(",", ".")
    try: return float(s)
    except: return 0.0

def _to_date(s):
    return pd.to_datetime(str(s or ""), dayfirst=True, errors="coerce")

def _norm_placa(x: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(x or "").upper())

def _guess_points(valor: float) -> int:
    """Estimativa simples por faixas usuais de valores (aproximação)."""
    v = float(valor or 0)
    # arredonda para 2 casas por segurança
    v = round(v, 2)
    # Faixas típicas atuais (exemplos):
    #  88.38 ~ leve (3 pts); 130.16 ~ média (4 pts); 195.23 ~ grave (5 pts); 293.47 ~ gravíssima (7 pts)
    if abs(v - 88.38) <= 0.5:   return 3
    if abs(v - 130.16) <= 0.8:  return 4
    if abs(v - 195.23) <= 1.0:  return 5
    if abs(v - 293.47) <= 1.5:  return 7
    # fallback por faixa
    if v <= 100:   return 3
    if v <= 160:   return 4
    if v <= 230:   return 5
    return 7

# =========================== Sinais ===========================

class _Sig(QObject):
    ready = pyqtSignal(str, pd.DataFrame)
    error = pyqtSignal(str)

# =========================== Janela ===========================

class CondutorWindow(QWidget):
    """
    Busca integrada por condutor:
      - Extrato Geral (combustível) + ExtratoSimplificado + Responsavel (dados atuais)
      - Multas: só planilhas (detalhamentos, pastores, condutor identificado)
      - Combinar/Comparar dois nomes
      - KPIs + Pontuação (janela) e Pontuação Fixa (últimos 12 meses desde hoje)
    """
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Condutor — Busca Integrada (Avançado)")
        self.resize(1320, 880)
        self.sig = _Sig()
        self.sig.ready.connect(self._on_chunk_ready)
        self.sig.error.connect(self._on_error)

        # ---- ARQUIVOS NA RAIZ ----
        # Combustível
        self.p_extrato = "ExtratoGeral.xlsx"              # seu arquivo já usado (ajuste se necessário)
        self.p_simpl   = "ExtratoSimplificado.xlsx"       # NOVO
        self.p_resp    = "Responsavel.xlsx"               # NOVO

        # Multas (várias fontes possíveis)
        self.p_multas_sources = [
            "Notificações de Multas - Detalhamento.xlsx",
            "Notificações de Multas - Detalhamento-2.xlsx",
            "Notificações de Multas - Detalhamento (1).xlsx",
            "Notificações de Multas - Fase Pastores.xlsx",
            "Notificações de Multas - Condutor Identificado.xlsx",
        ]

        # DataFrames da visão atual
        self._df_m = pd.DataFrame()   # multas (consolidadas de fontes)
        self._df_e = pd.DataFrame()   # extrato geral
        self._df_dados = {}           # "Dados atuais do condutor" (dict simples)
        self._presence = pd.DataFrame()  # matriz de presença de FLUIG por fonte
        self._kpis_multas = {"descontado":0.0, "pendente":0.0, "pontos_periodo":0, "pontos_12m":0}

        # cache por nome
        self._cache = {}  # nome -> {"M": df_m, "E": df_e, "DADOS": dict, "PRESENCE": df, "KPIS": dict}

        self.names_model = QStandardItemModel(self)
        self._build_ui()
        self._build_completer_source()

    # ---------------- UI ----------------
    def _build_ui(self):
        root = QVBoxLayout(self)

        # Cabeçalho
        head = QFrame(); head.setObjectName("glass")
        self._apply_shadow(head)
        hv = QVBoxLayout(head); hv.setContentsMargins(18,18,18,18)
        t = QLabel("Condutor — Busca Integrada (Avançado)"); t.setAlignment(Qt.AlignmentFlag.AlignCenter)
        t.setFont(QFont("Arial", 22, QFont.Weight.Bold))
        hv.addWidget(t)
        root.addWidget(head)

        # Barra de parâmetros
        bar = QFrame(); bar.setObjectName("card"); self._apply_shadow(bar, radius=16, blur=40)
        bl = QGridLayout(bar)

        self.ed_nome = QLineEdit();  self.ed_nome.setPlaceholderText("Nome 1 (ou escolha uma sugestão)…")
        self.ed_nome2 = QLineEdit(); self.ed_nome2.setPlaceholderText("Nome 2 (opcional para combinar/comparar)…")

        self.cb_mode = QComboBox(); self.cb_mode.addItems(["Combinar", "Comparar"])

        self.btn_carregar = QPushButton("Carregar (Nome 1)")
        self.btn_carregar.setMinimumHeight(36)

        self.btn_exec = QPushButton("Executar (Combinar/Comparar)")
        self.btn_exec.setMinimumHeight(36)

        self.de_ini = QDateEdit(); self.de_fim = QDateEdit()
        for de in (self.de_ini, self.de_fim):
            de.setCalendarPopup(True); de.setDisplayFormat(DATE_FORMAT)
        today = pd.Timestamp.today().normalize()
        self.de_ini.setDate(QDate(today.year, today.month, 1))
        self.de_fim.setDate(QDate(today.year, today.month, today.day))
        self.de_ini.dateChanged.connect(self._apply_filters)
        self.de_fim.dateChanged.connect(self._apply_filters)

        self.global_bar = GlobalFilterBar("Filtro global:")
        self.global_bar.changed.connect(self._apply_filters)

        bl.addWidget(QLabel("Nome 1:"), 0, 0); bl.addWidget(self.ed_nome, 0, 1)
        bl.addWidget(QLabel("Nome 2:"), 0, 2); bl.addWidget(self.ed_nome2, 0, 3)
        bl.addWidget(QLabel("Modo:"), 1, 0);  bl.addWidget(self.cb_mode, 1, 1)
        bl.addWidget(QLabel("Início:"), 1, 2); bl.addWidget(self.de_ini, 1, 3)
        bl.addWidget(QLabel("Fim:"),    1, 4); bl.addWidget(self.de_fim, 1, 5)
        bl.addWidget(self.global_bar, 2, 0, 1, 6)

        row_btns = QHBoxLayout()
        row_btns.addWidget(self.btn_carregar)
        row_btns.addStretch(1)
        row_btns.addWidget(self.btn_exec)
        bl.addLayout(row_btns, 3, 0, 1, 6)
        root.addWidget(bar)

        # KPIs e Dados Atuais
        cards = QFrame(); cards.setObjectName("glass"); self._apply_shadow(cards, radius=16, blur=50)
        cg = QGridLayout(cards)

        # KPIs (multas/combustível)
        self.k_multas = QLabel("0"); self.k_valor = QLabel("0,00")
        self.k_abast  = QLabel("0"); self.k_litros = QLabel("0,00"); self.k_custo = QLabel("0,00")
        self.k_desc   = QLabel("0,00"); self.k_pend = QLabel("0,00")
        self.k_pts_periodo = QLabel("0"); self.k_pts_12m = QLabel("0")

        kitems = [
            ("Multas", self.k_multas),
            ("Valor Multas (R$)", self.k_valor),
            ("Abastecimentos", self.k_abast),
            ("Litros", self.k_litros),
            ("Custo Combustível (R$)", self.k_custo),
            ("Descontado (R$)", self.k_desc),
            ("Não Descontado (R$)", self.k_pend),
            ("Pontos no Período", self.k_pts_periodo),
            ("Pontos (últimos 12m)", self.k_pts_12m),
        ]
        for i, (lab, val) in enumerate(kitems):
            cg.addWidget(QLabel(lab), 0, i)
            val.setFont(QFont("Arial", 13, QFont.Weight.Bold)); val.setAlignment(Qt.AlignmentFlag.AlignCenter)
            cg.addWidget(val, 1, i)

        # DADOS ATUAIS DO CONDUTOR
        self.box_dados = QFrame(); self.box_dados.setObjectName("card"); self._apply_shadow(self.box_dados, radius=14, blur=30)
        dv = QGridLayout(self.box_dados)
        self.h_dados_title = QLabel("DADOS ATUAIS DO CONDUTOR"); self.h_dados_title.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        dv.addWidget(self.h_dados_title, 0, 0, 1, 4)
        self.l_placa = QLabel("-"); self.l_modelo = QLabel("-"); self.l_fab = QLabel("-"); self.l_cidade = QLabel("-")
        self.l_lim_atual = QLabel("-"); self.l_compras = QLabel("-"); self.l_saldo = QLabel("-"); self.l_lim_next = QLabel("-")

        # linha 1
        dv.addWidget(QLabel("Placa Atual:"), 1, 0); dv.addWidget(self.l_placa, 1, 1)
        dv.addWidget(QLabel("Modelo:"), 1, 2);     dv.addWidget(self.l_modelo, 1, 3)
        # linha 2
        dv.addWidget(QLabel("Fabricante:"), 2, 0); dv.addWidget(self.l_fab, 2, 1)
        dv.addWidget(QLabel("Cidade/UF:"), 2, 2);  dv.addWidget(self.l_cidade, 2, 3)
        # linha 3
        dv.addWidget(QLabel("Limite Atual (R$):"), 3, 0); dv.addWidget(self.l_lim_atual, 3, 1)
        dv.addWidget(QLabel("Compras (R$):"), 3, 2);      dv.addWidget(self.l_compras, 3, 3)
        # linha 4
        dv.addWidget(QLabel("Saldo (R$):"), 4, 0);        dv.addWidget(self.l_saldo, 4, 1)
        dv.addWidget(QLabel("Limite Próx. Período (R$):"), 4, 2); dv.addWidget(self.l_lim_next, 4, 3)

        cg.addWidget(self.box_dados, 2, 0, 1, len(kitems))
        root.addWidget(cards)

        # Tabelas
        self.tbl_m = self._mk_table(["FONTE","FLUIG","Status","Data","Placa","Infração","Valor (R$)","Descontada?"])
        self.tbl_e = self._mk_table(["Data","Placa","Motorista","Combustível","Litros","R$/L","R$","Estabelecimento","Cidade/UF"])
        wrap = QHBoxLayout()
        wrap.addWidget(self.tbl_m, 1)
        wrap.addWidget(self.tbl_e, 1)
        root.addLayout(wrap)

        # ações
        self.btn_carregar.clicked.connect(self._load_one_and_show)
        self.btn_exec.clicked.connect(self._execute_mode)

    def _apply_shadow(self, w, radius=18, blur=60, color=QColor(0,0,0,70)):
        from PyQt6.QtWidgets import QGraphicsDropShadowEffect
        eff = QGraphicsDropShadowEffect()
        eff.setOffset(0, 6)
        eff.setBlurRadius(blur)
        eff.setColor(color)
        w.setGraphicsEffect(eff)

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

    # ---------------- Autocomplete ----------------
    def _build_completer_source(self):
        names = set()
        # Responsavel (NOME)
        if os.path.exists(self.p_resp):
            try:
                dr = pd.read_excel(self.p_resp, dtype=str).fillna("")
                if "NOME" in dr.columns:
                    names |= set([x for x in dr["NOME"].astype(str) if x.strip()])
            except Exception:
                pass
        # Extrato Geral (Motorista / Responsável)
        if os.path.exists(self.p_extrato):
            try:
                de = pd.read_excel(self.p_extrato, dtype=str).fillna("")
                for cand in ("NOME MOTORISTA","Motorista","MOTORISTA","Responsável","RESPONSÁVEL","RESPONSAVEL","Nome Responsável"):
                    if cand in de.columns:
                        names |= set([x for x in de[cand].astype(str) if x.strip()])
            except Exception:
                pass
        # Extrato Simplificado (Nome Responsável)
        if os.path.exists(self.p_simpl):
            try:
                ds = pd.read_excel(self.p_simpl, dtype=str).fillna("")
                for cand in ("Nome Responsável","RESPONSÁVEL","RESPONSAVEL","Responsável","Responsavel"):
                    if cand in ds.columns:
                        names |= set([x for x in ds[cand].astype(str) if x.strip()])
                        break
            except Exception:
                pass
        # Multas (várias fontes — coluna Nome)
        for p in self.p_multas_sources:
            if os.path.exists(p):
                try:
                    dm = pd.read_excel(p, dtype=str).fillna("")
                    for cand in ("Nome","NOME"):
                        if cand in dm.columns:
                            names |= set([x for x in dm[cand].astype(str) if x.strip()])
                            break
                except Exception:
                    pass

        self.names_model.clear()
        for n in sorted(names):
            self.names_model.appendRow(QStandardItem(n))
        comp1 = QCompleter(self.names_model, self); comp1.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive); comp1.setFilterMode(Qt.MatchFlag.MatchContains)
        comp2 = QCompleter(self.names_model, self); comp2.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive); comp2.setFilterMode(Qt.MatchFlag.MatchContains)
        self.ed_nome.setCompleter(comp1); self.ed_nome2.setCompleter(comp2)

    # ---------------- Fluxos ----------------
    def _load_one_and_show(self):
        name = self.ed_nome.text().strip()
        if not name:
            QMessageBox.information(self, "Condutor", "Informe o Nome 1.")
            return
        packs = self._load_for_many([name])
        self._set_active_from_single(name, packs)
        self._apply_filters()

    def _execute_mode(self):
        name1 = self.ed_nome.text().strip()
        name2 = self.ed_nome2.text().strip()
        mode = self.cb_mode.currentText()

        if not name1 and not name2:
            QMessageBox.information(self, "Condutor", "Informe pelo menos o Nome 1.")
            return
        if name1 and not name2:
            self._load_one_and_show(); return

        packs = self._load_for_many([name1, name2])
        if mode == "Combinar":
            self._set_active_combined(packs)
            self._apply_filters()
        else:  # Comparar
            self._set_active_combined(packs)
            self._apply_filters()
            self._show_compare_summary(packs)

    # ---------------- Carregador por nome ----------------
    def _load_for_many(self, names: list[str]) -> dict:
        names = [n for n in names if n.strip()]
        out = {}
        todo = []
        for n in names:
            if n in self._cache:
                out[n] = self._cache[n]
            else:
                todo.append(n)

        def _load_all_for(n):
            m, presence, kpis = self._load_multas_for(n)
            e = self._load_extrato_for(n)
            dados = self._load_dados_atuais(n)  # Responsavel + ExtratoSimplificado
            return n, {"M": m, "E": e, "DADOS": dados, "PRESENCE": presence, "KPIS": kpis}

        if todo:
            with ThreadPoolExecutor(max_workers=min(3, len(todo))) as ex:
                futs = [ex.submit(_load_all_for, n) for n in todo]
                for fut in as_completed(futs):
                    try:
                        nm, pack = fut.result()
                        self._cache[nm] = pack
                        out[nm] = pack
                    except Exception as e:
                        self.sig.error.emit(str(e))

        return out

    def _set_active_from_single(self, name, packs):
        pack = packs.get(name, {})
        self._df_m = pack.get("M", pd.DataFrame()).copy()
        self._df_e = pack.get("E", pd.DataFrame()).copy()
        self._presence = pack.get("PRESENCE", pd.DataFrame()).copy()
        self._kpis_multas = pack.get("KPIS", {"descontado":0.0, "pendente":0.0, "pontos_periodo":0, "pontos_12m":0})
        self._fill_dados_atuais(pack.get("DADOS", {}))

    def _set_active_combined(self, packs):
        ms = []; es = []
        # para comparação, não unificamos dados atuais/presence/kpis; exibimos só das tabelas e KPIs somados do período atual
        k_desc = k_pend = pts_periodo = pts_12m = 0.0
        for nm, pack in packs.items():
            m = pack.get("M", pd.DataFrame())
            e = pack.get("E", pd.DataFrame())
            if not m.empty:
                m = m.copy(); m["FONTE"] = m["FONTE"]  # já tem
                ms.append(m)
            if not e.empty:
                es.append(e.copy())
            k = pack.get("KPIS", {})
            k_desc += float(k.get("descontado", 0))
            k_pend += float(k.get("pendente", 0))
            pts_periodo += float(k.get("pontos_periodo", 0))
            pts_12m += float(k.get("pontos_12m", 0))

        self._df_m = pd.concat(ms, ignore_index=True) if ms else pd.DataFrame()
        self._df_e = pd.concat(es, ignore_index=True) if es else pd.DataFrame()
        self._presence = pd.DataFrame()
        self._kpis_multas = {"descontado":k_desc, "pendente":k_pend, "pontos_periodo":int(pts_periodo), "pontos_12m":int(pts_12m)}
        # limpa “Dados atuais” na combinação
        self._fill_dados_atuais({})

    # ---------------- Leitura: Dados atuais ----------------
    def _load_dados_atuais(self, name: str) -> dict:
        """
        Usa planilhas:
          - Responsavel.xlsx (para descobrir PLACA atual do pastor)
          - ExtratoSimplificado.xlsx (para limites/compras/saldo baseados na placa e/ou no nome)
        """
        placa_atual = modelo = fabricante = cidade_uf = ""
        lim_atual = compras = saldo = lim_next = ""

        # 1) Responsavel.xlsx -> placa/modelo/fabricante/status
        if os.path.exists(self.p_resp):
            try:
                dr = pd.read_excel(self.p_resp, dtype=str).fillna("")
                # Filtra por nome (coluna "NOME")
                if "NOME" in dr.columns:
                    q = dr[dr["NOME"].astype(str).str.contains(re.escape(name), case=False, na=False)].copy()
                else:
                    q = dr.copy()
                # Preferir registro “ativo”: STATUS não “VENDIDO” e DATA FIM vazia; senão, o mais recente por DATA INÍCIO
                if not q.empty:
                    # normaliza datas
                    q["DT_INI"] = _to_date(q.get("DATA INÍCIO", ""))
                    q["DT_FIM"] = _to_date(q.get("DATA FIM", ""))
                    act = q[(q.get("STATUS","").str.upper() != "VENDIDO") & (q["DT_FIM"].isna())]
                    cand = act if not act.empty else q
                    cand = cand.sort_values(["DT_INI"], ascending=[False])
                    r = cand.iloc[0]
                    placa_atual = str(r.get("PLACA",""))
                    modelo = str(r.get("MODELO",""))
                    fabricante = str(r.get("MARCA",""))
                    cidade_uf = str(r.get("UF","")).strip()  # UF pode vir separada; ficará combinado mais abaixo se necessário
            except Exception:
                pass

        # 2) ExtratoSimplificado.xlsx -> limites/saldo por placa/nome
        if os.path.exists(self.p_simpl):
            try:
                ds = pd.read_excel(self.p_simpl, dtype=str).fillna("")
                # normaliza nomes de colunas possíveis
                m = {
                    "Placa":"Placa",
                    "Família":"Familia",
                    "Tipo Frota":"Tipo Frota",
                    "Modelo":"Modelo",
                    "Fabricante":"Fabricante",
                    "Cidade/UF":"Cidade/UF",
                    "Nome Responsável":"Nome Responsável",
                    "Limite":"Limite",
                    "Valor Reservado":"Valor Reservado",
                    "Limite Atual":"Limite Atual",
                    "Compras (utilizado)":"Compras",
                    "Saldo":"Saldo",
                    "Limite Próximo Período":"Limite Próximo"
                }
                ren = {k:v for k,v in m.items() if k in ds.columns}
                ds = ds.rename(columns=ren)

                # tenta casar pela placa (com/sem hífen) e, na falta, pelo nome responsável
                hit = pd.DataFrame()
                if placa_atual:
                    pnorm = _norm_placa(placa_atual)
                    ds["_PL"] = ds.get("Placa","").map(_norm_placa)
                    hit = ds[ds["_PL"] == pnorm]
                if hit.empty:
                    for c in ("Nome Responsável",):
                        if c in ds.columns:
                            hit = ds[ds[c].astype(str).str.contains(re.escape(name), case=False, na=False)]
                            if not hit.empty: break
                if not hit.empty:
                    r = hit.iloc[0]
                    if not modelo: modelo = str(r.get("Modelo",""))
                    if not fabricante: fabricante = str(r.get("Fabricante",""))
                    if str(r.get("Cidade/UF","")).strip(): cidade_uf = str(r.get("Cidade/UF",""))
                    lim_atual = str(r.get("Limite Atual",""))
                    compras  = str(r.get("Compras",""))
                    saldo    = str(r.get("Saldo",""))
                    lim_next = str(r.get("Limite Próximo",""))
            except Exception:
                pass

        return {
            "placa": placa_atual or "-",
            "modelo": modelo or "-",
            "fabricante": fabricante or "-",
            "cidade": cidade_uf or "-",
            "limite_atual": lim_atual or "-",
            "compras": compras or "-",
            "saldo": saldo or "-",
            "limite_proximo": lim_next or "-",
        }

    def _fill_dados_atuais(self, d: dict):
        self.l_placa.setText(d.get("placa","-"))
        self.l_modelo.setText(d.get("modelo","-"))
        self.l_fab.setText(d.get("fabricante","-"))
        self.l_cidade.setText(d.get("cidade","-"))
        self.l_lim_atual.setText(d.get("limite_atual","-"))
        self.l_compras.setText(d.get("compras","-"))
        self.l_saldo.setText(d.get("saldo","-"))
        self.l_lim_next.setText(d.get("limite_proximo","-"))

    # ---------------- Leitura: Combustível (Extrato Geral) ----------------
    def _load_extrato_for(self, name: str) -> pd.DataFrame:
        if not os.path.exists(self.p_extrato):
            return pd.DataFrame()
        try:
            df = pd.read_excel(self.p_extrato, dtype=str).fillna("")
        except Exception:
            return pd.DataFrame()

        # filtra por nome (em Motorista; se não, em Responsável)
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

        # normaliza colunas
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

        df["DT_C"] = df.get("DATA_TRANSACAO","").map(_to_date)
        for c_src, c_num in [("LITROS","LITROS_NUM"),("VL_LITRO","VL_LITRO_NUM"),("VALOR","VALOR_NUM")]:
            df[c_num] = df.get(c_src, "").map(_num)

        return df

    # ---------------- Leitura: Multas (todas as planilhas) ----------------
    def _load_multas_for(self, name: str):
        """
        Consolida todas as fontes de multas, ignora CANCELADA, marca 'Descontada' pela Fase Pastores
        e constrói matriz de presença de FLUIG por fonte. Calcula KPIs de multas.
        """
        frames = []
        presentes = {}  # fonte -> set(FLUIG)
        fonte_alias = {
            "Notificações de Multas - Detalhamento.xlsx":"Detalhamento",
            "Notificações de Multas - Detalhamento-2.xlsx":"Detalhamento-2",
            "Notificações de Multas - Detalhamento (1).xlsx":"Detalhamento(1)",
            "Notificações de Multas - Fase Pastores.xlsx":"Fase Pastores",
            "Notificações de Multas - Condutor Identificado.xlsx":"Condutor Identificado",
        }

        # Carregar todas as fontes existentes
        for path in self.p_multas_sources:
            if not os.path.exists(path): 
                continue
            try:
                df = pd.read_excel(path, dtype=str).fillna("")
            except Exception:
                continue

            # filtro por Nome (quando existir a coluna)
            col_nome = None
            for c in ("Nome","NOME"):
                if c in df.columns:
                    col_nome = c; break
            if col_nome:
                df = df[df[col_nome].astype(str).str.contains(re.escape(name), case=False, na=False)]

            # ignora CANCELADA (se tiver col Status)
            col_status = next((c for c in df.columns if c.strip().lower() == "status"), None)
            if col_status:
                df = df[df[col_status].str.upper() != "CANCELADA"]

            # normaliza colunas necessárias
            col_fluig = next((c for c in df.columns if "FLUIG" in c.upper()), None)
            col_data  = next((c for c in df.columns if "DATA INFRA" in c.upper()), None)
            col_valor = next((c for c in df.columns if "VALOR TOTAL" in c.upper()), None)
            col_inf   = next((c for c in df.columns if c.upper() in ("INFRAÇÃO","INFRACAO")), None)
            col_placa = next((c for c in df.columns if c.strip().upper() == "PLACA"), None)

            tmp = pd.DataFrame()
            if col_fluig: tmp["FLUIG"] = df[col_fluig]
            else:         tmp["FLUIG"] = ""
            tmp["Status"] = df.get(col_status, "")
            tmp["Data"]   = df.get(col_data, "")
            tmp["Placa"]  = df.get(col_placa, "")
            tmp["Infração"] = df.get(col_inf, "")
            tmp["Valor"]  = df.get(col_valor, df.get("Valor", ""))

            tmp["VALOR_NUM"] = tmp["Valor"].map(_num)
            tmp["DT_M"] = tmp["Data"].map(_to_date)
            tmp["FONTE"] = fonte_alias.get(path, os.path.basename(path))

            # Fase Pastores: detectar “descontada”
            if "Fase Pastores" in tmp["FONTE"].iloc[0]:
                # precisa destas colunas se existirem
                col_tipo = next((c for c in df.columns if c.strip().upper() == "TIPO"), None)
                col_data_past = next((c for c in df.columns if c.strip().upper() == "DATA PASTORES"), None)
                # monta chave por índice alinhado
                tipo = df.get(col_tipo, "")
                data_p = df.get(col_data_past, "")
                disc = (tipo.astype(str).str.upper() == "MULTAS PASTORES") & (data_p.astype(str).str.strip() != "")
                tmp["DESCONTADA"] = disc.astype(bool)
            else:
                tmp["DESCONTADA"] = False

            frames.append(tmp)
            presentes[tmp["FONTE"].iloc[0]] = set(tmp["FLUIG"].astype(str).unique())

        # Consolida tudo
        df_all = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["FONTE","FLUIG","Status","Data","Placa","Infração","Valor","VALOR_NUM","DT_M","DESCONTADA"])

        # Matriz de presença por FLUIG x FONTE
        presence = pd.DataFrame()
        if not df_all.empty:
            sources = sorted(df_all["FONTE"].unique().tolist())
            presence = pd.DataFrame(index=sorted(df_all["FLUIG"].astype(str).unique()), columns=sources)
            presence[:] = ""
            for src, vals in presentes.items():
                for fl in vals:
                    presence.loc[str(fl), src] = "✓"
            presence.index.name = "FLUIG"

        # KPIs de multas (só janela: período será aplicado depois)
        # Aqui guardamos apenas totais brutos; _apply_filters recalcula por período
        kpis = {"descontado": float(df_all.loc[df_all["DESCONTADA"], "VALOR_NUM"].sum() or 0.0),
                "pendente": float(df_all.loc[~df_all["DESCONTADA"], "VALOR_NUM"].sum() or 0.0),
                "pontos_periodo": 0, "pontos_12m": 0}

        return df_all, presence, kpis

    # ---------------- Filtros + KPIs ----------------
    def _apply_filters(self):
        q0, q1 = self.de_ini.date(), self.de_fim.date()
        a = pd.Timestamp(q0.year(), q0.month(), q0.day())
        b = pd.Timestamp(q1.year(), q1.month(), q1.day())
        if a > b: a, b = b, a

        dm = self._df_m.copy()
        de = self._df_e.copy()

        # filtro por datas + busca global
        if not dm.empty and "DT_M" in dm.columns:
            dm = dm[(dm["DT_M"].notna()) & (dm["DT_M"] >= a) & (dm["DT_M"] <= b)]
        if not de.empty and "DT_C" in de.columns:
            de = de[(de["DT_C"].notna()) & (de["DT_C"] >= a) & (de["DT_C"] <= b)]

        dm = df_apply_global_texts(dm, self._global_values())
        de = df_apply_global_texts(de, self._global_values())

        # KPIs período
        vm = float(dm["VALOR_NUM"].sum() or 0.0) if not dm.empty else 0.0
        self.k_multas.setText(str(len(dm)))
        self.k_valor.setText(self._fmt_money(vm))

        ab_qt = len(de) if not de.empty else 0
        litros = float(de.get("LITROS_NUM", pd.Series()).sum() or 0.0) if not de.empty else 0.0
        custo  = float(de.get("VALOR_NUM", pd.Series()).sum() or 0.0) if not de.empty else 0.0
        self.k_abast.setText(str(ab_qt))
        self.k_litros.setText(self._fmt_num(litros))
        self.k_custo.setText(self._fmt_money(custo))

        # Descontado / Não descontado (no período filtrado)
        desc = float(dm.loc[dm["DESCONTADA"]==True, "VALOR_NUM"].sum() or 0.0)
        pend = float(dm.loc[dm["DESCONTADA"]==False, "VALOR_NUM"].sum() or 0.0)
        self.k_desc.setText(self._fmt_money(desc))
        self.k_pend.setText(self._fmt_money(pend))

        # Pontos no período (estimativa)
        pts = int(dm["VALOR_NUM"].map(_guess_points).sum()) if not dm.empty else 0
        self.k_pts_periodo.setText(str(pts))

        # Pontos fixos últimos 12 meses (hoje-1 ano .. hoje)
        today = pd.Timestamp.today().normalize()
        a12 = today - pd.DateOffset(years=1)
        dm12 = self._df_m.copy()
        if not dm12.empty and "DT_M" in dm12.columns:
            dm12 = dm12[(dm12["DT_M"].notna()) & (dm12["DT_M"] >= a12) & (dm12["DT_M"] <= today)]
            pts12 = int(dm12["VALOR_NUM"].map(_guess_points).sum())
        else:
            pts12 = 0
        self.k_pts_12m.setText(str(pts12))

        # updates
        self._fill_multas(dm)
        self._fill_extrato(de)

    def _fill_multas(self, dm: pd.DataFrame):
        headers = ["FONTE","FLUIG","Status","Data","Placa","Infração","Valor (R$)","Descontada?"]
        rows = []
        if not dm.empty:
            for _, r in dm.sort_values(["DT_M","FONTE","VALOR_NUM"], ascending=[True, True, False]).iterrows():
                rows.append([
                    r.get("FONTE",""),
                    r.get("FLUIG",""),
                    r.get("Status",""),
                    r["DT_M"].strftime("%d/%m/%Y") if pd.notna(r.get("DT_M", pd.NaT)) else str(r.get("Data","")),
                    r.get("Placa",""),
                    r.get("Infração",""),
                    f"{float(r.get('VALOR_NUM',0)):.2f}",
                    "Sim" if bool(r.get("DESCONTADA", False)) else "Não"
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

    def _global_values(self):
        try:
            return self.global_bar.values()
        except Exception:
            return []

    def _fmt_money(self, x): 
        return f"{float(x or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X",".")

    def _fmt_num(self, x): 
        return f"{float(x or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X",".")

    # ---------------- Comparação (resumo) ----------------
    def _show_compare_summary(self, packs: dict):
        lines = ["Resumo comparativo (período selecionado):\n"]
        a = pd.Timestamp(self.de_ini.date().year(), self.de_ini.date().month(), self.de_ini.date().day())
        b = pd.Timestamp(self.de_fim.date().year(), self.de_fim.date().month(), self.de_fim.date().day())

        for nm, pack in packs.items():
            dm = pack.get("M", pd.DataFrame()).copy()
            de = pack.get("E", pd.DataFrame()).copy()

            if not dm.empty and "DT_M" in dm:
                dm = dm[(dm["DT_M"].notna()) & (dm["DT_M"] >= a) & (dm["DT_M"] <= b)]
            if not de.empty and "DT_C" in de:
                de = de[(de["DT_C"].notna()) & (de["DT_C"] >= a) & (de["DT_C"] <= b)]

            multas_qt = len(dm)
            multas_val = float(dm["VALOR_NUM"].sum() or 0.0)
            ab_qt = len(de)
            litros = float(de.get("LITROS_NUM", pd.Series()).sum() or 0.0)
            custo  = float(de.get("VALOR_NUM", pd.Series()).sum() or 0.0)
            pts = int(dm["VALOR_NUM"].map(_guess_points).sum()) if not dm.empty else 0

            lines.append(
                f"• {nm}:\n"
                f"   - Multas: {multas_qt} (R$ { self._fmt_money(multas_val) }) | Pontos: {pts}\n"
                f"   - Combustível: {ab_qt} | Litros: { self._fmt_num(litros) } | Custo: R$ { self._fmt_money(custo) }"
            )

        QMessageBox.information(self, "Comparar", "\n".join(lines))

  
    def _on_chunk_ready(self, tag: str, df: pd.DataFrame): pass
    def _on_error(self, msg: str): QMessageBox.warning(self, "Condutor", msg)

