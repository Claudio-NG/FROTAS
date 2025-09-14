# organizar_planilhas.py
from PyQt6.QtCore import QObject, QThread, pyqtSignal
from PyQt6.QtWidgets import QDialog, QVBoxLayout, QHBoxLayout, QLabel, QProgressBar, QTextEdit, QPushButton
from sanitize_planilhas import processar_todas_planilhas

class _Worker(QObject):
    progressed = pyqtSignal(int)
    logged = pyqtSignal(str)
    finished = pyqtSignal(dict)
    failed = pyqtSignal(str)

    def __init__(self, root_folder: str):
        super().__init__()
        self.root = root_folder

    def run(self):
        try:
            def _log(msg): self.logged.emit(str(msg))
            def _prog(p): self.progressed.emit(int(p))
            res = processar_todas_planilhas(self.root, log=_log, progress=_prog)
            self.finished.emit(res)
        except Exception as e:
            self.failed.emit(str(e))

class OrganizarPlanilhasDialog(QDialog):
    def __init__(self, root_folder="PLANILHAS"):
        super().__init__()
        self.setWindowTitle("Organizando Planilhas…")
        self.resize(720, 420)

        v = QVBoxLayout(self)
        v.addWidget(QLabel(f"Pasta alvo: {root_folder}"))
        self.pb = QProgressBar(); self.pb.setRange(0, 100); v.addWidget(self.pb)
        self.log = QTextEdit(); self.log.setReadOnly(True); v.addWidget(self.log, 1)

        bar = QHBoxLayout(); v.addLayout(bar)
        self.btn_close = QPushButton("Fechar"); self.btn_close.setEnabled(False)
        bar.addStretch(1); bar.addWidget(self.btn_close)
        self.btn_close.clicked.connect(self.accept)

        # thread
        self.thread = QThread(self)
        self.worker = _Worker(root_folder)
        self.worker.moveToThread(self.thread)
        self.thread.started.connect(self.worker.run)
        self.worker.progressed.connect(self.pb.setValue)
        self.worker.logged.connect(lambda s: self.log.append(s))
        self.worker.finished.connect(self._on_done)
        self.worker.failed.connect(self._on_fail)
        self.thread.start()

    def _on_done(self, res: dict):
        self.log.append("\n✔ Concluído.\n")
        self.pb.setValue(100)
        self.result_data = res
        self.btn_close.setEnabled(True)
        self.thread.quit(); self.thread.wait()

    def _on_fail(self, err: str):
        self.log.append(f"\n❌ Erro: {err}")
        self.btn_close.setEnabled(True)
        self.thread.quit(); self.thread.wait()
