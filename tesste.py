from flask import Flask, send_from_directory, abort, request, redirect, url_for
from pathlib import Path
import threading
import socket

app = Flask(__name__)
ROOT = Path.cwd()

@app.get("/")
def index():
    files = [p.name for p in ROOT.iterdir() if p.is_file()]
    links = "".join(f'<li><a href="/download/{f}">{f}</a></li>' for f in files)
    return f"<h2>Servidor de teste</h2><p>Arquivos em {ROOT}</p><ul>{links}</ul>"

@app.get("/download/<path:filename>")
def download(filename):
    p = (ROOT / filename).resolve()
    try:
        p.relative_to(ROOT.resolve())
    except Exception:
        abort(404)
    if not p.exists() or not p.is_file():
        abort(404)
    return send_from_directory(str(ROOT), filename, as_attachment=True)

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip

def run_flask(port=8000):
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)

if __name__ == "__main__":
    port = 8000
    t = threading.Thread(target=run_flask, args=(port,), daemon=True)
    t.start()
    local_ip = get_local_ip()
    print(f"Servidor rodando localmente em: http://{local_ip}:{port}/")
    try:
        from pyngrok import ngrok
        public_url = ngrok.connect(port, "http")
        print(f"Tunnel público (ngrok): {public_url.public_url}")
    except Exception as e:
        print("pyngrok não disponível ou falhou ao criar túnel. Para expor à internet sem domínio use ngrok:")
        print(f"1) Instale ngrok e opcionalmente configure authtoken.")
        print(f"2) Execute no terminal: ngrok http {port}")
    print("Pressione Ctrl+C para encerrar.")
    try:
        while True:
            t.join(1)
    except KeyboardInterrupt:
        try:
            from pyngrok import ngrok
            ngrok.kill()
        except:
            pass
        print("Servidor finalizado.")