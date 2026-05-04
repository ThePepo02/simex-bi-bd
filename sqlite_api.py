from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import sqlite3
import urllib.request

DB_PATH = "/app/spark-warehouse/prime_logistics.sqlite"
OLLAMA_URL = "http://ollama:11434/api/generate"

class Handler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass

    def do_GET(self):
        if self.path == "/health":
            self._responder(200, {
                "status": "ok",
                "tablas": ["que_se_mueve", "quien_mueve", "rentabilidad", "desequilibrio"]
            })
        else:
            self._responder(404, {"error": "Ruta no encontrada"})

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = json.loads(self.rfile.read(length))

        if self.path == "/query":
            self._handle_query(body)
        elif self.path == "/chat":
            self._handle_chat(body)
        else:
            self._responder(404, {"error": "Ruta no encontrada"})

    def _handle_chat(self, body):
        pregunta = body.get("pregunta", "")

        prompt = (
            f"Eres un experto en SQL con acceso a una base de datos SQLite llamada prime_logistics "
            f"con 4 tablas con estas columnas EXACTAS: "
            f"que_se_mueve (category, commodity, flow, total_usd, total_kg, num_transacciones, valor_por_kg), "
            f"quien_mueve (country_or_area, flow, year, total_usd, total_kg, num_transacciones), "
            f"rentabilidad (category, year, total_usd, total_kg, usd_por_kg), "
            f"desequilibrio (country_or_area, year, Export, Import, balance_usd). "
            f'El usuario pregunta: "{pregunta}". '
            f"IMPORTANTE: consulta SOLO UNA tabla, la mas relevante. "
            f"Usa siempre el formato tabla.columna. "
            f"Responde UNICAMENTE con SQL puro sin explicaciones ni markdown."
        )

        ollama_payload = json.dumps({
            "model": "llama3.2:3b",
            "prompt": prompt,
            "stream": False
        }).encode()

        try:
            req = urllib.request.Request(
                OLLAMA_URL,
                data=ollama_payload,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=120) as r:
                ollama_resp = json.loads(r.read())

            sql_raw = ollama_resp.get("response", "")
            sql = sql_raw.replace("```sql", "").replace("```", "").strip()

            if not sql.upper().startswith("SELECT"):
                self._responder(400, {"error": "SQL no válido", "sql_generado": sql})
                return

            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            try:
                cur.execute(sql)
                rows = [dict(r) for r in cur.fetchall()]
                conn.close()
                self._responder(200, {
                    "pregunta": pregunta,
                    "sql": sql,
                    "respuesta": rows,
                    "total_filas": len(rows)
                })
            except Exception as e:
                conn.close()
                self._responder(400, {"error": str(e), "sql_generado": sql})

        except Exception as e:
            self._responder(500, {"error": str(e)})

    def _handle_query(self, body):
        sql = body.get("sql", "").strip()
        if not sql.upper().startswith("SELECT"):
            self._responder(400, {"error": "Solo se permiten consultas SELECT"})
            return
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(sql)
            rows = [dict(r) for r in cur.fetchall()]
            conn.close()
            self._responder(200, {"data": rows, "total": len(rows)})
        except Exception as e:
            self._responder(400, {"error": str(e)})

    def _responder(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(body)

print("SQLite API corriendo en puerto 5050...")
HTTPServer(("0.0.0.0", 5050), Handler).serve_forever()