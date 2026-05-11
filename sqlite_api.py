from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import sqlite3
import urllib.request

DB_PATH = "/app/spark-warehouse/prime_logistics.sqlite"
OLLAMA_URL = "http://ollama:11434/api/generate"


def formatear_fila(row, idx):
    nombres = {
        "category": "Categoría",
        "commodity": "Producto",
        "flow": "Flujo",
        "total_usd": "Total USD",
        "total_kg": "Total KG",
        "num_transacciones": "Transacciones",
        "valor_por_kg": "USD/kg",
        "country_or_area": "País",
        "year": "Año",
        "usd_por_kg": "USD/kg",
        "Export": "Exportación USD",
        "Import": "Importación USD",
        "balance_usd": "Balance USD",
    }
    lineas = [f"**{idx}.**"]
    for k, v in row.items():
        if v is None:
            continue
        if isinstance(v, (int, float)) and abs(v) > 1000:
            v_fmt = f"{v:,.0f}"
        else:
            v_fmt = str(v)
        clave = nombres.get(k, k)
        lineas.append(f"- {clave}: {v_fmt}")
    return "\n".join(lineas)


class Handler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass

    def do_GET(self):
        if self.path == "/health":
            self._responder(200, {"status": "ok"})
        else:
            self._responder(404, {"error": "Not found"})

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = json.loads(self.rfile.read(length))
        if self.path == "/chat":
            self._handle_chat(body)
        elif self.path == "/query":
            self._handle_query(body)
        else:
            self._responder(404, {"error": "Not found"})

    def _handle_chat(self, body):
        pregunta = body.get("pregunta", "").lower()

        if any(p in pregunta for p in ["producto", "commodity", "categoria", "category", "kg", "valor"]):
            tabla = "que_se_mueve"
            columnas = "category, commodity, flow, total_usd, total_kg, num_transacciones, valor_por_kg"
        elif any(p in pregunta for p in ["pais", "country", "exporta", "importa", "quien"]):
            tabla = "quien_mueve"
            columnas = "country_or_area, flow, year, total_usd, total_kg, num_transacciones"
        elif any(p in pregunta for p in ["rentab", "beneficio", "usd_por_kg"]):
            tabla = "rentabilidad"
            columnas = "category, year, total_usd, total_kg, usd_por_kg"
        elif any(p in pregunta for p in ["desequilibrio", "balance", "export", "import"]):
            tabla = "desequilibrio"
            columnas = "country_or_area, year, Export, Import, balance_usd"
        else:
            tabla = "que_se_mueve"
            columnas = "category, commodity, flow, total_usd, total_kg, num_transacciones, valor_por_kg"

        prompt = (
            f"Escribe SQL para SQLite.\n"
            f"USA SOLO esta tabla: {tabla}\n"
            f"Columnas disponibles: {columnas}\n"
            f"PROHIBIDO usar otras columnas o tablas.\n"
            f"Termina con LIMIT 10.\n"
            f"Escribe SOLO el SQL. PROHIBIDO escribir cualquier texto después del punto y coma. Sin notas, sin explicaciones, sin comentarios.\n\n"
            f"Pregunta: {pregunta}\n"
            f"SQL:"
        )

        payload = json.dumps({
            "model": "llama3.2:3b",
            "prompt": prompt,
            "stream": False
        }).encode()

        try:
            req = urllib.request.Request(
                OLLAMA_URL,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=120) as r:
                resp = json.loads(r.read())

            sql = resp.get("response", "").replace("```sql", "").replace("```", "").strip()

            # Cortar todo lo que venga después del primer punto y coma
            if ";" in sql:
                sql = sql[:sql.index(";") + 1].strip()
            # Eliminar líneas que no sean SQL
            sql = "\n".join(
                line for line in sql.splitlines()
                if not line.strip().startswith("Nota") and not line.strip().startswith("--")
            ).strip()

            if not sql.upper().startswith("SELECT"):
                self._responder(400, {"error": "SQL no valido", "sql_generado": sql})
                return

            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            try:
                cur.execute(sql)
                rows = [dict(r) for r in cur.fetchall()]
                conn.close()

                texto = f"Encontré **{len(rows)} resultado(s)**:\n\n"
                for i, row in enumerate(rows[:5], 1):
                    texto += formatear_fila(row, i) + "\n\n"
                if len(rows) > 5:
                    texto += f"_(mostrando 5 de {len(rows)})_"

                self._responder(200, {
                    "pregunta": pregunta,
                    "sql": sql,
                    "respuesta": texto,
                    "datos": rows,
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
            self._responder(400, {"error": "Solo SELECT"})
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