#!/usr/bin/env python3
"""
Renfe Cercanías GTFS Archiver
Descarga https://ssl.renfe.com/ftransit/Fichero_CER_FOMENTO/fomento_transit.zip
y extrae un CSV con todos los trayectos del día:
  data/YYYY/MM/gtfs-YYYY-MM-DD.csv

Solo escribe si el contenido ha cambiado respecto al día anterior.
"""

import csv
import datetime
import hashlib
import io
import os
import sys
import time
import urllib.request
import zipfile

GTFS_URL = "https://ssl.renfe.com/ftransit/Fichero_CER_FOMENTO/fomento_transit.zip"

COLUMNAS = [
    "fecha",
    "trip_id",
    "block_id",
    "codLinea",
    "ruta",
    "cod_origen",
    "estacion_origen",
    "hora_salida",
    "cod_destino",
    "estacion_destino",
    "hora_llegada",
    "num_paradas",
    "accesible",
]


def fetch() -> bytes:
    req = urllib.request.Request(
        GTFS_URL,
        headers={"User-Agent": "renfe-cercanias-gtfs-archiver/1.0"},
        method="GET",
    )
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                return resp.read()
        except Exception as e:
            if attempt == 2:
                print(f"ERROR tras 3 intentos: {e}", file=sys.stderr)
                sys.exit(1)
            print(f"Intento {attempt + 1} fallido: {e}. Reintentando...", file=sys.stderr)
            time.sleep(10)


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def last_hash_path(out_dir: str) -> str:
    return os.path.join(out_dir, ".last_hash_gtfs")


def load_last_hash(out_dir: str):
    p = last_hash_path(out_dir)
    return open(p).read().strip() if os.path.exists(p) else None


def save_hash(out_dir: str, h: str):
    with open(last_hash_path(out_dir), "w") as f:
        f.write(h)


def parse_gtfs(data: bytes) -> list:
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        def read(name):
            with zf.open(name) as f:
                lines = f.read().decode("utf-8-sig").splitlines()
            reader = csv.DictReader(lines)
            reader.fieldnames = [c.strip() for c in reader.fieldnames]
            return [{k.strip(): v.strip() for k, v in row.items()} for row in reader]

        trips      = read("trips.txt")
        stops      = read("stops.txt")
        stop_times = read("stop_times.txt")
        routes     = read("routes.txt")

    stop_name  = {s["stop_id"]: s["stop_name"] for s in stops}
    route_info = {r["route_id"]: (r.get("route_short_name", ""), r.get("route_long_name", "")) for r in routes}

    trip_info = {
        t["trip_id"]: {
            "block_id": t.get("block_id", ""),
            "route_id": t.get("route_id", ""),
            "accesible": t.get("wheelchair_accessible", ""),
        }
        for t in trips
    }

    # Agrupar stop_times por trip: solo primera y ultima parada
    from collections import defaultdict
    trip_first_last = {}
    for st in stop_times:
        tid = st["trip_id"]
        try:
            seq = int(st["stop_sequence"])
        except ValueError:
            seq = 0
        if tid not in trip_first_last:
            trip_first_last[tid] = {"first": (seq, st), "last": (seq, st), "count": 1}
        else:
            entry = trip_first_last[tid]
            entry["count"] += 1
            if seq < entry["first"][0]:
                entry["first"] = (seq, st)
            if seq > entry["last"][0]:
                entry["last"] = (seq, st)

    rows = []
    for trip_id, fl in trip_first_last.items():
        info = trip_info.get(trip_id, {})
        route_id = info.get("route_id", "")
        linea, ruta = route_info.get(route_id, ("", ""))

        first_st = fl["first"][1]
        last_st = fl["last"][1]

        rows.append({
            "trip_id":          trip_id,
            "block_id":         info.get("block_id", ""),
            "codLinea":         linea,
            "ruta":             ruta.strip(),
            "cod_origen":       first_st["stop_id"],
            "estacion_origen":  stop_name.get(first_st["stop_id"], first_st["stop_id"]),
            "hora_salida":      first_st.get("departure_time", ""),
            "cod_destino":      last_st["stop_id"],
            "estacion_destino": stop_name.get(last_st["stop_id"], last_st["stop_id"]),
            "hora_llegada":     last_st.get("arrival_time", ""),
            "num_paradas":      fl["count"],
            "accesible":        info.get("accesible", ""),
        })

    return sorted(rows, key=lambda r: (r["codLinea"], r["hora_salida"]))


def extract_raw(data: bytes, raw_dir: str):
    os.makedirs(raw_dir, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        zf.extractall(raw_dir)
    print(f"Raw GTFS  : {raw_dir}/")


def main():
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    fecha   = now_utc.strftime("%Y-%m-%d")
    out_dir = os.path.join("data", now_utc.strftime("%Y"), now_utc.strftime("%m"))
    os.makedirs(out_dir, exist_ok=True)

    print(f"Descargando GTFS desde {GTFS_URL} ...")
    data = fetch()
    h    = sha256(data)

    if load_last_hash(out_dir) == h:
        print("Sin cambios respecto a la descarga anterior. No se escribe CSV.")
        sys.exit(0)

    # Extraer CSV resumen: primera y ultima parada de cada tren
    print("Procesando GTFS...")
    trayectos = parse_gtfs(data)

    csv_path = os.path.join(out_dir, f"gtfs-{fecha}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNAS)
        writer.writeheader()
        for row in trayectos:
            writer.writerow({"fecha": fecha, **row})

    # No guardamos archivos raw (trips.txt es 21 MB)

    save_hash(out_dir, h)

    print(f"Guardado  : {csv_path}")
    print(f"Raw       : {raw_dir}/ (sin stop_times ni shapes)")
    print(f"Fecha     : {fecha}")
    print(f"Trayectos : {len(trayectos)}")


if __name__ == "__main__":
    main()
