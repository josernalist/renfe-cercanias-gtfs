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
    "tren",
    "codLinea",
    "nucleo",
    "sentido",
    "cod_origen",
    "estacion_origen",
    "cod_destino",
    "estacion_destino",
    "num_paradas",
    "paradas",
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
    route_info = {r["route_id"]: r.get("route_short_name", "") for r in routes}

    trip_info = {
        t["trip_id"]: {
            "tren": t.get("trip_short_name", "").strip(),
            "route_id": t.get("route_id", ""),
        }
        for t in trips
    }

    from collections import defaultdict
    trip_stops = defaultdict(list)
    for st in stop_times:
        try:
            seq = int(st["stop_sequence"])
        except ValueError:
            seq = 0
        trip_stops[st["trip_id"]].append((seq, st["stop_id"]))

    rows = {}
    for trip_id, stops_list in trip_stops.items():
        stops_list.sort(key=lambda x: x[0])
        info = trip_info.get(trip_id, {})
        tren = info.get("tren", "")
        if not tren:
            continue

        cod_origen  = stops_list[0][1]
        cod_destino = stops_list[-1][1]
        key = (tren, cod_origen, cod_destino)

        if key not in rows:
            linea = route_info.get(info.get("route_id", ""), "")
            sentido = "Par" if tren.isdigit() and int(tren) % 2 == 0 else "Impar" if tren.isdigit() else ""
            rows[key] = {
                "tren":             tren,
                "codLinea":         linea,
                "nucleo":           "",
                "sentido":          sentido,
                "cod_origen":       cod_origen,
                "estacion_origen":  stop_name.get(cod_origen, cod_origen),
                "cod_destino":      cod_destino,
                "estacion_destino": stop_name.get(cod_destino, cod_destino),
                "num_paradas":      len(stops_list),
                "paradas":          " > ".join(stop_name.get(s[1], s[1]) for s in stops_list),
            }

    return sorted(rows.values(), key=lambda r: r["tren"])


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

    # Guardar el zip directamente (no extraer, los raw son demasiado grandes)
    zip_path = os.path.join(out_dir, f"gtfs-{fecha}.zip")
    with open(zip_path, "wb") as f:
        f.write(data)

    save_hash(out_dir, h)

    print(f"Fecha     : {fecha}")
    print(f"ZIP       : {zip_path} ({len(data) / 1024 / 1024:.1f} MB)")


if __name__ == "__main__":
    main()
