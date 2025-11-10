from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import sqlite3

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "drops"
OUT = ROOT / "output"
OUT.mkdir(parents=True, exist_ok=True)
(OUT / "parquet").mkdir(parents=True, exist_ok=True)
(OUT / "quality").mkdir(parents=True, exist_ok=True)

# 1) Ingesta
files = sorted(DATA.glob("*.csv")) + sorted(DATA.glob("*.ndjson")) + sorted(DATA.glob("*.jsonl"))
raw = []
for f in files:
    if f.suffix.lower() == ".csv":
        df = pd.read_csv(f, dtype=str)
    else:  # ndjson/jsonl
        df = pd.read_json(f, lines=True, dtype=str)
    df["_source_file"] = f.name
    df["_ingest_ts"] = datetime.now(timezone.utc).isoformat()
    raw.append(df)

if raw:
    raw_df = pd.concat(raw, ignore_index=True)
else:
    cols = ["fecha","id_cliente","id_producto","unidades","precio_unitario","_source_file","_ingest_ts"]
    raw_df = pd.DataFrame(columns=cols)

# 2) Limpieza (coerción + validación + dedupe)
def to_float_money(x):
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return None

df = raw_df.copy()
for c in ["fecha","id_cliente","id_producto","unidades","precio_unitario"]:
    if c not in df.columns: 
        df[c] = None

df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date
df["unidades"] = pd.to_numeric(df["unidades"], errors="coerce")
df["precio_unitario"] = df["precio_unitario"].apply(to_float_money)

<<<<<<< HEAD
# Límites razonables para validación
MAX_UNIDADES = 100  # máximo 100 unidades por transacción
MAX_PRECIO = 1000.0  # máximo 1000€ por unidad
MIN_FECHA = pd.to_datetime("2025-01-01").date()  # no ventas antes de 2025
MAX_FECHA = datetime.now(timezone.utc).date()  # no ventas futuras

valid = (
    # Validaciones existentes
=======
valid = (
>>>>>>> 7bdfc871baa9bcef1032f7aef3e635b35571e00b
    df["fecha"].notna()
    & df["unidades"].notna() & (df["unidades"] >= 0)
    & df["precio_unitario"].notna() & (df["precio_unitario"] >= 0)
    & df["id_cliente"].notna() & (df["id_cliente"] != "")
    & df["id_producto"].notna() & (df["id_producto"] != "")
<<<<<<< HEAD
    # Nuevas validaciones de rango
    & (df["unidades"] <= MAX_UNIDADES)  # límite superior de unidades
    & (df["precio_unitario"] <= MAX_PRECIO)  # límite superior de precio
    & (df["fecha"] >= MIN_FECHA)  # fecha mínima
    & (df["fecha"] <= MAX_FECHA)  # no ventas futuras
    # Validación de formato de IDs
    & df["id_cliente"].str.match(r'^C\d{3}$').fillna(False)  # formato CXXX
    & df["id_producto"].str.match(r'^P\d{3}$').fillna(False)  # formato PXXX
=======
>>>>>>> 7bdfc871baa9bcef1032f7aef3e635b35571e00b
)

quarantine = df.loc[~valid].copy()
clean = df.loc[valid].copy()

if not clean.empty:
    clean = (clean.sort_values("_ingest_ts")
                  .drop_duplicates(subset=["fecha","id_cliente","id_producto"], keep="last"))
    clean["importe"] = clean["unidades"] * clean["precio_unitario"]

# 3) Persistencia: Parquet (fuente de reporte) + SQLite (opcional integrado)
quarantine.to_csv(OUT / "quality" / "ventas_invalidas.csv", index=False)
PARQUET_FILE = OUT / "parquet" / "clean_ventas.parquet"
if not clean.empty:
    clean.to_parquet(PARQUET_FILE, index=False)

# SQLite
DB = OUT / "ut1.db"
con = sqlite3.connect(DB)
# DDL
con.executescript((ROOT / "sql" / "00_schema.sql").read_text(encoding="utf-8"))
# RAW
if not df.empty:
    df_raw = df[["fecha","id_cliente","id_producto","unidades","precio_unitario","_ingest_ts","_source_file"]].copy()
    df_raw["_batch_id"] = "demo"  # simplificado
    df_raw.to_sql("raw_ventas", con, if_exists="append", index=False)
# CLEAN via UPSERT
if not clean.empty:
    upsert_sql = (ROOT / "sql" / "10_upserts.sql").read_text(encoding="utf-8")
    for _, r in clean.iterrows():
        con.execute(
            upsert_sql,
            {
                "fecha": str(r["fecha"]),
                "idc": r["id_cliente"],
                "idp": r["id_producto"],
                "u": float(r["unidades"]),
                "p": float(r["precio_unitario"]),
                "ts": r["_ingest_ts"],
            },
        )
    con.commit()
# Vistas
con.executescript((ROOT / "sql" / "20_views.sql").read_text(encoding="utf-8"))
con.close()

# 4) Reporte releído desde PARQUET
if PARQUET_FILE.exists():
    clean_rep = pd.read_parquet(PARQUET_FILE)
else:
    clean_rep = pd.DataFrame(columns=["fecha","id_cliente","id_producto","unidades","precio_unitario","_ingest_ts","importe"])

if not clean_rep.empty:
    ingresos = float(clean_rep["importe"].sum())
    trans = int(len(clean_rep))
    ticket = float(ingresos / trans) if trans > 0 else 0.0

    top = (clean_rep.groupby("id_producto", as_index=False)
                    .agg(importe=("importe","sum"))
                    .sort_values("importe", ascending=False))
    total_imp = top["importe"].sum() or 1.0
    top["pct"] = (100*top["importe"]/total_imp).round(0).astype(int).astype(str) + "%"

    by_day = (clean_rep.groupby("fecha", as_index=False)
                        .agg(importe_total=("importe","sum"),
                             transacciones=("importe","count")))
    periodo_ini = str(clean_rep["fecha"].min())
    periodo_fin = str(clean_rep["fecha"].max())
    producto_lider = top.iloc[0]["id_producto"] if not top.empty else "—"
else:
    ingresos = 0.0; ticket = 0.0; trans = 0
    top = pd.DataFrame(columns=["id_producto","importe","pct"])
    by_day = pd.DataFrame(columns=["fecha","importe_total","transacciones"])
    periodo_ini = "—"; periodo_fin = "—"; producto_lider = "—"

report = (
    "# Reporte UT1 · Ventas\n"
    f"**Periodo:** {periodo_ini} a {periodo_fin} · **Fuente:** clean_ventas (Parquet) · **Generado:** {datetime.now(timezone.utc).isoformat()}\n\n"
    "## 1. Titular\n"
    f"Ingresos totales {ingresos:.2f} €; producto líder: {producto_lider}.\n\n"
    "## 2. KPIs\n"
    f"- **Ingresos netos:** {ingresos:.2f} €\n"
    f"- **Ticket medio:** {ticket:.2f} €\n"
    f"- **Transacciones:** {trans}\n\n"
    "## 3. Top productos\n"
    f"{(top.to_markdown(index=False) if not top.empty else '_(sin datos)_')}\n\n"
    "## 4. Resumen por día\n"
    f"{(by_day.to_markdown(index=False) if not by_day.empty else '_(sin datos)_')}\n\n"
    "## 5. Calidad y cobertura\n"
    f"- Filas bronce: {len(df)} · Plata: {len(clean)} · Cuarentena: {len(quarantine)}\n\n"
    "## 6. Persistencia\n"
    f"- Parquet: {PARQUET_FILE}\n"
    f"- SQLite : {DB} (tablas: raw_ventas, clean_ventas; vista: ventas_diarias)\n\n"
    "## 7. Conclusiones\n"
    "- Reponer producto líder según demanda.\n"
    "- Revisar filas en cuarentena (rangos/tipos).\n"
    "- Valorar particionado por fecha para crecer.\n"
)

(OUT / "reporte.md").write_text(report, encoding="utf-8")
print("OK · Generado:", OUT / "reporte.md")
print("OK · Parquet :", PARQUET_FILE if PARQUET_FILE.exists() else "sin datos")
print("OK · SQLite  :", DB)
