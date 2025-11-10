from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import sqlite3


def main(regen_data: bool = False, regen_force: bool = False):
    """Ejecuta la canalización: ingesta -> limpieza -> persistencia -> reporte.

    Args:
        regen_data: si es True, regenera los datos de ejemplo antes de ejecutar
            (llama al helper generate_sample en get_data)
        regen_force: si es True y regen_data es True, fuerza la sobrescritura del archivo de muestra
    """
    ROOT = Path(__file__).resolve().parents[1]
    DATA = ROOT / "data" / "drops"
    OUT = ROOT / "output"
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "parquet").mkdir(parents=True, exist_ok=True)
    (OUT / "quality").mkdir(parents=True, exist_ok=True)

    if regen_data:
        try:
            from . import get_data
        except Exception:
            import project.ingest.get_data as get_data
        # wrapper compatible
        get_data.generate_sample(drop_dir=DATA, force=regen_force)

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
        cols = ["fecha", "id_cliente", "id_producto", "nombre_producto", "unidades", "precio_unitario", "_source_file", "_ingest_ts"]
        raw_df = pd.DataFrame(columns=cols)

    # 2) Limpieza (coerción de tipos + validación por filas + deduplicación)
    def to_float_money(x):
        try:
            return float(str(x).replace(",", "."))
        except Exception:
            return None

    df = raw_df.copy()
    for c in ["fecha", "id_cliente", "id_producto", "nombre_producto", "unidades", "precio_unitario"]:
        if c not in df.columns:
            df[c] = None

    # validación a nivel de fila
    def validate_row(r):
        reasons = []
        # fecha
        try:
            f = pd.to_datetime(r.get("fecha"), errors="coerce")
            if pd.isna(f):
                reasons.append("fecha inválida o faltante")
        except Exception:
            reasons.append("fecha inválida")

        # id_cliente
        idc = r.get("id_cliente")
        if idc is None or str(idc).strip() == "":
            reasons.append("id_cliente faltante")

        # id_producto
        idp = r.get("id_producto")
        if idp is None or str(idp).strip() == "":
            reasons.append("id_producto faltante")

        # unidades
        try:
            u = float(str(r.get("unidades")))
            if u < 0:
                reasons.append("unidades negativas")
            if u == 0:
                reasons.append("unidades cero")
        except Exception:
            reasons.append("unidades no numéricas")

        # precio
        try:
            p = to_float_money(r.get("precio_unitario"))
            if p is None:
                reasons.append("precio no numérico o faltante")
            elif p < 0:
                reasons.append("precio negativo")
        except Exception:
            reasons.append("precio inválido")

        is_valid = len(reasons) == 0
        return is_valid, "; ".join(reasons)

    validated = raw_df.apply(lambda row: validate_row(row), axis=1)
    valid_mask = [v[0] for v in validated]
    reasons = [v[1] for v in validated]

    df["_reason"] = reasons
    quarantine = df.loc[[not v for v in valid_mask]].copy()
    clean = df.loc[valid_mask].copy()

    if not clean.empty:
        # coercionar tipos para el dataset limpio
        clean["fecha"] = pd.to_datetime(clean["fecha"], errors="coerce").dt.date
        clean["unidades"] = pd.to_numeric(clean["unidades"], errors="coerce")
        clean["precio_unitario"] = clean["precio_unitario"].apply(to_float_money)

        clean = (clean.sort_values("_ingest_ts")
                     .drop_duplicates(subset=["fecha", "id_cliente", "id_producto"], keep="last"))
        clean["importe"] = clean["unidades"] * clean["precio_unitario"]

    # 3) Persistencia: Parquet (fuente de reporte) + SQLite (opcional integrado)
    # Guardar cuarentena con motivo
    if not quarantine.empty:
        qcols = [c for c in quarantine.columns if c in ["fecha", "id_cliente", "id_producto", "unidades", "precio_unitario", "_source_file", "_ingest_ts", "_reason"]]
        quarantine[qcols].to_csv(OUT / "quality" / "ventas_invalidas.csv", index=False)
    else:
        (OUT / "quality" / "ventas_invalidas.csv").write_text("fecha,id_cliente,id_producto,unidades,precio_unitario,_source_file,_ingest_ts,_reason\n", encoding="utf-8")

    PARQUET_FILE = OUT / "parquet" / "clean_ventas.parquet"
    if not clean.empty:
        clean.to_parquet(PARQUET_FILE, index=False)

    # SQLite
    DB = OUT / "ut1.db"
    con = sqlite3.connect(DB)
    # DDL
    con.executescript((ROOT / "sql" / "00_schema.sql").read_text(encoding="utf-8"))
    # RAW: escribir sólo las columnas que existen en el esquema para evitar conflictos
    if not df.empty:
        df_raw = df[["fecha", "id_cliente", "id_producto", "unidades", "precio_unitario", "_ingest_ts", "_source_file"]].copy()
        df_raw["_batch_id"] = "demo"
        df_raw.to_sql("raw_ventas", con, if_exists="append", index=False)

    # CLEAN via UPSERT (usa sql/10_upserts.sql)
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
        clean_rep = pd.DataFrame(columns=["fecha", "id_cliente", "id_producto", "nombre_producto", "unidades", "precio_unitario", "_ingest_ts", "importe"])  # include nombre_producto for shape

    if not clean_rep.empty:
        ingresos = float(clean_rep["importe"].sum())
        trans = int(len(clean_rep))
        ticket = float(ingresos / trans) if trans > 0 else 0.0

        # filtrar por precio máximo
        clean_for_report = clean[clean['precio_unitario'] <= 150]

        # top productos (id + nombre si existe)
        if "nombre_producto" in clean_for_report.columns:
            top = (clean_for_report.groupby(["id_producto", "nombre_producto"], as_index=False)
                       .agg(importe=("importe", "sum"))
                       .sort_values("importe", ascending=False)
                       .head(5))
        else:
            top = (clean_for_report.groupby("id_producto", as_index=False)
                       .agg(importe=("importe", "sum"))
                       .sort_values("importe", ascending=False)
                       .head(5))
        total_imp = top["importe"].sum() or 1.0
        top["pct"] = (100 * top["importe"] / total_imp).round(0).astype(int).astype(str) + "%"

        by_day = (clean_for_report.groupby("fecha", as_index=False)
                          .agg(importe_total=("importe", "sum"),
                               transacciones=("importe", "count"))
                          .sort_values("fecha", ascending=False)
                          .head(20))
        periodo_ini = str(clean_rep["fecha"].min())
        periodo_fin = str(clean_rep["fecha"].max())
        producto_lider = top.iloc[0]["id_producto"] if not top.empty else "—"
    else:
        ingresos = 0.0
        ticket = 0.0
        trans = 0
        top = pd.DataFrame(columns=["id_producto", "importe", "pct"]) if "nombre_producto" not in clean.columns else pd.DataFrame(columns=["id_producto", "nombre_producto", "importe", "pct"]) 
        by_day = pd.DataFrame(columns=["fecha", "importe_total", "transacciones"])
        periodo_ini = "—"
        periodo_fin = "—"
        producto_lider = "—"

    # preparar insights para conclusiones dinámicas
    try:
        if not quarantine.empty and "_reason" in quarantine.columns:
            razones = quarantine["_reason"].dropna().astype(str).str.split(";")
            razones = razones.explode().str.strip()
            top_raz = razones.value_counts().head(3)
            razones_texto = "\n".join([f"  - {r}: {c} filas" for r, c in top_raz.items()])
        else:
            razones_texto = "  - (sin filas en cuarentena)"
    except Exception:
        razones_texto = "  - (no disponible)"

    try:
        count_above_150 = int((clean_rep["precio_unitario"] > 150).sum()) if "precio_unitario" in clean_rep.columns else 0
    except Exception:
        count_above_150 = 0

    conclusiones = (
        "## 7. Conclusiones\n"
        f"- Reponer stock del producto líder ({producto_lider}) acorde a la demanda observada.\n"
        "- Analizar las filas en cuarentena y corregir las fuentes/validaciones; causas principales:\n"
        f"{razones_texto}\n"
        f"- En el dataset limpio había {count_above_150} filas con precio > 150 € (se filtran en el reporte). Revisar la política de precios.\n"
        "- Considerar particionado por fecha y compresión en Parquet para mejorar rendimiento y escalabilidad.\n"
    )

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
        f"{conclusiones}\n"
    )

    (OUT / "reporte.md").write_text(report, encoding="utf-8")
    print("OK · Generado:", OUT / "reporte.md")
    print("OK · Parquet :", PARQUET_FILE if PARQUET_FILE.exists() else "sin datos")
    print("OK · SQLite  :", DB)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ejecutar pipeline de ingesta, limpieza y reporte")
    parser.add_argument("--regen-data", action="store_true", help="Regenerar datos de ejemplo antes de ejecutar")
    parser.add_argument("--force", action="store_true", help="Forzar sobrescritura al regenerar los datos de ejemplo")
    args = parser.parse_args()

    main(regen_data=args.regen_data, regen_force=args.force)


ejecutar = main
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import sqlite3


def main(regen_data: bool = False, regen_force: bool = False):
    """Ejecuta la canalización: ingesta -> limpieza -> persistencia -> reporte.

    Args:
        regen_data: si es True, regenera los datos de ejemplo antes de ejecutar
            (llama al helper generate_sample en get_data)
        regen_force: si es True y regen_data es True, fuerza la sobrescritura del archivo de muestra
    """
    ROOT = Path(__file__).resolve().parents[1]
    DATA = ROOT / "data" / "drops"
    OUT = ROOT / "output"
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "parquet").mkdir(parents=True, exist_ok=True)
    (OUT / "quality").mkdir(parents=True, exist_ok=True)

    if regen_data:
        try:
            from . import get_data
        except Exception:
            import project.ingest.get_data as get_data
        get_data.generate_sample(drop_dir=DATA, force=regen_force)

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
        cols = ["fecha", "id_cliente", "id_producto", "nombre_producto", "unidades", "precio_unitario", "_source_file", "_ingest_ts"]
        raw_df = pd.DataFrame(columns=cols)

    # 2) Limpieza (coerción de tipos + validación por filas + deduplicación)
    def to_float_money(x):
        try:
            return float(str(x).replace(",", "."))
        except Exception:
            return None

    df = raw_df.copy()
    for c in ["fecha", "id_cliente", "id_producto", "nombre_producto", "unidades", "precio_unitario"]:
        if c not in df.columns:
            df[c] = None

    # Realizaremos validación a nivel de fila y mantendremos una columna _reason para la cuarentena
    def validate_row(r):
        reasons = []
        # fecha
        try:
            f = pd.to_datetime(r.get("fecha"), errors="coerce")
            if pd.isna(f):
                reasons.append("fecha inválida o faltante")
        except Exception:
            reasons.append("fecha inválida")

        # id_cliente
        idc = r.get("id_cliente")
        if idc is None or str(idc).strip() == "":
            reasons.append("id_cliente faltante")

        # id_producto
        idp = r.get("id_producto")
        if idp is None or str(idp).strip() == "":
            reasons.append("id_producto faltante")

        # unidades
        try:
            u = float(str(r.get("unidades")))
            if u < 0:
                reasons.append("unidades negativas")
            if u == 0:
                reasons.append("unidades cero")
        except Exception:
            reasons.append("unidades no numéricas")

        # precio
        try:
            p = to_float_money(r.get("precio_unitario"))
            if p is None:
                reasons.append("precio no numérico o faltante")
            elif p < 0:
                reasons.append("precio negativo")
        except Exception:
            reasons.append("precio inválido")

        is_valid = len(reasons) == 0
        return is_valid, "; ".join(reasons)

    # Aplicar validación
    validated = raw_df.apply(lambda row: validate_row(row), axis=1)
    valid_mask = [v[0] for v in validated]
    reasons = [v[1] for v in validated]

    df["_reason"] = reasons
    quarantine = df.loc[[not v for v in valid_mask]].copy()
    clean = df.loc[valid_mask].copy()

    if not clean.empty:
        # coercionar tipos para el dataset limpio
        clean["fecha"] = pd.to_datetime(clean["fecha"], errors="coerce").dt.date
        clean["unidades"] = pd.to_numeric(clean["unidades"], errors="coerce")
        clean["precio_unitario"] = clean["precio_unitario"].apply(to_float_money)

        clean = (clean.sort_values("_ingest_ts")
                     .drop_duplicates(subset=["fecha", "id_cliente", "id_producto"], keep="last"))
        clean["importe"] = clean["unidades"] * clean["precio_unitario"]

    # 3) Persistencia: Parquet (fuente de reporte) + SQLite (opcional integrado)
    # Guardar cuarentena con motivo
    if not quarantine.empty:
        # keep relevant cols and reason
        qcols = [c for c in quarantine.columns if c in ["fecha", "id_cliente", "id_producto", "unidades", "precio_unitario", "_source_file", "_ingest_ts", "_reason"]]
        quarantine[qcols].to_csv(OUT / "quality" / "ventas_invalidas.csv", index=False)
    else:
        # asegurar que exista un fichero vacío con cabecera
        (OUT / "quality" / "ventas_invalidas.csv").write_text("fecha,id_cliente,id_producto,unidades,precio_unitario,_source_file,_ingest_ts,_reason\n", encoding="utf-8")
    PARQUET_FILE = OUT / "parquet" / "clean_ventas.parquet"
    if not clean.empty:
        clean.to_parquet(PARQUET_FILE, index=False)

    # SQLite
    DB = OUT / "ut1.db"
    con = sqlite3.connect(DB)
    # Ejecutar DDL (esquema)
    con.executescript((ROOT / "sql" / "00_schema.sql").read_text(encoding="utf-8"))
    # Insertar RAW
    if not df.empty:
        # Only persist the columns defined in the raw_ventas table (avoid schema mismatch)
        df_raw = df[["fecha", "id_cliente", "id_producto", "unidades", "precio_unitario", "_ingest_ts", "_source_file"]].copy()
        df_raw["_batch_id"] = "demo"  # simplificado
        df_raw.to_sql("raw_ventas", con, if_exists="append", index=False)
    # Actualizar/insertar CLEAN mediante UPSERT
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
    # Crear vistas
    con.executescript((ROOT / "sql" / "20_views.sql").read_text(encoding="utf-8"))
    con.close()

    # 4) Generar reporte leyendo desde PARQUET
    if PARQUET_FILE.exists():
        clean_rep = pd.read_parquet(PARQUET_FILE)
    else:
        clean_rep = pd.DataFrame(columns=["fecha", "id_cliente", "id_producto", "unidades", "precio_unitario", "_ingest_ts", "importe"])

    if not clean_rep.empty:
        ingresos = float(clean_rep["importe"].sum())
        trans = int(len(clean_rep))
        ticket = float(ingresos / trans) if trans > 0 else 0.0

        clean = clean[clean['precio_unitario'] <= 150]  # filtrar filas con precio por encima de 150

        # calcular top productos incluyendo nombre si está disponible
        if "nombre_producto" in clean.columns:
            top = (clean.groupby(["id_producto", "nombre_producto"], as_index=False)
                       .agg(importe=("importe", "sum"))
                       .sort_values("importe", ascending=False)
                       .head(5))  # limitar a los 5 productos principales
        else:
            top = (clean.groupby("id_producto", as_index=False)
                       .agg(importe=("importe", "sum"))
                       .sort_values("importe", ascending=False)
                       .head(5))  # limitar a los 5 productos principales
        total_imp = top["importe"].sum() or 1.0
        top["pct"] = (100 * top["importe"] / total_imp).round(0).astype(int).astype(str) + "%"

        by_day = (clean.groupby("fecha", as_index=False)
                          .agg(importe_total=("importe", "sum"),
                               transacciones=("importe", "count"))
                          .sort_values("fecha", ascending=False)
                          .head(20))  # limitar a 20 filas
        periodo_ini = str(clean_rep["fecha"].min())
        periodo_fin = str(clean_rep["fecha"].max())
        producto_lider = top.iloc[0]["id_producto"] if not top.empty else "—"
    else:
        ingresos = 0.0
        ticket = 0.0
        trans = 0
        top = pd.DataFrame(columns=["id_producto", "importe", "pct"]) if "nombre_producto" not in clean.columns else pd.DataFrame(columns=["id_producto", "nombre_producto", "importe", "pct"]) 
        by_day = pd.DataFrame(columns=["fecha", "importe_total", "transacciones"])
        periodo_ini = "—"
        periodo_fin = "—"
        producto_lider = "—"

    # preparar insights para conclusiones dinámicas
    try:
        # contar causas principales en cuarentena
        if not quarantine.empty and "_reason" in quarantine.columns:
            razones = quarantine["_reason"].dropna().astype(str).str.split(";")
            razones = razones.explode().str.strip()
            top_raz = razones.value_counts().head(3)
            razones_texto = "\n".join([f"  - {r}: {c} filas" for r, c in top_raz.items()])
        else:
            razones_texto = "  - (sin filas en cuarentena)"
    except Exception:
        razones_texto = "  - (no disponible)"

    try:
        # contar cuántas filas tenían precio > 150 en el conjunto limpio (antes de filtrar)
        count_above_150 = int((clean_rep["precio_unitario"] > 150).sum()) if "precio_unitario" in clean_rep.columns else 0
    except Exception:
        count_above_150 = 0

    conclusiones = (
        "## 7. Conclusiones\n"
        f"- Reponer stock del producto líder ({producto_lider}) acorde a la demanda observada.\n"
        "- Analizar las filas en cuarentena y corregir las fuentes/validaciones; causas principales:\n"
        f"{razones_texto}\n"
        f"- En el dataset limpio había {count_above_150} filas con precio > 150 € (se filtran en el reporte). Revisar la política de precios.\n"
        "- Considerar particionado por fecha y compresión en Parquet para mejorar rendimiento y escalabilidad.\n"
    )

<<<<<<< HEAD
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
=======
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
>>>>>>> 6eab657 (feat: traducir get_data y run; añadir nombres de productos y reporte dinámico; actualizar output/reporte.md)
    "## 5. Calidad y cobertura\n"
    f"- Filas bronce: {len(df)} · Plata: {len(clean)} · Cuarentena: {len(quarantine)}\n\n"
        "## 6. Persistencia\n"
        f"- Parquet: {PARQUET_FILE}\n"
        f"- SQLite : {DB} (tablas: raw_ventas, clean_ventas; vista: ventas_diarias)\n\n"
        f"{conclusiones}\n"
    )

    (OUT / "reporte.md").write_text(report, encoding="utf-8")
    print("OK · Generado:", OUT / "reporte.md")
    print("OK · Parquet :", PARQUET_FILE if PARQUET_FILE.exists() else "sin datos")
    print("OK · SQLite  :", DB)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ejecutar pipeline de ingesta, limpieza y reporte")
    parser.add_argument("--regen-data", action="store_true", help="Regenerar datos de ejemplo antes de ejecutar")
    parser.add_argument("--force", action="store_true", help="Forzar sobrescritura al regenerar los datos de ejemplo")
    args = parser.parse_args()

    main(regen_data=args.regen_data, regen_force=args.force)

ejecutar = main

