from pathlib import Path
from datetime import datetime, timezone
import re
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

    # límites para validaciones adicionales
    MAX_UNIDADES = 100
    MAX_PRECIO = 1000.0
    MIN_FECHA = pd.to_datetime("2025-01-01", errors="coerce").date()
    MAX_FECHA = datetime.now(timezone.utc).date()

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
        else:
            df = pd.read_json(f, lines=True, dtype=str)
        df["_source_file"] = f.name
        df["_ingest_ts"] = datetime.now(timezone.utc).isoformat()
        raw.append(df)

    raw_df = pd.concat(raw, ignore_index=True) if raw else pd.DataFrame(
        columns=["fecha", "id_cliente", "id_producto", "nombre_producto", "unidades", "precio_unitario", "_source_file", "_ingest_ts"]
    )

    # helper to parse money fields
    def to_float_money(x):
        try:
            return float(str(x).replace(",", "."))
        except Exception:
            return None

    df = raw_df.copy()
    for c in ["fecha", "id_cliente", "id_producto", "nombre_producto", "unidades", "precio_unitario"]:
        if c not in df.columns:
            df[c] = None

    # row-level validation with reasons
    def validate_row(r):
        reasons = []
        # fecha
        try:
            f = pd.to_datetime(r.get("fecha"), errors="coerce")
            if pd.isna(f):
                reasons.append("fecha inválida o faltante")
            else:
                f_date = f.date()
                if f_date < MIN_FECHA or f_date > MAX_FECHA:
                    reasons.append("fecha fuera de rango")
        except Exception:
            reasons.append("fecha inválida")

        # id_cliente
        idc = r.get("id_cliente")
        if idc is None or str(idc).strip() == "":
            reasons.append("id_cliente faltante")
        else:
            if not re.match(r"^C\d{3}$", str(idc).strip()):
                reasons.append("id_cliente formato inválido")

        # id_producto
        idp = r.get("id_producto")
        if idp is None or str(idp).strip() == "":
            reasons.append("id_producto faltante")
        else:
            if not re.match(r"^P\d{3}$", str(idp).strip()):
                reasons.append("id_producto formato inválido")

        # unidades
        try:
            u = float(str(r.get("unidades")))
            if u < 0:
                reasons.append("unidades negativas")
            if u == 0:
                reasons.append("unidades cero")
            if u > MAX_UNIDADES:
                reasons.append("unidades fuera de rango")
        except Exception:
            reasons.append("unidades no numéricas")

        # precio
        try:
            p = to_float_money(r.get("precio_unitario"))
            if p is None:
                reasons.append("precio no numérico o faltante")
            else:
                if p < 0:
                    reasons.append("precio negativo")
                if p > MAX_PRECIO:
                    reasons.append("precio fuera de rango")
        except Exception:
            reasons.append("precio inválido")

        return (len(reasons) == 0), "; ".join(reasons)

    validated = raw_df.apply(lambda row: validate_row(row), axis=1)
    valid_mask = [v[0] for v in validated]
    reasons = [v[1] for v in validated]

    df["_reason"] = reasons
    quarantine = df.loc[[not v for v in valid_mask]].copy()
    clean = df.loc[valid_mask].copy()

    if not clean.empty:
        clean["fecha"] = pd.to_datetime(clean["fecha"], errors="coerce").dt.date
        clean["unidades"] = pd.to_numeric(clean["unidades"], errors="coerce")
        clean["precio_unitario"] = clean["precio_unitario"].apply(to_float_money)

        clean = clean.sort_values("_ingest_ts").drop_duplicates(subset=["fecha", "id_cliente", "id_producto"], keep="last")
        clean["importe"] = clean["unidades"] * clean["precio_unitario"]

    # persist quarantine
    qcols = [c for c in quarantine.columns if c in ["fecha", "id_cliente", "id_producto", "unidades", "precio_unitario", "_source_file", "_ingest_ts", "_reason"]]
    if not quarantine.empty:
        quarantine[qcols].to_csv(OUT / "quality" / "ventas_invalidas.csv", index=False)
    else:
        (OUT / "quality" / "ventas_invalidas.csv").write_text("fecha,id_cliente,id_producto,unidades,precio_unitario,_source_file,_ingest_ts,_reason\n", encoding="utf-8")

    PARQUET_FILE = OUT / "parquet" / "clean_ventas.parquet"
    if not clean.empty:
        clean.to_parquet(PARQUET_FILE, index=False)

    # SQLite persistence
    DB = OUT / "ut1.db"
    con = sqlite3.connect(DB)
    con.executescript((ROOT / "sql" / "00_schema.sql").read_text(encoding="utf-8"))

    if not df.empty:
        df_raw = df[["fecha", "id_cliente", "id_producto", "unidades", "precio_unitario", "_ingest_ts", "_source_file"]].copy()
        df_raw["_batch_id"] = "demo"
        df_raw.to_sql("raw_ventas", con, if_exists="append", index=False)

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

    con.executescript((ROOT / "sql" / "20_views.sql").read_text(encoding="utf-8"))
    con.close()

    # report (read from parquet)
    if PARQUET_FILE.exists():
        clean_rep = pd.read_parquet(PARQUET_FILE)
    else:
        clean_rep = pd.DataFrame(columns=["fecha", "id_cliente", "id_producto", "nombre_producto", "unidades", "precio_unitario", "_ingest_ts", "importe"])

    if not clean_rep.empty:
        ingresos = float(clean_rep["importe"].sum())
        trans = int(len(clean_rep))
        ticket = float(ingresos / trans) if trans > 0 else 0.0

        # use the cleaned dataset for reporting calculations (apply price filter)
        clean_for_report = clean[clean["precio_unitario"] <= 150] if not clean.empty else pd.DataFrame(columns=clean.columns)

        # top products (include name if present)
        if "nombre_producto" in clean_for_report.columns:
            top = (
                clean_for_report.groupby(["id_producto", "nombre_producto"], as_index=False)
                .agg(importe=("importe", "sum"))
                .sort_values("importe", ascending=False)
                .head(5)
            )
            producto_lider = (f"{top.iloc[0]['id_producto']} ({top.iloc[0]['nombre_producto']})" if not top.empty else "—")
        else:
            top = (
                clean_for_report.groupby("id_producto", as_index=False)
                .agg(importe=("importe", "sum"))
                .sort_values("importe", ascending=False)
                .head(5)
            )
            producto_lider = top.iloc[0]["id_producto"] if not top.empty else "—"

        total_imp = top["importe"].sum() or 1.0
        top["pct"] = (100 * top["importe"] / total_imp).round(0).astype(int).astype(str) + "%"

        by_day = (
            clean_for_report.groupby("fecha", as_index=False)
            .agg(importe_total=("importe", "sum"), transacciones=("importe", "count"))
            .sort_values("fecha", ascending=False)
            .head(20)
        )
        periodo_ini = str(clean_rep["fecha"].min())
        periodo_fin = str(clean_rep["fecha"].max())
    else:
        ingresos = 0.0
        ticket = 0.0
        trans = 0
        top = pd.DataFrame(columns=["id_producto", "importe", "pct"])
        by_day = pd.DataFrame(columns=["fecha", "importe_total", "transacciones"])
        periodo_ini = "—"
        periodo_fin = "—"
        producto_lider = "—"

    # prepare insights for conclusions
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

