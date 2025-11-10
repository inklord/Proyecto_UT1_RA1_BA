from pathlib import Path
from typing import Optional
import random
from datetime import date, timedelta


def generar_muestra(directorio_drops: Optional[Path] = None, forzar: bool = False, n_filas: int = 10000, n_invalidas: Optional[int] = None) -> Path:
	"""Crear un archivo CSV de ejemplo en el directorio de 'drops'.

	Args:
		directorio_drops: Ruta opcional al directorio de drops. Si no se proporciona,
			se usa la carpeta por defecto project/data/drops.
		forzar: Si es True, sobrescribe el archivo de ejemplo existente. Si es False,
			no sobrescribe cuando ya existe.

	Returns:
		Path: ruta al archivo generado (o existente).
	"""
	base = Path(__file__).resolve().parents[1]
	DATA = (directorio_drops or base / "data" / "drops")
	DATA.mkdir(parents=True, exist_ok=True)

	# generar filas sintéticas
	fecha_inicio = date(2025, 1, 1)
	clientes = [f"C{str(i).zfill(3)}" for i in range(1, 501)]
	productos = [f"P{str(i).zfill(3)}" for i in range(1, 201)]

	# Generar nombres propios para los productos (200)
	_adjetivos = [
		"Básico", "Premium", "Clásico", "Moderno", "Ligero", "Cómodo", "Compacto", "Elegante",
		"Resistente", "Económico", "Plus", "Limitado", "Sport", "Casual", "Formal",
		"Versátil", "Colorido", "Minimal", "Rústico", "Vintage"
	]
	_sustantivos = [
		"Camiseta", "Pantalón", "Zapato", "Sombrero", "Bolso", "Chaqueta", "Calcetín", "Bufanda",
		"Gorra", "Sudadera", "Vestido", "Falda", "Blusa", "Corbata", "Cinturón", "Guante",
		"Botín", "Sandalia", "Abrigo", "Chaleco"
	]
	producto_nombres = []
	for idx, pid in enumerate(productos, start=1):
		adj = _adjetivos[idx % len(_adjetivos)]
		sus = _sustantivos[idx % len(_sustantivos)]
		producto_nombres.append(f"{sus} {adj} #{str(idx).zfill(3)}")

	producto_a_nombre = dict(zip(productos, producto_nombres))

	random.seed(42)
	lineas = ["fecha,id_cliente,id_producto,nombre_producto,unidades,precio_unitario"]

	# determinar el número de filas inválidas: por defecto = 7% del total
	if n_invalidas is None:
		n_invalidas = max(0, int(round(n_filas * 0.07)))

	# generar primero las filas válidas
	filas_validas = max(0, n_filas - n_invalidas)
	for i in range(filas_validas):
		d = fecha_inicio + timedelta(days=(i % 365))
		cliente = random.choice(clientes)
		producto = random.choice(productos)
		nombre_producto = producto_a_nombre.get(producto, "")
		unidades = random.randint(1, 10)
		precio = round(random.uniform(1.0, 150.0), 2)  # tope de precio ajustado a 150
		lineas.append(f"{d.isoformat()},{cliente},{producto},{nombre_producto},{unidades},{precio:.2f}")

	# inyectar varias filas inválidas para probar la cuarentena
	# tipos: unidades negativas, precio no numérico, cliente faltante, fecha inválida, unidades cero, producto vacío
	casos_invalidos = [
		# (fecha, cliente, id_producto, nombre_producto, unidades, precio)
		(fecha_inicio.isoformat(), "C001", "P001", producto_a_nombre.get("P001", ""), -1, "10.00"),           # unidades negativas
		("2025-13-01", "C002", "P002", producto_a_nombre.get("P002", ""), 2, "20.00"),                    # fecha inválida
		(fecha_inicio.isoformat(), "", "P003", producto_a_nombre.get("P003", ""), 3, "30.00"),               # cliente faltante
		(fecha_inicio.isoformat(), "C004", "", "", 1, "40.00"),                                             # producto faltante
		(fecha_inicio.isoformat(), "C005", "P005", producto_a_nombre.get("P005", ""), 0, "50.00"),           # unidades cero
		(fecha_inicio.isoformat(), "C006", "P006", producto_a_nombre.get("P006", ""), 2, "doce"),            # precio no numérico
		(fecha_inicio.isoformat(), "C007", "P007", producto_a_nombre.get("P007", ""), "tres", "60.00"),    # unidades no numéricas
		(fecha_inicio.isoformat(), None, "P008", producto_a_nombre.get("P008", ""), 1, "70.00"),               # cliente None
		(fecha_inicio.isoformat(), "C009", "P009", producto_a_nombre.get("P009", ""), 1, ""),                # precio vacío
		(fecha_inicio.isoformat(), "C010", "P010", producto_a_nombre.get("P010", ""), -100, "1000.00"),      # unidades muy negativas
	]

	# repetir casos inválidos hasta alcanzar n_invalidas si hace falta
	for i in range(n_invalidas):
		caso = casos_invalidos[i % len(casos_invalidos)]
		fecha, cli, prod, prod_nombre, unidades, precio = caso
		# normalizar None a cadena vacía para un CSV realista
		cli = "" if cli is None else cli
		prod = "" if prod is None else prod
		prod_nombre = "" if prod_nombre is None else prod_nombre
		precio = "" if precio is None else precio
		lineas.append(f"{fecha},{cli},{prod},{prod_nombre},{unidades},{precio}")

	csv = "\n".join(lineas) + "\n"

	target = DATA / "ventas_ejemplo.csv"
	if target.exists() and not forzar:
		return target

	target.write_text(csv, encoding="utf-8")
	return target



def generate_sample(drop_dir: Optional[Path] = None, force: bool = False, n_rows: int = 10000, n_invalid: Optional[int] = None) -> Path:
	"""Wrapper compatible con la firma original en inglés.

	Parámetros compatibles:
		drop_dir, force, n_rows, n_invalid
	Se mapean a: directorio_drops, forzar, n_filas, n_invalidas
	"""
	return generar_muestra(directorio_drops=drop_dir, forzar=force, n_filas=n_rows, n_invalidas=n_invalid)


if __name__ == "__main__":
	import argparse
	parser = argparse.ArgumentParser(description="Generar CSV de ejemplo en data/drops")
	parser.add_argument("--force", action="store_true", dest="forzar", help="Sobrescribir si existe")
	parser.add_argument("--rows", "-n", type=int, default=10000, dest="rows", help="Número de filas a generar (sin incluir la cabecera)")
	parser.add_argument("--invalid", "-i", type=int, default=None, dest="invalid", help="Número de filas inválidas a inyectar (si se omite, 7% del total)")
	parser.add_argument("--no-run", dest="run", action="store_false", help="No ejecutar pipeline después de generar los datos")
	parser.set_defaults(run=True)
	args = parser.parse_args()
	path = generar_muestra(directorio_drops=None, forzar=args.forzar, n_filas=args.rows, n_invalidas=args.invalid)
	print("Generado:", path)
