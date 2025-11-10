from pathlib import Path
<<<<<<< HEAD
from datetime import datetime, timedelta
import random

DATA = Path(__file__).resolve().parents[1] / "data" / "drops"
DATA.mkdir(parents=True, exist_ok=True)

# Configuración de datos
clientes = [f"C{str(i).zfill(3)}" for i in range(1, 21)]  # C001 a C020
productos = {
    "P001": 9.99,    # Café premium
    "P002": 15.50,   # Té especial
    "P003": 12.75,   # Chocolate gourmet
    "P004": 7.99,    # Galletas artesanales
    "P005": 22.50,   # Vino tinto reserva
    "P006": 18.25,   # Aceite de oliva virgen
    "P007": 5.99,    # Mermelada casera
    "P008": 8.50,    # Pan artesanal
    "P009": 13.99,   # Queso curado
    "P010": 11.25,   # Embutido ibérico
    "P011": 6.75,    # Frutos secos
    "P012": 4.99,    # Pasta italiana
    "P013": 16.50,   # Miel orgánica
    "P014": 9.25,    # Cereales premium
    "P015": 14.99,   # Café descafeinado
}

# Generar fechas desde hace 1 mes hasta hoy
end_date = datetime.now()
start_date = end_date - timedelta(days=30)
dates = [(start_date + timedelta(days=x)).strftime("%Y-%m-%d") for x in range(31)]

# Generar datos
rows = ["fecha,id_cliente,id_producto,unidades,precio_unitario"]
num_transactions = 60  # Aumentamos a 60 transacciones para tener más variedad de errores

# Lista para almacenar transacciones válidas que usaremos para crear duplicados
valid_transactions = []

for i in range(num_transactions):
    fecha = random.choice(dates)
    cliente = random.choice(clientes)
    producto = random.choice(list(productos.keys()))
    precio = productos[producto]
    unidades = random.randint(1, 10)
    
    # Generamos diferentes tipos de errores
    if i < num_transactions - 45:  # Los primeros 15 registros tendrán errores variados
        error_type = i % 8  # 8 tipos diferentes de errores
        
        if error_type == 0:
            # Valores nulos
            campo_nulo = random.choice(['fecha', 'id_cliente', 'id_producto', 'unidades', 'precio_unitario'])
            if campo_nulo == 'fecha':
                fecha = ""
            elif campo_nulo == 'id_cliente':
                cliente = ""
            elif campo_nulo == 'id_producto':
                producto = ""
            elif campo_nulo == 'unidades':
                unidades = ""
            else:
                precio = ""
                
        elif error_type == 1:
            # Fechas mal formateadas
            fecha = "2025/13/45"  # Fecha inválida
            
        elif error_type == 2:
            # IDs mal formateados
            cliente = "Cliente123"  # No sigue el formato CXXX
            
        elif error_type == 3:
            # Producto inexistente
            producto = "P999"
            
        elif error_type == 4:
            # Unidades como texto
            unidades = "diez"
            
        elif error_type == 5:
            # Precio como texto
            precio = "quince con cincuenta"
            
        elif error_type == 6:
            # Valores negativos
            if random.random() < 0.5:
                unidades = -random.randint(1, 5)
            else:
                precio = -random.randint(1, 100)
                
        elif error_type == 7:
            # Valores extremadamente grandes
            if random.random() < 0.5:
                unidades = "999999999"
            else:
                precio = "999999999.99"
    
    else:
        # Guardamos algunas transacciones válidas para crear duplicados
        valid_transactions.append(f"{fecha},{cliente},{producto},{unidades},{precio}")
    
    row = f"{fecha},{cliente},{producto},{unidades},{precio}"
    rows.append(row)

# Añadir algunos duplicados exactos
for _ in range(3):
    if valid_transactions:
        rows.append(random.choice(valid_transactions))

# Añadir algunos duplicados con timestamp diferente (misma fecha, cliente, producto)
for _ in range(2):
    if valid_transactions:
        original = random.choice(valid_transactions)
        fecha, cliente, producto, _, _ = original.split(',')
        # Mismo día, cliente y producto pero diferentes unidades/precio
        rows.append(f"{fecha},{cliente},{producto},{random.randint(1,10)},{random.randint(1,100)}.99")

csv = "\n".join(rows)
=======
DATA = Path(__file__).resolve().parents[1] / "data" / "drops"
DATA.mkdir(parents=True, exist_ok=True)

csv = """fecha,id_cliente,id_producto,unidades,precio_unitario
2025-01-03,C001,P10,2,12.50
2025-01-04,C002,P10,1,12.50
2025-01-04,C001,P20,3,8.00
2025-01-05,C003,P20,1,8.00
2025-01-05,C003,P20,-1,8.00
2025-01-06,C004,P99,2,doce
"""
>>>>>>> 7bdfc871baa9bcef1032f7aef3e635b35571e00b
(DATA / "ventas_ejemplo.csv").write_text(csv, encoding="utf-8")
print("Generado:", DATA / "ventas_ejemplo.csv")
