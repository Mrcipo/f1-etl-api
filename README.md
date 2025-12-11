# F1 ETL & Analytics API (Django + PostgreSQL + Pandas)

Este proyecto implementa un pipeline completo **Extract → Transform → Load (ETL)** para Fórmula 1, consumiendo datos de la API pública de Jolpi/Ergast, procesándolos con pandas y exponiéndolos mediante una API REST construida con Django REST Framework.

El objetivo es servir como proyecto demostrativo para portfolio: integra manejo de datos, modelado relacional, automatización y diseño de APIs analíticas.

---

## 🚀 Tecnologías principales

- **Python 3.12**
- **Django 6** + **Django REST Framework**
- **PostgreSQL**
- **Pandas**
- **Pydantic**
- Programación ETL modular
- API pública: **Jolpi/Ergast**

---

## 📦 Instalación y configuración

### 1. Clonar el repositorio
```bash
git clone https://github.com/tuusuario/f1-etl-api.git
cd f1-etl-api
```

### 2. Crear entorno virtual
```bash
python -m venv venv
source venv/bin/activate     # Linux/Mac
venv\Scripts\activate        # Windows
```

### 3. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 4. Crear archivo `.env`

Crear `.env` en la raíz del proyecto y completar con tus credenciales locales:
```env
SECRET_KEY=REPLACE_ME
DB_NAME=f1_database
DB_USER=postgres
DB_PASSWORD=REPLACE_ME
DB_HOST=localhost
DB_PORT=5432
ENV=development
```

> **Nota:** El proyecto incluye `.gitignore` para evitar subir el `.env` real.

### 5. Crear base de datos en PostgreSQL
```sql
CREATE DATABASE f1_database;
```

### 6. Aplicar migraciones
```bash
python manage.py migrate
```

---

## 🏎️ Ejecutar el ETL

El proyecto incluye un comando personalizado de Django:

### Cargar una temporada específica
```bash
python manage.py run_etl --mode season --seasons 2023
```

### Cargar la temporada más reciente
```bash
python manage.py run_etl --mode incremental
```

### Cargar múltiples temporadas
```bash
python manage.py run_etl --mode season --seasons 2022 2023 2024
```

> Los datos se guardan en PostgreSQL listos para consulta.

---

## 🌐 Iniciar la API
```bash
python manage.py runserver
```

La API estará disponible en:

**http://127.0.0.1:8000/api/v1/**

### Endpoints principales

| Endpoint                                    | Descripción                       |
|---------------------------------------------|-----------------------------------|
| `/api/v1/drivers/`                          | Pilotos                           |
| `/api/v1/constructors/`                     | Equipos                           |
| `/api/v1/circuits/`                         | Circuitos                         |
| `/api/v1/races/`                            | Carreras                          |
| `/api/v1/results/`                          | Resultados                        |
| `/api/v1/qualifying/`                       | Clasificación                     |
| `/api/v1/metrics/drivers/`                  | Métricas por piloto               |
| `/api/v1/metrics/constructors/`             | Métricas por constructor          |
| `/api/v1/standings/drivers/?season=2024`    | Posiciones finales                |
| `/api/v1/analytics/compare/drivers/`        | Comparación analítica             |

### Documentación interactiva

- **Swagger UI:** http://127.0.0.1:8000/api/docs/
- **ReDoc:** http://127.0.0.1:8000/api/redoc/

---

## 📊 Diagrama del pipeline (simplificado)
```
+-------------+       +---------------+      +--------------+
|  EXTRACT    | --->  |  TRANSFORM    | ---> |     LOAD     |
+-------------+       +---------------+      +--------------+
       |                      |                      |
       v                      v                      v
API Jolpi/Ergast     Limpieza, normalización    PostgreSQL
                    métricas con pandas      (modelo relacional)
                                                     |
                                                     v
                                              Django REST API
```

---

## 🎯 Habilidades demostradas

✅ Integración de APIs externas (Jolpi/Ergast)  
✅ Diseño de pipelines ETL profesionales  
✅ Limpieza y modelado de datos con pandas  
✅ ORM avanzado con Django + PostgreSQL  
✅ Construcción de API REST modular y escalable  
✅ Manejo de `.env` y buenas prácticas de seguridad  
✅ Automatización y logging estructurado  
✅ Diseño orientado a portfolio  

---

## 📂 Estructura del proyecto
```
f1-etl-api/
├── core/                      # App principal de Django
│   ├── models.py              # Modelos de datos (Driver, Race, Result, etc.)
│   ├── serializers.py         # Serializers de DRF
│   ├── views.py               # ViewSets y vistas personalizadas
│   ├── urls.py                # Configuración de rutas
│   └── management/
│       └── commands/
│           └── run_etl.py     # Comando personalizado para ETL
├── etl/                       # Módulo ETL
│   ├── config.py              # Configuración del ETL
│   ├── extract/               # Capa de extracción
│   ├── transform/             # Capa de transformación
│   ├── load/                  # Capa de carga
│   ├── orchestrator.py        # Orquestador del pipeline
│   └── run_etl.py             # Script standalone (opcional)
├── f1api/                     # Configuración del proyecto Django
│   ├── settings.py            # Settings con DRF configurado
│   ├── urls.py                # URLs principales
│   └── wsgi.py
├── requirements.txt           # Dependencias del proyecto
├── .env.example               # Ejemplo de variables de entorno
├── .gitignore
└── README.md
```

---

## 🛠️ Comandos útiles

### Crear superusuario (para Django Admin)
```bash
python manage.py createsuperuser
```

### Ver logs del ETL

Los logs se guardan en `f1api.log` y también se muestran en consola.

### Acceder al Django Admin

http://127.0.0.1:8000/admin/

---

## 🔄 Automatización con Cron (Producción)

Para ejecutar el ETL automáticamente en un servidor:
```bash
# ETL incremental diario a las 02:00 UTC
0 2 * * * /ruta/a/venv/bin/python /ruta/a/proyecto/manage.py run_etl --mode incremental >> /var/log/f1_etl.log 2>&1

# Backfill mensual el día 1 a las 04:00 UTC
0 4 1 * * /ruta/a/venv/bin/python /ruta/a/proyecto/manage.py run_etl --mode backfill >> /var/log/f1_etl_backfill.log 2>&1
```

---

## 🧪 Testing (Futuro)
```bash
python manage.py test
```

---

## 📝 Licencia

Este proyecto es de código abierto y está disponible bajo la licencia MIT.

---

## 👤 Autor

**Tu Nombre**  
📧 maty.riffo@gmail.com  
🔗 [GitHub](https://github.com/Mrcipo)

---

## ⭐ ¿Te gustó el proyecto?

Si este proyecto te resultó útil, no olvides darle una estrella ⭐ en GitHub.

---

**✔️ Listo para ejecutar localmente**

Con estas instrucciones cualquier persona puede clonar, configurar y correr el proyecto sin dificultades.
