F1 ETL & Analytics API (Django + PostgreSQL + Pandas)

Este proyecto implementa un pipeline completo Extract → Transform → Load (ETL) para Fórmula 1, consumiendo datos de la API pública de Jolpi/Ergast, procesándolos con pandas y exponiéndolos mediante una API REST construida con Django REST Framework.

El objetivo es servir como proyecto demostrativo para portfolio: integra manejo de datos, modelado relacional, automatización y diseño de APIs analíticas.

🚀 Tecnologías principales

Python 3.12

Django 6 + Django REST Framework

PostgreSQL

Pandas

Pydantic

Programación ETL modular

API pública: Jolpi/Ergast

📦 Instalación y configuración
1. Clonar el repositorio
git clone https://github.com/tuusuario/f1-etl-api.git
cd f1-etl-api

2. Crear entorno virtual
python -m venv venv
source venv/bin/activate     # Linux/Mac
venv\Scripts\activate        # Windows

3. Instalar dependencias
pip install -r requirements.txt

4. Crear archivo .env

Crear .env en la raíz del proyecto y completar con tus credenciales locales:

SECRET_KEY=REPLACE_ME
DB_NAME=f1_database
DB_USER=postgres
DB_PASSWORD=REPLACE_ME
DB_HOST=localhost
DB_PORT=5432
ENV=development


El proyecto incluye .gitignore para evitar subir el .env real.

5. Crear base de datos en PostgreSQL
CREATE DATABASE f1_database;

6. Aplicar migraciones
python manage.py migrate

🏎️ Ejecutar el ETL

El proyecto incluye un comando personalizado de Django:

Cargar una temporada específica
python manage.py run_etl --mode season --seasons 2023

Cargar la temporada más reciente
python manage.py run_etl --mode incremental

Cargar N temporadas
python manage.py run_etl --mode season --seasons 2022 2023 2024


Los datos se guardan en PostgreSQL listos para consulta.

🌐 Iniciar la API
python manage.py runserver


La API estará disponible en:

http://127.0.0.1:8000/api/v1/


Endpoints principales:

Endpoint	Descripción
/api/v1/drivers/	Pilotos
/api/v1/constructors/	Equipos
/api/v1/circuits/	Circuitos
/api/v1/races/	Carreras
/api/v1/results/	Resultados
/api/v1/qualifying/	Clasificación
/api/v1/metrics/drivers/	Métricas por piloto
/api/v1/metrics/constructors/	Métricas por constructor
/api/v1/standings/drivers/?season=2024	Posiciones finales
/api/v1/analytics/compare/drivers/	Comparación analítica
📊 Diagrama del pipeline (simplificado)
          +-------------+       +---------------+      +--------------+
          |  EXTRACT    | --->  |  TRANSFORM    | ---> |     LOAD     |
          +-------------+       +---------------+      +--------------+
                 |                       |                      |
                 v                       v                      v
         API Jolpi/Ergast         Limpieza, normalización   PostgreSQL
                                 métricas con pandas        (modelo relacional)
                                                              |
                                                              v
                                                         Django REST API

🎯 Habilidades demostradas

Integración de APIs externas (Jolpi/Ergast)

Diseño de pipelines ETL profesionales

Limpieza y modelado de datos con pandas

ORM avanzado con Django + PostgreSQL

Construcción de API REST modular y escalable

Manejo de .env y buenas prácticas de seguridad

Automatización y logging estructurado

Diseño orientado a portfolio

✔️ Listo para ejecutar localmente

Con estas instrucciones cualquier persona puede clonar, configurar y correr el proyecto sin dificultades.