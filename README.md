# Data Engineer Challenge

### Requirements and Installation

#### Docker Compose Cluster

The repository contains 4 shell scripts that will set up a docker-compose 
cluster in order to let a Spark development for the Feature Engineering 
local development. The functions of each shell can be described next:

* start.sh: Docker build of all the containers and set them up and ready to use.
* stop.sh: Stops all the containers without delete any of them
* restart.sh:Stops all the containers and runs them all again
* reset.sh: Deletes all the containers in the machine for a clean start

#### Airflow Connections

##### Sign in Airflow with:
- User: airflow
- Password: airflow

The airflow scheduler needs to add some fields to the 
postgres and the api connections, this has to be done manually due issues
with the airflow CLI.

##### postgres connection
- Conn Type: Postgres
- Password: airflow

##### pronostico_api connection
- Conn Type: HTTP

After setting up the conections, activate the DAG and
wait for the process to complete.

## Preguntas adicionales

##### ¿Qué mejoras propondrías a tu solución para siguientes versiones?

- Uso de alguna nube GCP o AWS.
- Documentar código.
- Airflow distribuido.
- Implementación de Operador que regrese el folder más reciente de data_municipios.
- Mejor manejo de alertas (mandar correos cuando haya algún error en el pipeline).
- Almacenar logs en algun repositorio (actualmente solo se manejan a nivel de código). 
- Mejorar el manejo de excepciones.
- Definir zona horaria para ejecución del proceso.
- El API tiene ciertos periodos de intermitencia, o algunas ejecuciones donde no era posible descomprimir el archivo. Sería conveniente revisar cómo manejar este error.

##### Tu solución le ha gustado al equipo y deciden incorporar otros procesos, habrá nuevas personas colaborando contigo, ¿Qué aspectos o herramientas considerarías para escalar, organizar y automatizar tu solución?

- Implementación de prácticas DevOps. Como CI/CD.
- Uso de una buena estructura de branches en el repositorio
- Versionamiento
- Creación de ambientes de desarrollo, pruebas, etc.
- Ambiente distribuido

### Airflow DAG 
1. Cada hora debes consumir el último registro devuelto por el servicio de pronóstico por municipio y por hora.
  - create_table: se crea la tabla pronosticoxmunicipios en postgresql
  - extract_pronostico: se hace la llamada GET al api, el cual devuelve un archivo .gz que se descomprime.
  - store_pronostico: el json obtenido se inserta en la tabla pronosticoxmunicipios
2. A partir de los datos extraídos en el punto 1, generar una tabla a nivel municipio en la que cada registro contenga el promedio de temperatura y precipitación de las últimas dos horas.
  - get_average_pronostico: se obtiene la query correspondiente que calcula el promedio de la temperatura y precipación , de las dos tablas mas recientes.
  - execute_average_pronostico: se ejecuta la query del paso anterior.
3. Hay una carpeta “data_municipios” que contiene datos a nivel municipio organizados por fecha, cada vez que tu proceso se ejecute debe generar una tabla en la cual se crucen los datos más recientes de esta carpeta con los datos generados en el punto 2.
  - get_data_municipios (Dummy): el objetivo de este paso es obtener el path de la carpeta con los datos más recientes de data_municipios.
  - create_data_municipios_table: se crea la tabla data_municipios en postgres
  - process_data_municipios: a partir del csv de la carpeta data_municipios se llena la tabla creada en el paso anterior. 
  - merge_data_pronostico_mun: se hace el cruce de las tablas y se actualiza la tabla "current"
4. Versiona todas tus tablas de acuerdo a la fecha y hora en la que se ejecutó el proceso, en el caso del entregable del punto 3, además, genera una versión “current” que siempre contenga una copia de los datos más recientes.