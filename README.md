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
- Airflow distribuido.
- Implementación de Operador que regrese el folder más reciente de data_municipios.
- Mejor manejo de alertas (mandar correos cuando haya algún error en el pipeline).
- Almacenar logs en algun repositorio (actualmente solo se manejan a nivel de código). 
- Mejorar el manejo de excepciones.

##### Tu solución le ha gustado al equipo y deciden incorporar otros procesos, habrá nuevas personas colaborando contigo, ¿Qué aspectos o herramientas considerarías para escalar, organizar y automatizar tu solución?

- Implementación de prácticas DevOps. Como CI/CD.
- Uso de una buena estructura de branches en el repositorio
- Versionamiento
- Creación de ambientes de desarrollo
- Ambiente distribuido

### Airflow DAG 
1. Cada hora debes consumir el último registro devuelto por el servicio de pronóstico por municipio y por hora.
  - create_table
  - extract_pronostico
  - store_pronostico
2. A partir de los datos extraídos en el punto 1, generar una tabla a nivel municipio en la que cada registro contenga el promedio de temperatura y precipitación de las últimas dos horas.
  - get_average_pronostico
  - execute_average_pronostico
3. Hay una carpeta “data_municipios” que contiene datos a nivel municipio organizados por fecha, cada vez que tu proceso se ejecute debe generar una tabla en la cual se crucen los datos más recientes de esta carpeta con los datos generados en el punto 2.
  - get_data_municipios
  - create_data_municipios_table
  - process_data_municipios 
  - merge_data_pronostico_mun
4. Versiona todas tus tablas de acuerdo a la fecha y hora en la que se ejecutó el proceso, en el caso del entregable del punto 3, además, genera una versión “current” que siempre contenga una copia de los datos más recientes.

##### Observaciones adicionales
* El API tiene ciertos periodos de intermitencia, en donde no es posible hacer la llamada GET del archivo .gz