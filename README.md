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

Sign in with:
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
- Almacenar logs en algun repositorio.

##### Tu solución le ha gustado al equipo y deciden incorporar otros procesos, habrá nuevas personas colaborando contigo, ¿Qué aspectos o herramientas considerarías para escalar, organizar y automatizar tu solución?

- Implementación de prácticas DevOps. Como CI/CD.
- Uso de una buena estructura de branches en el repositorio
- Versionamiento
- Creación de ambientes de desarrollo
- Ambiente distribuido

* El API tiene ciertos periodos de intermitencia, en donde no es posible hacer la llamada GET del archivo .gz