# BigDataProcessng-
ProyectoBigData Arquitectura Lambda con uso de Spark, Postgress, (Batch Layer and Stream Layer)
# DATA FLOW
Para  la captura de datos es necesario realizar el siguiente procedimiento:
1. Haber configurado el server de Kafka con la ip publica que recibirá la información.
2. Levantar el Zookeeper en segundo plano (Daemon)
3. Levantamos el brocker de Kafka en segundo plano (Daemon)
4. creamos los topics para kafka que se consumiran, también el número de particiones y el factor de replicación
5. Enviamos y recibimos recursos al topic , en una shell generamos el productor que recibirá los datos en stream y los enviará al consumidor
6. Tendrémos generada un Postgres donde se guardará la información procesada y recibida de stream layer y se guardarán las tablas de las consultas realizadas en el batch.

# STREAM LAYER 
 --------------
 Debemos tener previamente una interfaz que contenga los métodos que generaran los jobs.
 1. Generamos con la conexión con ayuda del driver y el jdbc con la postgresql, en mi caso no use google cloud, y la generé en  un Docker, debemos crear las tablas que almacenaran los datos 
 2. creamos una clase objeto donde se implementarán los métodos de la interfaz. En esta clase 
 3. Inciamos la sessión con Spark y nos subscribimos al topic generado en Kafka.
 4. leemos los datos recibidos del Kafka y nos suscribimos al topic.
 5. En vista que los datos serán almacenados en un Postgress, los datos deben estar procesados y estructurados, para ello, debemos generar una estructura con el uso de StructType, generamos cada una de las columnas con StructField("nombre", "tipodedato", nullable =false) , con la estructura generamos el dataframe se parcean los datos y se da la estructura.
 6. con el jdbc se genera la conexion con  la base de datos, tener en cuenta la contraseña y usuario que son necesarios
 7. De ser necesario la tabla generada con la información puede ser enriquesida con otras tablas, para eso se debe realizar un join de las tablas identificando cada una y borrando las columnas iguales, en este caso no era completamente necesario pero decidí hacerlo para mostrar la información que se solicitaba agrupada de forma diferente en cada consulta 
 8. en este proyecto han solicitado :
  - Suma de los bytes de cada usuario, se usó  la función suma, con un waithmark para los mensajes atrasados, agrupamos por id de usuario, con withcolumn generamos la columna tipo literal que será el nombre de la función Usuario_bytes_total , aquí decidí usar el name para hacer uso de la tabla de la información de los usuarios, para que apareciera el nombre en lugar de un numero 
  - 
 ![image](https://user-images.githubusercontent.com/86910759/155903164-6ca75f85-edd0-4cbe-9d68-8d9d638b6a75.png)

