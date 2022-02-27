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

# BATH LAYER
--------------
Teniendo en cuenta que la mayoría de métodos que se implementarán en esta y que son compartidos con el Stream Layer, nos ocuparemos de explicar lo que sea diferente, y específico para este layer.

En esta capa ya no se consumen los datos en streaming, aquí se leen datos  del strorage que se han generarán de json a en formato parquet, y guardados en un bucket o archivos temporales, aquí la lectura ya no está hecha en streamming

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
#  - Suma de los bytes de cada usuario

se usó  la función suma, con un waithmark para los mensajes atrasados, agrupamos por id de usuario, con withcolumn generamos la columna tipo literal que será el nombre de la función Usuario_bytes_total , aquí decidí usar el name para hacer uso de la tabla de la información de los usuarios, para que apareciera el nombre en lugar de un numero 
  - 
![image](https://user-images.githubusercontent.com/86910759/155903230-3eb444e1-bda2-4c78-b327-6a8db5bd9345.png)

 
# Para los bytes totales por antena:
seleccionamos los datos que necesitamos para obtener el total, que son: anntena_id, timestamp y los bytes, los agrupamos por antenna_id que es el valor que requerimos , hacemos una agregación con la suma de los bytes transmitidos por la antena y  hacemos una ventana con timestamp con una duración de 90 segundos para obtenr resultados ,seleccionoamos los datos que llevaremos a la tabla de byts, y obtendremos la siguiente tabla
 
 ![image](https://user-images.githubusercontent.com/86910759/155903221-ee2190e4-a3d0-4d17-858b-3789e4544744.png)
 
# Para el total de bytes de la aplicación:

![image](https://user-images.githubusercontent.com/86910759/155903265-122c7338-da95-4e7d-a157-39e7d4364960.png)

 
el siguiente paso es guardar en postgres, lo importante es que la estructura esté adaptada a la tabla bytes donde vamos a guardar los datos totales de los bytes, para ello debemos hacer un futuro, para hacer pequeñas escrituras por cada dataset que va llegando,para eso hacemos un foreachBatch, escribimos en stream, para que pueda guardar debemos conectarnos al postgres con jdbc , estableciendo el modo Append para  agregar los datos a la tabla  y no la sobreescriba 


para guardar por año mes día y hora sería otro futuro.  

Debemos establecer 3 futuros para cada una de las métricas de bytes de esta forma se guardarán en el postgres y un 4 futuro es para que se escriban los datos en un fichero temporal

Debemos hacer un Await de resultado de los futuros con la lista de futuros y el tiempo.inf

la comprobación del futuro de Antena_bytes_total y podemos ver que están cargando todos los datos de usuario, de antena y de aplicación

![image](https://user-images.githubusercontent.com/86910759/155903296-d8593d36-310e-4215-af6d-e667198d82af.png)

![image](https://user-images.githubusercontent.com/86910759/155903305-e48b3bc3-1c6f-4edf-acdb-e4dddeea058c.png)


 


