# BigDataProcessng-
ProyectoBigData Arquitectura Lambda con uso de Spark, Postgress, (Batch Layer and Stream Layer)
# DATA FLOW
Para  la captura de datos es necesario realizar el siguiente procedimiento:
1. Haber configurado el server de Kafka con la ip publica que recibirá la información.
2. Levantar el Zookeeper en segundo plano (Daemon)
3. Levantamos el brocker de Kafka en segundo plano (Daemon)
4. creamos los topics para kafka que se consumiran, también el número de particiones y el factor de replicación
5. Enviamos y recibimos recursos al topic , en una shell generamos el productor que recibirá los datos en stream y los enviará al consumidor

#
