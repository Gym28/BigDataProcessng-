package io.keepcoding.spark.exercise.provisioner

import java.sql.{Connection, DriverManager}

object JdbcProvisioner {

  def main(args: Array[String]) {
    val IpServer = "localhost"

    // connect to the database named "mysql" on the localhost
    val driver = "org.postgresql.Driver"
    val url = s"jdbc:postgresql://$IpServer:5432/postgres"
    val username = "postgres"
    val password = "mysecretpassword"

    // there's probably a better way to do this
    var connection: Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      println("Conexión establecida correctamente!")

       println("Creando la tabla user_metadata (id TEXT, model TEXT, version TEXT, location TEXT)")
      statement.execute("CREATE TABLE IF NOT EXISTS user_metadata(id TEXT, name TEXT, email TEXT, quota BIGINT)")

      println("Creando la tabla antenna_agg (location TEXT, date TIMESTAMP, avg_devices_count BIGINT, max_devices_count BIGINT, min_devices_count BIGINT)")
      statement.execute("CREATE TABLE IF NOT EXISTS antenna_agg (location TEXT, date TIMESTAMP, avg_devices_count BIGINT, max_devices_count BIGINT, min_devices_count BIGINT)")

      println("Creando la tabla antenna_1h_agg (location TEXT, date TIMESTAMP, avg_devices_count BIGINT, max_devices_count BIGINT, min_devices_count BIGINT)")
      statement.execute("CREATE TABLE IF NOT EXISTS antenna_1h_agg (location TEXT, date TIMESTAMP, avg_devices_count BIGINT, max_devices_count BIGINT, min_devices_count BIGINT)")



      println("Creando la tabla bytes (date TIMESTAMP, id TEXT, value BIGINT, type TEXT )")
      statement.execute("CREATE TABLE IF NOT EXISTS bytes(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT)")

      println("Creando la tabla bytesByHour (date TIMESTAMP, id TEXT, value BIGINT, type TEXT )")
      statement.execute("CREATE TABLE IF NOT EXISTS bytesByHour(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT)")


      println("Dando de alta la información de usuarios")

      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000001', 'andres', 'andres@gmail.com', 200000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000002', 'paco', 'paco@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000003', 'juan', 'juan@gmail.com', 100000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000004', 'fede', 'fede@gmail.com', 5000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000005', 'gorka', 'gorka@gmail.com',200000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000006', 'luis', 'luis@gmail.com', 200000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000007', 'eric', 'eric@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000008', 'carlos', 'carlos@gmail.com', 100000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000009', 'david', 'david@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000010', 'juanchu', 'juanchu@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000011', 'charo', 'charo@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000012', 'delicidas', 'delicidas@gmail.com', 1000000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000013', 'milagros', 'milagros@gmail.com', 200000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000014', 'antonio', 'antonio@gmail.com', 1000000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000015', 'sergio', 'sergio@gmail.com', 1000000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000016', 'maria', 'maria@gmail.com', 1000000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000017', 'cristina', 'cristina@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000018', 'lucia', 'lucia@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000019', 'carlota', 'carlota@gmail.com', 200000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000020', 'emilio', 'emilio@gmail.com', 200000)")
    } catch {
      case e => e.printStackTrace()
    }
    connection.close()
  }

}
