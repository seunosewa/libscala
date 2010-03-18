package libscala

import java.sql.{ResultSet, PreparedStatement, Connection}
import java.sql.{DriverManager, SQLException, SQLTransientException, SQLRecoverableException}
import java.sql.{SQLNonTransientConnectionException}


class MySQL (host:String, port:Int, user:String, pass:String, dbname:String, poolSize: Int)
  extends DBPool (util.connString("mysql", host, port, dbname, user, pass), poolSize) {
   Class.forName("com.mysql.jdbc.Driver").newInstance() //load the mysql driver.
}

class DBPool (connstring: String, poolSize: Int) {
    val pool = new java.util.concurrent.LinkedBlockingDeque[Connection](poolSize)
    for (i <- 0 until poolSize) pool.put(newConnection)

    def newConnection = {
        val conn = DriverManager.getConnection(connstring + "&autoReconnect=true")
        conn.setAutoCommit(false)
        conn
    }

    /** Wrap func in a database <b>transaction</b>. Retry on deadlock or broken connection. */
    def tx[R](func: (Sql) => R) = {
        var completed = false
        var result: R = null.asInstanceOf[R]
        var conn = pool.takeLast //LIFO: maximises thread reuse and hence helps cache performance.
        try {
            do {
                try {
                    val sql = new Sql(conn)
                    result = func(sql)
                    conn.commit()
                    completed = true
                } catch {
                    case e: SQLTransientException => { println("Transient: " + e.getSQLState) }
                    case e: SQLRecoverableException => {
                        println("Recoverable: " + e.getSQLState); conn = replaceConnection(conn)
                    }
                    case e: SQLNonTransientConnectionException => {
                        println("ConnLost: " + e.getSQLState);
                        conn = replaceConnection(conn)
                    }
                }
            } while (!completed)
        } finally {
            if(!completed) {
                println("trying to ROLLBACK.")
                try { conn.rollback } catch {
                    case e: SQLException => {
                        println("IO error on attempted rollback.")
                        conn = replaceConnection(conn)
                    }
                }
            }
            pool.put(conn) // exceptions will drain the pool if we don't do this.
        }  
        result
    }
    def replaceConnection(oldConn: Connection) = {
        Thread.sleep(1000) //prevent unintended denial of service
        try { oldConn.close } catch { case e:SQLException => println("error closing") }
        newConnection
    }
}

case class Sql (conn: Connection) {
    var rs: ResultSetIterator = null

    def apply (statement: String, parameters: Seq[Any]): ResultSetIterator = {
        val st = conn.prepareStatement(statement)
        val l = parameters.length

        for(i <- 0 until l) {
            val param = parameters(i) //Any isn't object. grr.
            val param2 = if (param.isInstanceOf[String]) param.asInstanceOf[String].getBytes("UTF-8") else param
            st.setObject(i+1, param2)
        }
        st.execute()
        new ResultSetIterator(st.getResultSet, st)
    }

    val array = new Array[Any](0)
    def apply (statement: String): ResultSetIterator = apply(statement,array)
}

class ResultSetExhausted (m: String) extends SQLException(m)
class AlreadyIterating (m: String) extends SQLException(m)

class ResultSetIterator (rs: ResultSet, st: PreparedStatement) extends Iterator[ResultSetIterator] {
    private var rowNum = 0
    private var peeked = false

    def hasNext = if (peeked) true else {
        peeked = rs.next
        peeked  // you can't peek at what doesn't exist
    }

    def next = if (!hasNext) throw new ResultSetExhausted("try reset") else {
        rowNum += 1
        peeked = false
        this
    }

    // Convenience methods:
    def reset = { rs.beforeFirst; rowNum = 0 }
    def exists = if (rowNum > 1) throw new AlreadyIterating("try reset") else hasNext
    def empty = !exists
    def get = next

    // Retrieving field values:
    def bool(col: Int) = rs.getBoolean(col)
    def int(col: Int) = rs.getInt(col)
    def double(col: Int) = rs.getDouble(col)
    def bytes(col: Int) = rs.getBytes(col)
    def ascii(col: Int) = new String(rs.getBytes(col), "ASCII")
    def latin1(col: Int) = new String(rs.getBytes(col), "ISO-8859-1")
    def unicode(col: Int) = new String(rs.getBytes(col), "UTF-8")
    def decode(col: Int, encoding: String) = new String(rs.getBytes(col), encoding)
    def raw(col: Int) = rs.getString(col)

    //convenience hack:
    def ln(numRows: Int) = {
        util.joinSeq(" ", for (i <- 1 to numRows) yield this.raw(i))
    }
    def println(numRows: Int) = scala.Predef.println(ln(numRows))
    // prevent memory leaks:
    override def finalize = { rs.close;  st.close; }
}