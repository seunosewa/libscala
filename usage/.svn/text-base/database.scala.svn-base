package libscala.usage
import libscala._

object database {
    var mysql: MySQL = null

    def main(args: Array[String]) = {
        val password = if (args.length>=1) args(0) else ""
        println("Using password: " + password )

        mysql = new MySQL("localhost", 3306, "root", password, "test", 2)
        showProcesslist
        mysql.tx(createAndSelect) //wrapping a function in a transaction
    }

    def showProcesslist = {
        val rs = mysql.tx(_("show processlist")) //wrapping a single cmmand in a transaction
        for (row <- rs) row.println(8)
    }

    def createAndSelect(sql: Sql) = {
        //transaction-wrapped functions can call other data functions within the same transaction.
        create(sql)
        select(sql)
    }
    
    def create(sql: Sql) = {
        sql(" drop table if exists libscala ")
        sql(" create table libscala( id int unsigned primary key, name varchar(20) not null ) " +
            " type=innodb ") //innodb is essential to transaction support
        for (i <- 1 to 10) {
            sql("insert into libscala values (?, ?)", Seq(i, "NUMBER " + i.toString))
        }
    }

    def select(sql: Sql) = {
        val rs = sql("select id, name from libscala")
        for (row <- rs) {
            val (id, name) = (row int 1, row ascii 2)
            println("ID = " + id + " and NAME = " + name)
        }
    }
    
    def noTransaction = {
        val conn = mysql.newConnection
        val st = conn.createStatement
        st.execute("drop table libscala")
        st.close; conn.close
    }
}