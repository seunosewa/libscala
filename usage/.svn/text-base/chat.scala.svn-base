package libscala.usage
import libscala._

/*  Database Format
 *  
 *  rooms: roomName -> Vector(Topics)
 *  topics: title,
 */

object chat extends STM{
    case class Chat(poster: String, body: String)

    //create and initialize "tables".
    val users = ref(Map("seun" -> ref(0), "mukina2" -> ref(0), "debosky" -> ref(0)))

    //update refs within a transaction
    val rooms = ref {
        val t = for (room <- Seq("python", "java", "scala"))
          yield (room -> ref(List(Chat("seun", "first line"))))
        Map[String, Ref[List[Chat]]](t:_*)
    }

    def say(room: String, user: String, body: String) = {
        if (!users().contains(user)) throw new RuntimeException("user not found: " + user)
        if (!rooms().contains(room)) throw new RuntimeException("room not found" + room)
        val listRef = rooms()(room)
        listRef() = (Chat(user, body) :: listRef()).take(10)
        users()(user)() += 1
    }

    def sayMany(room: String, user: String) = util.spawn {
        util.bench {
            var n = 0; while(n < 3000000) {
                transact{say(room, user, Thread.currentThread.getName + " " + n)}
                n += 1
            }
        }
    }

    def main(a: Array[String]) = {
        println(users())
        println
        println(rooms())
    }
}