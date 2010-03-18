package libscala.usage
import libscala._

object stm {
    import globalSTM._ //use the default STM dataspace
    val i = ref[Int](0)
    val j = ref[Int](0) // inferred from the above.
    val k = ref[Int](0) // inferred from the above.

    def spawn(x: Ref[Int], y: Ref[Int]) = util.spawn { util.bench{pingpong(x, y)} }

    def pingpong(x: Ref[Int], y: Ref[Int]) {
      val rand = new java.util.Random;
      var n = 0; while (n < 10000) {
          val dir = rand.nextBoolean;
          transact {
              val xx =  if (dir) x else y;
              val yy = if (dir) y else x;
              if(xx() < 2000000000) yy() = (yy()+1);
          }
          n+=1;
      }
      println(transact{x()+" "+y()})
    }
    def test(x: Ref[Int]) = util.bench { 
        var n = 0; while (n < 10000000) {
            transact { x() = x() + 1 }
            n += 1
        }
     }

    def ii = transact(i())
    def jj = transact(j())

    def main(array: Array[String]) = {
        for (n <- 1 to 4) {spawn(i,j); spawn(j,i);}
        Thread.sleep(100000)
    }
}