package libscala.usage

import libscala._

object concurrency {
    def main(args: Array[String]) = {
        produceConsume
    }

    /** A minimal producer-consumer example. Exercise: do this with actors.*/
    def produceConsume {
        val queue = new TaskQueue[Int](4) //integer queue with buffer size 4.
        val coord = new TaskJoiner        //construct for waiting for task completiion.

        // PRODUCER: spawn a thread which writes 100 random integers to a queue and dies.
        util.thread {()=>
            val random = new java.security.SecureRandom
            for (i <- 1 to 100) queue.put(random.nextInt())
            queue.close
        }

        // CONSUMER: spawn a thread which reads the integers and prints them out.
        queue.process { i =>
            println(Thread.currentThread.getName + " recieved " + i)
        }.into(coord)

        coord.join // wait for task completion.
    }
}