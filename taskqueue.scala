package libscala

import java.util.concurrent.ArrayBlockingQueue
import scala.collection.mutable.ArrayBuffer
import util._

trait TaskQueueT[IN] {
    def put (task: IN): Unit
    def take: IN
    def close: Unit
}

/** Thread-safe queue class for multithreading. Contains helper functions that make it very
 *  easy to setup threads that process messages from this queue using the assembly-line pattern.
 */
class TaskQueue[IN] (size: Int) extends TaskQueueT[IN] {
    /** The backing queue. Use this (carefully) when you need more fancy features.*/
    val q = new ArrayBlockingQueue[(Boolean, IN)](size)

    /** Put an item into this queue for processing. */
    def put(task: IN) = if (task != null) q.put((false, task))

    /** Take an item from this queue.  Blocks until it succeeds or the queue is halted. */
    def take: IN = {
        val (closed, task) = q.take
        if (closed) {
            q.clear; q.put((closed, task)) //kill other threads watching this.
            throw ClosedQueueException
        } else task
    }
    def close: Unit = q.put((true, null.asInstanceOf[IN]))

    /** Spawn a thread to process the queue.  Buffer the results in out. */
    def process[OUT](func: (IN) => OUT) = new TaskProcessor[IN, OUT](this) {
        def process (task: IN) = func(task)
    }

    def unfold[OUT](func: (IN)=> Seq[OUT]) = {
        new TaskProcessor[IN, OUT](this) {
            override def run = {
                try { while(true) for (i <- func(in.take)) out.put(i) }
                catch {
                    case ClosedQueueException => println (Thread.currentThread.getName + " S")
                } finally out.close
            }
            def process (task: IN): OUT = null.asInstanceOf[OUT]
        }
    }

    /** Spawn 'n' threads to process tasks in parallel. Initial order not preserved.*/
    def process[OUT](n: Int)(func: (IN) => OUT) = 
        for (i <- 1 to n) new TaskProcessor[IN, OUT](this) {
            def process (task: IN) = func(task)
        }

    /** Process the queue in a pipeline with n nodes. Parallel computation, results in order! */
    def pipeline[OUT](n: Int)(func: (IN)=> OUT) = {
        if (n >= 2) {
            case class Task (in: IN, num: Int){ var out = null.asInstanceOf[OUT] }

            def mayProcess(curProcess: Int, task: Task) =
                if (task.num % n == (n-1) - curProcess) task.out = func(task.in)
                // If the first thread processed the first task, it would starve other threads in
                // the pipeline while processing that task. curProcess is inverted to prevent this.

            @volatile var num = 0
            val firstQ = process { in =>
                val task = new Task(in, num);
                num += 1
                mayProcess(0, task)
                task
            }.newQueue(size)
            var curQ = firstQ
            for (i <- 1 until n-1) {
                curQ = curQ.process { task =>
                    mayProcess(i, task); task
                }.newQueue(size)
            }

            val lastQ = curQ
            lastQ.process { task =>
                mayProcess(n-1, task); task.out
            }
        } else process(func)
    }

    /** Spawn a thread that inserts the messages in the iterator 'iter' into this queue. */
    def generate(iter: Iterator[IN]) = util.thread { () =>
        while(iter.hasNext) this.put(iter.next)
        this.close
    }

    override def finalize = close // useless in practice: processes hold references to the queue.
}

/** Used with Queue.generate to quietly shut down a task feeder thread. */


/** The queue you're trying to read from can't be read from anymore */
object ClosedQueueException extends RuntimeException

/** TaskProcessor already initialized with a different queue */
object AlreadyStartedException extends RuntimeException

/** A thread that reads from a queue 'in', runs the function 'process' on each enqueued item,
 *  and enqueues the results in another queue 'out'. The thread dies when 'in' is halted. */
abstract class TaskProcessor [IN, OUT] (protected val in: TaskQueueT[IN]) extends Thread {
    protected var out = null.asInstanceOf[TaskQueueT[OUT]]

    private val started = new java.util.concurrent.atomic.AtomicBoolean(false)
    private def startWith(outQ: TaskQueueT[OUT]) = {
        val alreadyStarted = !started.compareAndSet(false, true)
        if (alreadyStarted) throw AlreadyStartedException else { out = outQ; start }
    }

    override def run = {
        try { while(true) out.put(process(in.take)) }
        catch {
            case ClosedQueueException => println (Thread.currentThread.getName + " halted")
        } finally out.close
    }
    setDaemon(true)
    def process(task: IN): OUT

    def discard = startWith(new TaskQueueT[OUT] {
      def put(item: OUT): Unit = {}
      def close: Unit = {}
      def take: OUT = throw ClosedQueueException
    })

    /** Send the results of this TaskProcess into a new TaskQueue and return it. */
    def newQueue(size: Int): TaskQueue[OUT] = { val q = new TaskQueue[OUT](size); startWith(q); q }

    /** Send the results of this TaskProcess into two new TaskQueues and return them */
    def newQueues(size1: Int, size2: Int): (TaskQueue[OUT],TaskQueue[OUT]) = {
        val out1 = new TaskQueue[OUT](size1)
        val out2 = new TaskQueue[OUT](size2)
        val proxyQ = new TaskQueueT[OUT] {
            def put(item: OUT) = { out1.put(item); out2.put(item); }
            def close = { out1.close; out2.close }
            def take = throw ClosedQueueException //write-only queue
        }
        startWith(proxyQ)
        (out1, out2)
    }
    
    /** Send the results of this TaskProcess into an array of new TaskQueues and return them.
     *  Not tested yet, oops.
     */
    def newQueues(sizes: Int*): Array[TaskQueue[OUT]] = {
        val outs = new Array[TaskQueue[OUT]](sizes.length)
        for (i <- 0 until outs.length) outs(i) = new TaskQueue(sizes(i))
        val proxyQ = new TaskQueueT[OUT] {
          def put(item: OUT) = {for (out <- outs) out.put(item)}
          def close = {for (out <- outs) out.close}
          def take = throw ClosedQueueException //write-only
        }
        startWith(proxyQ)
        outs
    }

    /** Send the results of this process into an already-created TaskQueueT */
    def into(queue: TaskQueueT[OUT]) = startWith(queue)

    /** Track the status of this process with a TaskJoiner. */
    def into(coord: TaskJoiner) = startWith(coord.queue)
}

/** Use this to wait until a series of tasks have been completed.  Use from a single thread. */
class TaskJoiner {
    private val joins = new scala.collection.mutable.ArrayBuffer[() => Any]

    /** Returns a queue that blocks on 'take' until halted. */
    def queue[T] = { val q = new JoinQueue[T]; joins += (q.join _); q }

    /** block until every watched queue is halted. */
    def join = joins.foreach(i => print(" ("+ i() + ") "))

    /** A queue that blocks on take until it's halted*/
    class JoinQueue[T] extends TaskQueueT[T] {
        private val closed = new java.util.concurrent.atomic.AtomicBoolean
        def put(item: T) = {}
        def take: T = { 
            while(!closed.get) synchronized {this.wait}
            null.asInstanceOf[T]
        }
        def close = { closed.set(true); synchronized{this.notifyAll} }
        def join: Unit = take
    }
}