package libscala

/**  A software transactional memory "data space". Relies on Refs that point to immutable values.
  *  Refs can only be read or modified within transactions, like this: stm.transact {ref()}
  */

import scala.collection.immutable.IntMap
import java.util.concurrent.atomic.{AtomicReference, AtomicInteger}
import java.util.HashSet

/** To use libscala's STM without any fuss, just type:
 *  import libscala.globalSTM._
 *
 *  To create a transaction-safe variable, aka a Ref, use the 'ref' function:
 *  val stmInt = ref[Int](10) // an integer Ref with initial value 10. same as ref(10)
 *
 *  You must wrap a ref in a 'transact' block to read or update it.  Example:
 *  transact{println(stmtInt())} // This is WRONG. the transact block must be side-effect free
 *  // because it may be retried many times if there are conflicts. Solution:
 *  println(transact{stmtInt=1000; stmInt()})  // move the side-effecty command outside 'transact'
 *
 *  Transact blocks are guaranteed to commit successfully or throw an exception and do nothing:
 *  transact{stmtInt() = 80; stmtInt() = stmtInt() / 0} //stmtInt() is unchanged.
 *
 *  You can point a Ref to immutable data structures or values such as Map, Set, List, Int, Long:
 *  You can use Refs just like a vars; but with brackets() that imply 'dereferencing()' :-
 *  val stmMap = ref(Map[Int, String]()) //a ref that points to an immutable map.
 *  transact { stmMap() = stmMap().updated(100, "one hundred!") } //add a mapping to the map
 *  transact { stmMap()(100) } //read the Map.
 *  val stmSet = ref(Set(1, 2,3,5,7,11,13,17, 18)) //ref that points to a set of integers
 *  transact { stmSet() -= 18; stmSet() += 19; stmSet() -=  1}
 *
 */
object globalSTM extends STM

class STM {
    protected val emptyIntMap = IntMap[Any]() // we'll only ever need one of these.
    protected val refs = new AtomicReference(emptyIntMap) // points to the global list of refs
    protected val idRef = new AtomicInteger(0) //a unique id assigned to each Ref.
    abstract class RefT{var id: Int} // lets us store all transactional types in the same structs.

    /** A transaction-safe variable that points to <b>immutable values</b> of type T.
     */
    class Ref[T](initialValue: T) extends RefT {
        object NoTransactionException extends RuntimeException
        var id = idRef.incrementAndGet

        var tryAgain  = false
        do { // add this ref to the list of refs - atomically.
            val cur = refs.get
            tryAgain = !refs.compareAndSet(cur, cur.updated(id, initialValue))
        } while(tryAgain)

        def trans = {
            val t = transaction.get
            if (t != null) t else throw NoTransactionException
        }
        /** Read the reference without 'touching' it. Unsafe; works outside transactions. */
        def peek: T = try { trans.peek(this).asInstanceOf[T] }
                      catch { case NoTransactionException => transact { this.peek } }

        /** Get the value of the ref in the current transaction. */
        def apply(): T = trans.get(this).asInstanceOf[T]

        /** Set the value of the ref to 'v'. */
        def update(v: T) = trans.set(this, v)

        /**
         *  Remove the IntMap entry corresponding to this reference.  Programs that create a lot of
         *  Refs should enjoy memory savings and faster commits.  Cost: some commit contention.
         */
        override def finalize = {
            var tryAgain = false
            do { // remove id from list of refs - atomically.
                val cur = refs.get
                tryAgain = !refs.compareAndSet(cur, cur - id)
            } while(tryAgain)
        }
    }

    /** Create a Ref that points to an immutable data structure or value of type T.
     */
    def ref[T](initialValue: T) = new Ref(initialValue)

    /** The transaction associated with the current thread */
    protected var transaction = new ThreadLocal[Transaction]
    
    /** Start a new transaction and throw away the old one, if any */
    protected def begin() = { transaction.set(new Transaction) } //garbage collector handles it.

    /** Commit the running transaction.  Returns true if there were no errors */
    protected def commit() = { transaction.get.commit }

    /** Wrap the <b>side-effect-free function</b> 'func' in a transaction. Retry until successful.
     */
    def transact[T](func: => T) = {
        begin()
        var ans = func
        var success = commit()
        while(!success) {
            Thread.`yield` // Transactions are potentially very long, so don't retry too greedily.
            begin()
            ans = func
            success = commit()
        }
        transaction.set(null) // solves the 'phantom transaction' problem
        ans
    }
    def apply[T](func: => T) = transact(func)

    /** A memory transaction.  Relies on persistent data structures for efficiency. */
    protected class Transaction {
        val initial = refs.get
        var modified = emptyIntMap
        val touched = new java.util.HashSet[RefT] // compatible accross scala 2.7 and 2.8!

        def peek(ref: RefT) = modified.getOrElse(ref.id, initial(ref.id))

        def get(ref: RefT) = {
            touched.add(ref)
            peek(ref)
        }

        def set(ref: RefT, value: Any) = {
            touched.add(ref)
            modified = modified.updated(ref.id, value)
        }

        def commit = {
            if (modified.size == 0) true //don't waste cycles on read-only transactions.
            else {
                var tryAgain, success = false
                do {
                    // check whether any of the refs we touched ('get', 'set', but not 'peek')
                    // have been modified.  If so, we will need to retry the transaction.
                    var changed = false
                    val iter = touched.iterator
                    val current = refs.get //from parent.
                    while(iter.hasNext && !changed){
                        val id = iter.next.id
                        if(current(id) !=  initial(id)) changed = true
                    }

                    if (!changed) { 
                        //attempt atomic commit. fails if another transaction was committed:
                        success = refs.compareAndSet(current, current ++ modified) //'mergeable'
                        tryAgain = !success // try again: the other commit may be non-conflicting.
                    } else {
                        tryAgain = false
                    }
                } while(tryAgain)
                success
            }
        }
    }
}

