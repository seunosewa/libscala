package n
/** My first attempt at creating a library to help make web app development easy with Scala.
 *  It's functional and very useful, but won't be further developed.
 */
object web {
    import javax.servlet.ServletException
    import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
    import org.mortbay.jetty.Server
    import org.mortbay.jetty.bio.SocketConnector
    import org.mortbay.jetty.servlet.{ServletHandler, ServletHolder}
    def serve(host: String, port: Int, servlet: HttpServlet) {
        //Ugly boilerplate garbage from the Internets. Glad to have a place to hide it.
        val server = new Server
        
        val connector = new SocketConnector
        connector.setHost(host)
        connector.setPort(port)
        server.setConnectors (Array(connector))

        val sHandler = new ServletHandler
        server.setHandler(sHandler)

        val holder = new ServletHolder(servlet)

        sHandler.addServletWithMapping(holder, "/")

        server.start()
        server.join()
    }

    def serve(host: String, port: Int, handler: (HttpServletRequest, HttpServletResponse)
              => Unit) {
        serve(host, port, create_servlet(handler))
    }

    private def create_servlet(handler: (HttpServletRequest, HttpServletResponse)=> Unit): HttpServlet = {
        class NewServlet extends HttpServlet {
            override def service (request:HttpServletRequest, response:HttpServletResponse)
            = handler(request, response)
        }
        new NewServlet()
    }
}

object cache {
    import java.util.{LinkedHashMap, Collections}
    import java.util.Map.Entry
    /** A concurrent version isn't trivial due to LRU semantics.  We can't really have
     non-blocking gets because every get moves an element to the front of the queue.
     USAGE: val a = new nlib.cache.LRU[Int,Int](5); a(k) = v; x = a(key); a.-=(key)
     */
    class LRU [K,V] (cacheSize: Int, synced: Boolean) {
        private val rawMap = new LinkedHashMap[K,V](cacheSize, 0.75F, true) {
            override def removeEldestEntry(eldest:Entry[K, V]) = (this.size > cacheSize)
        }
        val map =  if (synced) Collections.synchronizedMap(rawMap) else rawMap
        def apply(key: K) = map.get(key)
        def update(key:K, value:V): Unit = map.put(key, value)
        override def toString = map.toString
        def remove(key: K): Unit = map.remove(key)
    }
}

object random {
    /** Returns an array of random bytes.  Allocates the byte array for you. */
    def bytes(size: Int) = {
        val random = new java.util.Random
        val buf = new Array[Byte](size)
        random.nextBytes(buf)
        buf
    }

    /** Characters that can be returned in a url-safe string */
    val safeChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890._"

    /** URL-Safe string consisting of only the characters in safechars */
    def safeString(size: Int) = {
        val sb = new StringBuilder
        for (b <- bytes(size)) sb.append (safeChars(b&63))
        sb.toString
    }
}

object email {
    import java.util.regex.Pattern

    private val emailRegex = ".+@.+\\.[a-z]+"
    def emailIsValid(email: String) = Pattern.matches(emailRegex, email)

    def clean(rawEmail: String) = {
        //TODO: normalize, remove spaces, etc
        var email = rawEmail.toLowerCase
        if (!emailIsValid(email)) throw new RuntimeException(email + " is not valid")
        email
    }

    import java.util.Properties
    import javax.mail._
    import javax.mail.internet._

    var smtpHost = "localhost"
    var smtpPort = "25" //why not int, for christsakes

    //nlib.email.send("apache@nairaland.com", "seunosewa@gmail.com", "Testing Java Mail","message body")
    def send(from: String, to: String, title: String, body: String) = {
        val prop = new Properties()
        prop.put("mail.smtp.host", smtpHost)
        prop.put("mail.smtp.port", smtpPort)
        val sess = Session.getDefaultInstance(prop, null)

        val msg = new MimeMessage(sess);
        msg.setFrom(new InternetAddress(from));
        msg.setRecipient(Message.RecipientType.TO, new InternetAddress(to));
        msg.setSubject(title);
        msg.setText(body);
        Transport.send(msg);
    }
}

object util {
    /** For templating. Concatenates all arguments into a String, which it returns */
    def join(items: Any*) = {
        val builder = new StringBuilder(items.length)
        for (item <- items) builder.append(item)
        builder.toString()
    }

    def ints2long(i: Int, j: Int) = {
        val ii: Long = i.asInstanceOf[Long]
        val jj: Long = j.asInstanceOf[Long]
        (ii << 32) | (jj & 0x00000000FFFFFFFFL)
    }

    def long2ints(l: Long) = {
        val i = (l>>32).asInstanceOf[Int]
        val j = (l.asInstanceOf[Int])
        (i, j)
    }

    def forRange(l: Int)(func: (Int) => Unit) = {
        var i = 0; while (i < l) {
            func(i)
            i += 1
        }
    }
}

object py {
    def max(items: Int*) = items.reduceLeft(_.max(_))
    def max(items: Long*) = items.reduceLeft(_.max(_))
    def max(items: Double*) = items.reduceLeft(_.max(_))

    def min(items: Int*) = items.reduceLeft(_.min(_))
    def min(items: Long*) = items.reduceLeft(_.min(_))
    def min(items: Double*) = items.reduceLeft(_.min(_))
}

object diff {
    import name.fraser.neil.plaintext.diff_match_patch.Diff
    import java.util.LinkedList
    val dmp = new name.fraser.neil.plaintext.diff_match_patch()
    import dmp._
    Match_Distance = 1024

    var lastDiff: LinkedList[Diff] = null
    def delta(original: String, modified:String)= {
        val diff = diff_main(original, modified)
        lastDiff = diff
        diff_cleanupEfficiency(diff)
        diff_toDelta(diff)
    }

    def merge(original: String, delta:String) = {
        val diff = diff_fromDelta(original, delta)
        lastDiff = diff
        diff_text2(diff)
    }

    def htmlDelta(original: String, modified: String) = {
        val diff = diff_main(original, modified)
        lastDiff = diff
        diff_prettyHtml(diff)
    }

    def additions(ref: String, current: String) = {
        val diff = diff_main(ref, current)
        lastDiff = diff
        val diffArray = diff.toArray(new Array[Diff](0))
        for (d <- diffArray) {

        }
    }
}
object xdiff {
    import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream}
    import com.nothome.delta.{Delta, GDiffWriter, GDiffPatcher}

    def delta(original: String, modified: String) = {
        val source = original.getBytes("UTF-8")
        val target = modified.getBytes("UTF-8")
        val targetIs = new ByteArrayInputStream(target)
        val outputBuffer = new ByteArrayOutputStream()
        val output = new GDiffWriter(new DataOutputStream(outputBuffer))
        Delta.computeDelta(source, targetIs, target.length, output)
        output.flush
        outputBuffer.toByteArray
    }

    def merge(original: String, delta: Array[Byte]) = {
        val source = original.getBytes("UTF-8")
        val patchIs = new ByteArrayInputStream(delta)
        val outputBuffer = new ByteArrayOutputStream()
        val g = new GDiffPatcher(source, patchIs, outputBuffer)
        outputBuffer.toString("UTF-8")
    }
}

object algo {
    def deflate(input: Array[Byte]): Array[Byte] = {
        val compressor = new java.util.zip.Deflater(9, true)
        val output = new Array[Byte](input.length + 32) //padding

        compressor.setInput(input)
        compressor.finish
        val compressedSize = compressor.deflate(output)

        java.util.Arrays.copyOf(output, compressedSize)
    }

    def deflate(inputString: String): Array[Byte] = deflate(inputString.getBytes("UTF-8"))
}

object hash {
    import java.security.MessageDigest

    def SHA1(input:String): Array[Byte] = SHA1(input.getBytes("UTF-8"))

    def SHA1(input:Array[Byte]): Array[Byte] = MessageDigest.getInstance("SHA").digest(input)

    /** Checks whether 'hash' is the SHA1 hash of 'input'. Not so obvious. */
    def SHA1Match(input: String, hash: Array[Byte]) = java.util.Arrays.equals(hash, SHA1(input))

    import java.math.BigInteger
    def bytesToHex(bytes: Array[Byte]) = new BigInteger(1, bytes).toString(16)

    /** 64-bit hash equivalent to http://stackoverflow.com/questions/1660613 */
    def toLong (word: String) = word.foldLeft(1125899906842597L)(31 * _ + _)
    def toLong2(word: String) = {
        var h: Long = 1125899906842597L
        val len = word.length
        var i = 0; while (i < len) {
            h = 31*h + word.charAt(i)
            i += 1
        }
        h
    }
    def toInt (word:String) = word.hashCode
    def toIntAsLong(word:String) = word.hashCode.asInstanceOf[Long]
    def toLong (word1: String, word2: String) =
        (toIntAsLong(word1)<<32) | (toIntAsLong(word2) & 0xFFFFFFFFL)
}

object file {
    import java.io._

    def deserializeFrom (fileName: String) = {
        val inp = new ObjectInputStream(new FileInputStream(fileName))
        val obj = inp.readObject
        inp.close
        obj
    }

    def serializeTo (fileName: String, obj: Object) = {
        val out = new ObjectOutputStream(new FileOutputStream(fileName))
        out.writeObject(obj)
        out.close
    }
}

object sys {
    def mem = {
        val r = Runtime.getRuntime
        r.totalMemory - r.freeMemory
    }
    def gc = {
        Runtime.getRuntime.gc
    }
}