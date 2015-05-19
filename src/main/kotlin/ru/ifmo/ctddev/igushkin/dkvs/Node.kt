package ru.ifmo.ctddev.igushkin.dkvs

import java.io.BufferedReader
import java.io.File
import java.io.FileWriter
import java.io.IOException
import java.net.*
import java.util.concurrent.LinkedBlockingDeque
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.concurrent.thread

/**
 * @param id Node identifier which should be unique across the system instance.
 */
public class Node(val id: Int) : Runnable, AutoCloseable {

    private val persistenceFileName = "dkvs_$id.log"
    private val diskPersistence = FileWriter(persistenceFileName, true).buffered()

    val logger = Logger.getLogger("node.$id")
    fun log(s: String) = logger.info(s)
    fun logErr(s: String, t: Throwable? = null) = logger.log(Level.SEVERE, s, t)

    private fun saveToDisk(data: Any) {
        synchronized(diskPersistence) {
            with(diskPersistence) {
                write(data.toString())
                newLine()
                flush()
            }
        }
    }

    private val inSocket = ServerSocket(globalConfig.port(id))

    private volatile var started = false
    private volatile var stopping = false

    override public fun run() {
        if (started)
            throw IllegalStateException("Cannot start a node which has already been started")

        started = true

        for (i in 1..globalConfig.nodesCount)
            if (i != id)
                thread { speakToNode(id) }

        thread {
            while (!stopping)
                try {
                    val client = inSocket.accept()
                    thread { handleRequest(client) }
                } catch (ignored: SocketException) {
                }
        }
    }

    override fun close() {
        stopping = true
        inSocket.close()
        for (n in neighbors) {
            if (n.input != null) n.input!!.close()
            if (n.output != null) n.output!!.close()
        }
    }

    private data class NeighborEntry(var input: Socket? = null,
                                     var output: Socket? = null) {
        val messages: LinkedBlockingDeque<Message> = LinkedBlockingDeque()
    }

    private val neighbors = Array(globalConfig.nodesCount) { NeighborEntry() }

    private fun handleRequest(client: Socket) {
        val reader = client.getInputStream().reader(CHARSET).buffered()
        reader.use {
            dispatch(it) { parts ->
                when (parts[0]) {
                    "node" -> listenToNode(client, parts[1].toInt())
                    "get"  -> {
                    }
                }
            }
        }
    }

    private fun send(to: Int, message: Message) {
        neighbors[to].messages.addLast(message)
    }

    private fun listenToNode(client: Socket, nodeId: Int) {
        val reader = client.getInputStream().reader(CHARSET).buffered()
        log("Started listening to node.$nodeId from ${client.getInetAddress()}")
        dispatch(reader) { parts ->

        }
    }

    private fun speakToNode(nodeId: Int) {
        val clientSocket = Socket()
        neighbors[nodeId].output = clientSocket

        val address = globalConfig.address(nodeId)
        val port = globalConfig.port(nodeId)

        if (address == null) {
            println("Couldn't get address for $nodeId, closing.")
            return
        }

        while (!stopping) {
            try {
                clientSocket.connect(InetSocketAddress(address, port))
                log("Connected to node.$nodeId.")
                send(nodeId, NodeMessage(id))
                val writer = clientSocket.getOutputStream().writer(CHARSET)

                forever {
                    val m = neighbors[nodeId].messages.pollFirst()
                    try {
                        writer.write("$m\n")
                    } catch (ioe: IOException) {
                        neighbors[nodeId].messages.addFirst(m)
                    }
                }

            } catch (e: SocketException) {
                logErr("Connection to node.$nodeId lost.", e)
            }
        }
    }
}