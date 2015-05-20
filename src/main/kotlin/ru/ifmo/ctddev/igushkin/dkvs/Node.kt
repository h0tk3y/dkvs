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

    private val serverSocket = ServerSocket(globalConfig.port(id))

    private volatile var started = false
    private volatile var stopping = false

    private val sender = { to: Int, m: Message -> send(to, m) }
    private val localReplica = Replica(id, sender, globalConfig.ids)

    override public fun run() {
        if (started)
            throw IllegalStateException("Cannot start a node which has already been started.")

        started = true

        for (i in 1..globalConfig.nodesCount)
            if (i != id)
                thread { speakToNode(id) }

        thread {
            handleMessages()
        }

        thread {
            while (!stopping)
                try {
                    val client = serverSocket.accept()
                    thread { handleRequest(client) }
                } catch (ignored: SocketException) {
                }
        }
    }

    override fun close() {
        stopping = true
        serverSocket.close()
        for (n in neighbors) {
            with(n) {
                input?.close()
                output?.close()
            }
        }
    }

    private data class NeighborEntry(var input: Socket? = null,
                                     var output: Socket? = null) {
        val messages: LinkedBlockingDeque<Message> = LinkedBlockingDeque()
    }

    private val neighbors = Array(globalConfig.nodesCount) { NeighborEntry() }
    private val clients = sortedMapOf<Int, NeighborEntry>()

    private fun send(to: Int, message: Message) {
        if (to == id)
            eventQueue.offer(message)
        else
            neighbors[to].messages.offer(message)
    }

    /**
     * Executed in new thread, it decides what kind of connection [client] belongs to
     * and switches to [listenToNode] or [listenToClient]
     */
    private fun handleRequest(client: Socket) {
        val reader = client.getInputStream().reader(CHARSET).buffered()
        reader.use {
            dispatch(it) { parts ->
                when (parts[0]) {
                    "node"                 -> {
                        val nodeId = parts[1].toInt()
                        with (neighbors[nodeId]) {
                            input?.close()
                            input = client
                        }
                        listenToNode(reader, nodeId)
                    }
                    "get", "set", "delete" -> {
                        val newClientId = (clients.keySet().max() ?: 0) + 1
                        clients[newClientId] = NeighborEntry(client)

                        // since we've already read a message, we have to handle it on the spot
                        val firstMessage = ClientRequest.parse(newClientId, parts)
                        eventQueue.offer(firstMessage)

                        thread { speakToClient(newClientId) }
                        listenToClient(reader, newClientId)
                    }
                }
            }
        }
    }

    /**
     * Executed in main thread, it takes received messages one by one from
     * [eventQueue] and handles them or forwards them to the proper receivers.
     */
    private fun handleMessages() {
        forever {
            val m = eventQueue.poll()
            when (m) {
                is ReplicaMessage -> localReplica.receiveMessage(m)
            }
        }
    }

    /**
     * Messages from this queue are polled and handled by handleMessages.
     * Every communication thread puts its received messages into the queue.
     */
    val eventQueue = LinkedBlockingDeque<Message>()

    /**
     * Executed in a communication thread, it puts all the messages received from
     * another nodes into [eventQueue].
     */
    private fun listenToNode(reader: BufferedReader, nodeId: Int) {
        log("Started listening to node.$nodeId.")
        dispatch(reader) { parts ->
            val message = Message.parse(parts)
            eventQueue.offer(message)
        }
    }

    /**
     * Executed in a communication thread, it puts all the messages received from
     * a client into [eventQueue].
     */
    private fun listenToClient(reader: BufferedReader, clientId: Int) {
        log("Client $clientId connected.")
        dispatch(reader) { parts ->
            val message = ClientRequest.parse(clientId, parts)
            log("Message from $clientId: ${joined(parts.asList(), " ")}")
            localReplica.receiveMessage(message)
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
                        log("Sending to $nodeId: $m")
                        writer.write("$m\n")
                    } catch (ioe: IOException) {
                        logErr("Couldn't send a message. Retrying.")
                        neighbors[nodeId].messages.addFirst(m)
                    }
                }

            } catch (e: SocketException) {
                logErr("Connection to node.$nodeId lost.", e)
            }
        }
    }

    private fun speakToClient(clientId: Int) {
        val entry = clients[clientId]!!
        val queue = entry.messages
        val writer = entry.input!!.getOutputStream().writer()
        forever {
            val m = queue.poll()
            try {
                log("Sending to client $clientId: $m")
                writer write "$m\n"
            } catch (ioe: IOException) {
                logErr("Couldn't send a message. Retrying.")
                neighbors[clientId].messages.addFirst(m)
            }
        }
    }
}