package ru.ifmo.ctddev.igushkin.dkvs

import java.io.File
import java.io.FileWriter
import java.net.*
import java.util.logging.Logger
import kotlin.concurrent.thread

public class Node(val id: Int) : Runnable, AutoCloseable {

    private val persistenceFileName = "dkvs_$id.log"
    private val diskPersistence = FileWriter(persistenceFileName, true).buffered()

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

        for (i in 1..globalConfig.nodesCount) {
            if (i != id)
                thread { neighborCommunication(id) }
        }

        thread {
            while (!stopping) {
                try {
                    val client = inSocket.accept()
                    thread { incomingCommunication(client) }
                } catch (ignored: SocketException) {
                }
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
                                     var output: Socket? = null)

    private val neighbors = Array(globalConfig.nodesCount) { NeighborEntry() }

    private fun incomingCommunication(client: Socket) {
        val reader = client.getInputStream().reader(CHARSET).buffered()
        reader.use {
            for (line in it.lines()) {
                val parts = line.split("\\s+")
                when (parts[0]) {
                    "node" -> {

                    }
                }
            }
        }
    }

    private fun neighborCommunication(neighborId: Int) {
        val clientSocket = Socket()
        neighbors[neighborId].output = clientSocket

        val address = globalConfig.address(neighborId)
        val port = globalConfig.port(neighborId)

        if (address == null) {
            println("Coundn't get address for $neighborId, closing.")
            return
        }

        while (!stopping) {
            try {
                clientSocket.connect(InetSocketAddress(address, port))

            } catch (ignored: SocketException) {
            }
        }
    }
}