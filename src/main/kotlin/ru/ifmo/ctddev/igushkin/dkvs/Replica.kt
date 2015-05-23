package ru.ifmo.ctddev.igushkin.dkvs

import java.io.File
import java.io.FileWriter
import java.util.ArrayList
import java.util.HashMap

/**
 * Represents `replica` of Multi-Paxos protocol.
 *
 * For complete description, see [Paxos Made Moderately Complex]
 * [http://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf]
 *
 * @param id Replica identifier, unique across the protocol instance.
 * @param send (nodeId, message) Way to send messages to the other nodes.
 * @param leaderIds List of ids which the replica will ask for decisions.
 *
 * @property state Replica state. Paxos supports any abstract state, but
 * in DKVS the state is a key-value string map.
 *
 * @property slotIn Next slot to propose a request to.
 * @property slotOut First slot with non-applied request.
 *
 * @property requests [ClientRequest]s which haven't been proposed yet
 * @property proposals slot -> [ClientRequest] which have been proposed for certain slots but haven''t been processed by [Leader]s yet
 * @property decisions slot -> [ClientRequest] which have been accepted by [Leader]s
 * @property performed [ClientRequest]s which have already been [perform]ed.
 */

public class Replica(val id: Int,
                     val send: (nodeId: Int, Message) -> Unit,
                     val sendToClient: (clientId: Int, text: String) -> Unit,
                     val leaderIds: List<Int>
) {

    private val persistenceFileName = "dkvs_$id.log"
    private val diskPersistence = FileWriter(persistenceFileName, true).buffered()

    private fun saveToDisk(data: Any) {
        with(diskPersistence) {
            write(data.toString())
            newLine()
            flush()
        }
    }

    private val state: MutableMap<String, String> = readStateFromDisk()

    private fun readStateFromDisk(): HashMap<String, String> {
        if (!File(persistenceFileName).exists())
            return HashMap()
        val reader = File(persistenceFileName).reader().buffered()
        val lines = ArrayList<String>()
        for (l in reader.lines())
            lines add l
        val result = hashMapOf<String, String>()
        val removedKeys = hashSetOf<String>()
        for (l in lines.reverse()) {
            val parts = l.split(' ')
            val key = parts[1]
            if (key in result || key in removedKeys)
                continue
            when (parts[0]) {
                "set"    -> {
                    result[key] = parts[2..parts.lastIndex].join(" ")
                }
                "delete" -> {
                    removedKeys add key
                }
            }
        }
        return result
    }

    public volatile var slotIn: Int = 0; private set
    public volatile var slotOut: Int = 0; private set

    //refactor
    private val awaitingClients = hashMapOf<ClientRequest, Int>()

    private val requests = linkedListOf<ClientRequest>()
    private val proposals = hashMapOf<Int, ClientRequest>()
    private val decisions = hashMapOf<Int, ClientRequest>()

    private val performed = hashSetOf<ClientRequest>()

    private fun perform(c: ClientRequest) {
        logPxs("PERFORMING $c at $slotOut")
        if (c in performed)
            return
        when (c) {
            is SetRequest    -> {
                state[c.key] = c.value
                val awaitingClient = awaitingClients[c]
                if (awaitingClient != null) {
                    sendToClient(awaitingClient, "STORED")
                    awaitingClients remove awaitingClient
                }
            }
            is DeleteRequest -> {
                val result = (state remove c.key) != null
                val awaitingClient = awaitingClients[c]
                if (awaitingClient != null) {
                    sendToClient(awaitingClient, if (result) "DELETED" else "NOT_FOUND")
                    awaitingClients remove awaitingClient
                }
            }
        }
        performed add c
        if (c !is GetRequest)
            saveToDisk(c)
    }

    private fun propose() {
        while (requests.isNotEmpty()) {
            val c = requests.first()
            logPxs("PROPOSING $c to $slotIn")
            if (slotIn !in decisions) {
                requests remove c
                proposals[slotIn] = c
                leaderIds.forEach { send(it, ProposeMessage(id, slotIn, c)) }
            }
            ++slotIn
        }
    }

    /**
     * Should be called from the replica's container to pass to the replica each message
     * addressed to it.
     * @param message Message that should be handled by the replica.
     */
    public fun receiveMessage(message: ReplicaMessage) {
        when (message) {
            is GetRequest      -> {
                sendToClient(message.fromId,
                             if (message.key in state)
                                 "VALUE ${message.key} ${state[message.key]}" else
                                 "NOT_FOUND")
            }
            is ClientRequest   -> {
                requests add message
                awaitingClients[message] = message.fromId
            }
            is DecisionMessage -> {
                logPxs("DECISION $message")
                val slot = message.slot
                decisions.put(slot, message.request)

                while (slotOut in decisions) {
                    val cmd = decisions[slotOut]!!
                    if (slotOut in proposals) {
                        val proposalCmd = proposals[slotOut]
                        proposals remove slotOut
                        if (cmd != proposalCmd) {
                            requests add proposalCmd
                        }
                    }
                    perform(cmd)
                    ++slotOut
                }
            }
        }
        propose()
    }
}