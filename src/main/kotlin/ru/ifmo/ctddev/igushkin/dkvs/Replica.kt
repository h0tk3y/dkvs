package ru.ifmo.ctddev.igushkin.dkvs

import java.io.FileWriter
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
 */

public class Replica(val id: Int,
                     val send: (nodeId: Int, Message) -> Unit,
                     val sendToClient: (clientId: Int, text: String) -> Unit,
                     val leaderIds: List<Int>
) {

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

    private val state: MutableMap<String, String> = HashMap()
    //todo read saved state on initialization

    public volatile var slotIn: Int = 0; private set
    public volatile var slotOut: Int = 0; private set

    private val performed = hashSetOf<ClientRequest>()

    private val requests = hashSetOf<ClientRequest>()
    private val proposals = hashMapOf<Int, ClientRequest>()
    private val decisions = hashMapOf<Int, ClientRequest>()

    private fun perform(c: ClientRequest) {
        if (c in performed)
            return
        when (c) {
            is GetRequest    -> {
                sendToClient(c.fromId,
                             if (c.key in state)
                                 "VALUE ${c.key} ${state[c.key]}" else
                                 "NOT_FOUND")
            }
            is SetRequest    -> {
                state[c.key] = c.value
                sendToClient(c.fromId, "STORED")
            }
            is DeleteRequest -> {
                val result = (state remove c.key) != null
                sendToClient(c.fromId, if (result) "DELETED" else "NOT_FOUND")
            }
        }
        performed add c
    }

    private fun propose() {
        while (requests.isNotEmpty()) {
            val c = requests.first()
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
            is ClientRequest   -> {
                requests add message
            }
            is DecisionMessage -> {
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

public data class Proposal(val slot: Int, val command: ClientRequest)