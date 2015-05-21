package ru.ifmo.ctddev.igushkin.dkvs

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
 * @property state Replica state. Paxos support any abstract state, but
 * in DKVS the state is a key-value string map.
 *
 * @property slotIn Next slot to propose a request to.
 * @property slotOut First slot with non-applied request.
 */

public class Replica(val id: Int,
                     val send: (Int, Message) -> Unit,
                     val leaderIds: List<Int>
) {

    public val state: Map<String, String> = HashMap()
    //todo read saved state on initialization

    public volatile var slotIn: Int = 0; private set
    public volatile var slotOut: Int = 0; private set

    private val requests = hashSetOf<ClientRequest>()
    private val proposals = hashMapOf<Int, ClientRequest>()
    private val decisions = hashMapOf<Int, ClientRequest>()

    private fun perform(c: ClientRequest) {
        ++slotOut
        //todo
    }

    /**
     * Should be called from the replica's container to pass to the replica each message
     * addressed to it.
     * @param message Message that should be handled by the replica.
     */
    public fun receiveMessage(message: ReplicaMessage) {
        //todo propose requests
        when (message) {
            is ClientRequest -> {
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
                }
            }
        }
    }

}

public data class Proposal(val slot: Int, val command: ClientRequest)