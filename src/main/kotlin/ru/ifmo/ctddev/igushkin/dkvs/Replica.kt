package ru.ifmo.ctddev.igushkin.dkvs

/**
 * Represents `replica` of Multi-Paxos protocol.
 *
 * For complete description, see [Paxos Made Moderately Complex]
 * [http://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf]
 *
 * @param id Replica identifier, unique across the protocol instance.
 * @param send (nodeId, message) Way to send messages to the other nodes.
 * @param leaderIds List of ids which the replica will ask for decisions.
 */

public class Replica(val id: Int,
                     val send: (Int, Message) -> Unit,
                     val leaderIds: List<Int>
) {
    //todo read saved state

    public volatile var slotIn: Int = 0; private set
    public volatile var slotOut: Int = 0; private set

    private val requests = setOf<ClientRequest>()
    private val proposals = setOf<ClientRequest>()
    private val decisions = setOf<ClientRequest>()

    /**
     * Should be called from the replica's container to pass to the replica each message
     * addressed to it.
     * @param message Message that should be handled by the replica.
     */
    public fun receiveMessage(message: ReplicaMessage) {

    }
}

public data class Proposal(val slot: Int, val command: ClientRequest)