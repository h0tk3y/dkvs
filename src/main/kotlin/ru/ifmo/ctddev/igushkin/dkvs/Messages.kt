package ru.ifmo.ctddev.igushkin.dkvs

/**
 * Messages between [Node]s and Clients.
 *
 * Overriding of [Any.toString] is used to serialize messages into
 * string representation and send them.
 */
public abstract class Message() {
    companion object {
        public fun parse(parts: Array<String>): Message =
                when (parts[0]) {
                    "node" -> NodeMessage(parts[1].toInt())
                    "ping" -> PingMessage()
                    "pong" -> PongMessage()

                    "p1a" -> PhaseOneRequest(parts[1].toInt(), parts[2].toInt())
                    "p2a" -> PhaseTwoRequest(parts[1].toInt(), parts[2].toInt(), AcceptProposal.parse(parts[3..parts.lastIndex]))

                    "get", "set", "delete" -> throw IllegalArgumentException("Use ClientRequest.parse(id, parts) instead.")
                    else   -> throw IllegalArgumentException("Unknown message.")
                }
    }
}

/**
 * Message which is sent first in order to establish connection between [Node]s.
 */
public class NodeMessage(val fromId: Int) : Message() {
    override fun toString() = "node $fromId"
}

/**
 * Request for checking connectivity between [Node]s.
 * [PongMessage] is the right response.
 */
public class PingMessage(): Message() {
    override fun toString() = "ping"
}

/**
 * Response for [PingMessage] which shows positive connectivity.
 */
public class PongMessage(): Message() {
    override fun toString() = "pong"
}

//----- Replica messages -----

/**
 * Sub-hierarchy of messages addressed to [Replica]s.
 */
public abstract class ReplicaMessage(val fromId: Int): Message()

/**
 * Sub-hierarchy of client requests.
 * These represent certain application (DKVS) and not Paxos itself.
 *
 * Client messages are only received and dispatched to replicas and are never sent
 * themselves but can still be sent as payload.
 *
 * @param clientId Node-local client id.
 */
public abstract class ClientRequest(clientId: Int): ReplicaMessage(clientId) {
    companion object {
        public fun parse(clientId: Int, parts: Array<String>): ClientRequest =
                when (parts[0]) {
                    "get" -> GetRequest(clientId, parts[1])
                    "set" -> SetRequest(clientId, parts[1], joined(parts.drop(2), " "))
                    "delete" -> DeleteRequest(clientId, parts[1])
                    else -> throw IllegalArgumentException("Invalid client request ${parts[0]}.")
                }
    }
}

public class GetRequest(fromId: Int, val key: String): ClientRequest(fromId) {
    override fun toString() = "get $key"
}
public class SetRequest(fromId: Int, val key: String, val value: String): ClientRequest(fromId) {
    override fun toString() = "set $key $value"
}
public class DeleteRequest(fromId: Int, val key: String): ClientRequest(fromId) {
    override fun toString() = "delete $key"
}

//----- Leader messages -----

public abstract class LeaderMessage(val fromId: Int): Message()

//----- Acceptor messages -----

public abstract class AcceptorMessage(val fromId: Int, val ballotNum: Int): Message()

public class PhaseOneRequest(fromId: Int, ballotNum: Int): AcceptorMessage(fromId, ballotNum) {
    override fun toString() = "p1a $fromId $ballotNum"
}

public class PhaseTwoRequest(fromId: Int, ballotNum: Int, val payload: AcceptProposal): AcceptorMessage(fromId, ballotNum) {
    override fun toString() = "p2a $fromId $payload"
}