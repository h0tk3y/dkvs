package ru.ifmo.ctddev.igushkin.dkvs

/**
 * Messages between [Node]s and Clients.
 */
public abstract class Message() {
    companion object {
        public fun parse(parts: Array<String>): Message =
                when (parts[0]) {
                    "node" -> NodeMessage(parts[1].toInt())
                    "ping" -> PingMessage()
                    "pong" -> PongMessage()
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

/**
 * Sub-hierarchy of messages addressed to [Replica]s.
 */
public abstract class ReplicaMessage(val fromId: Int): Message()

/**
 * Sub-hierarchy of client requests.
 * These represent certain application (DKVS) and not Paxos itself.
 *
 * Client messages are only received and dispatched to replicas and are never sent,
 * thus they don't need toString().
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

public class SetRequest(fromId: Int, val key: String, val value: String): ClientRequest(fromId)
public class GetRequest(fromId: Int, val key: String): ClientRequest(fromId)
public class DeleteRequest(fromId: Int, val key: String): ClientRequest(fromId)
