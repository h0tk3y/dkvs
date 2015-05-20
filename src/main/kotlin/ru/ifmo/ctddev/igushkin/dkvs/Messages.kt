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
                    "get" -> GetRequest(parts[1].toInt(), parts[2])
                    "set" -> SetRequest(parts[1].toInt(), parts[2], joined(parts.drop(3), " "))
                    "delete" -> DeleteRequest(parts[1].toInt(), parts[2])
                    else   -> throw IllegalArgumentException("Unknown message.")
                }
    }
}

val FROM_UNKNOWN = -1

/**
 * Message which is sent first in order to establish connection between [Node]s.
 */
public class NodeMessage(val fromId: Int) : Message() {
    override fun toString() = "node $fromId"
}

/**
 * Request for checking connectivity between nodes.
 * [PongMessage] is the right response.
 */
public class PingMessage(): Message() {
    override fun toString() = "ping"
}

/**
 * Response for [PingMessage] which shows connectivity
 */
public class PongMessage(): Message() {
    override fun toString() = "pong"
}

/**
 * Sub-hierarchy of messages addressed to [Replica]s.
 */
public abstract class ReplicaMessage(val fromId: Int): Message()

/**
 * Sub-hierarchy of client requests
 * These represent certain application (DKVS) and not Paxos itself.
 */
public abstract class ClientRequest(clientId: Int): ReplicaMessage(clientId)

public data class GetRequest(fromId: Int, val key: String): ClientRequest(fromId) {
    override fun toString() = "get $fromId $key"
}

public data class SetRequest(fromId: Int, val key: String, val value: String): ClientRequest(fromId) {
    override fun toString() = "set $fromId $key $value"
}

public data class DeleteRequest(fromId: Int, val key: String): ClientRequest(fromId) {
    override fun toString() = "delete $fromId $key"
}
