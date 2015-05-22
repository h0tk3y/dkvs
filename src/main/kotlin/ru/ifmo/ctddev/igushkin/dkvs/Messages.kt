package ru.ifmo.ctddev.igushkin.dkvs

import java.util.HashSet
import java.util.LinkedHashMap
import java.util.LinkedHashSet

/**
 * Messages between [Node]s and Clients.
 *
 * Overriding of [Any.toString] is used to serialize messages into
 * string representation and send them.
 */
public abstract class Message() {
    companion object {
        public fun parse(s: String): Message = parse(s.split(' '))

        public fun parse(parts: Array<String>): Message =
                when (parts[0]) {
                    "node" -> NodeMessage(parts[1].toInt())
                    "ping" -> PingMessage()
                    "pong" -> PongMessage()

                    "decision" -> DecisionMessage(parts[1].toInt(), ClientRequest.parse(-1, parts[2..parts.lastIndex]))

                    "propose" -> ProposeMessage(parts[1].toInt(), parts[2].toInt(), ClientRequest.parse(-1, parts[3..parts.lastIndex]))

                    "p1a" -> PhaseOneRequest(parts[1].toInt(), Ballot.parse(parts[2]))
                    "p2a" -> PhaseTwoRequest(parts[1].toInt(), AcceptProposal.parse(parts[2..parts.lastIndex]))

                    "p1b" -> PhaseOneResponse.parse(parts)
                    "p2b" -> PhaseTwoResponse(parts[1].toInt(), Ballot.parse(parts[2]))

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

public class DecisionMessage(val slot: Int, val request: ClientRequest): ReplicaMessage(-1) {
    override fun toString() = "decision $slot $request"
}

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
                    "set" -> SetRequest(clientId, parts[1], parts.drop(2).join(" "))
                    "delete" -> DeleteRequest(clientId, parts[1])
                    "ping" -> PingRequest(clientId)
                    else -> throw IllegalArgumentException("Invalid client request ${parts[0]}.")
                }
    }
}

public data class GetRequest(fromId: Int, val key: String): ClientRequest(fromId) {
    override fun toString() = "get $key"
}
public data class SetRequest(fromId: Int, val key: String, val value: String): ClientRequest(fromId) {
    override fun toString() = "set $key $value"
}
public data class DeleteRequest(fromId: Int, val key: String): ClientRequest(fromId) {
    override fun toString() = "delete $key"
}
//refactor Join with [PingMessage] may be?
public data class PingRequest(fromId: Int): ClientRequest(fromId)

//----- Leader messages -----

public abstract class LeaderMessage(val fromId: Int): Message()

public class ProposeMessage(fromId: Int, val slot: Int, val request: ClientRequest): LeaderMessage(fromId) {
    override fun toString() = "propose $fromId $slot $request"
}

/**
 * Sent to [Scout] from [Acceptor] in response to [PhaseOneRequest].
 */
public class PhaseOneResponse(fromId: Int, val ballotNum: Ballot, val pvalues: Collection<AcceptProposal>): LeaderMessage(fromId) {
    override fun toString() = "p1b $fromId $ballotNum ${pvalues.joinToString(payloadSplitter)}"

    companion object { fun parse(parts: Array<String>): PhaseOneResponse {
        if (parts[0] != "p1b") throw IllegalArgumentException("PhaseOneResponse should start by \"p1b\"")
        val fromId = parts[1].toInt()
        val ballotNum = Ballot.parse(parts[2])
        val pvalues = parts[3..parts.lastIndex].join(" ").split("$payloadSplitter") map {it.split(' ')} map { AcceptProposal.parse(it) }
        return PhaseOneResponse(fromId, ballotNum, LinkedHashSet(pvalues))
    }}
}

val payloadSplitter = " _#_ "

/**
 * Sent to [Scout] from [Acceptor] in response to [PhaseTwoRequest].
 */
public class PhaseTwoResponse(fromId: Int, val ballotNum: Ballot): LeaderMessage(fromId) {
    override fun toString() = "p2b $fromId $ballotNum"
}

//----- Acceptor messages -----

/**
 * Sub-hierarchy of messages sent to [Acceptor]s.
 */
public abstract class AcceptorMessage(val fromId: Int, val ballotNum: Ballot): Message()

/**
 * Sent by [Scout] to [Acceptor].
 * Normal response is [PhaseOneResponse].
 */
public class PhaseOneRequest(fromId: Int, ballotNum: Ballot): AcceptorMessage(fromId, ballotNum) {
    override fun toString() = "p1a $fromId $ballotNum"
}

/**
 * Sent by active [Leader] to [Acceptor].
 * Normal response is [PhaseTwoResponse].
 */
public class PhaseTwoRequest(fromId: Int, val payload: AcceptProposal): AcceptorMessage(fromId, payload.ballotNum) {
    override fun toString() = "p2a $fromId $payload"
}

/**
 * Never received by [Node]s.
 * The only usage is for sending responses to the Clients.s
 */
public class TextMessage(val text: String): Message() {
    override fun toString() = text
}