package ru.ifmo.ctddev.igushkin.dkvs

/**
 * Represents [Acceptor] part of Multi-Paxos protocol.
 *
 * Created by Sergey on 21.05.2015.
 */

public class Acceptor(val id: Int,
                      val send: (leaderId: Int, Message) -> Unit,
                      val persistence: Persistence

) {
    private volatile var ballotNumber = Ballot(persistence.lastBallotNum, globalConfig.ids.first())

    /** Slot -> most recent AcceptProposal */
    private val accepted = hashMapOf<Int, AcceptProposal>()

    public fun receiveMessage(message: AcceptorMessage) {
        when (message) {
            is PhaseOneRequest -> {
                if (message.ballotNum > ballotNumber) {
                    ballotNumber = message.ballotNum
                    NodeLogger.logProtocol("ACCEPTOR ADOPTED $ballotNumber")
                }
                send(message.fromId, PhaseOneResponse(id, message.ballotNum, ballotNumber, accepted.values()))
            }
            is PhaseTwoRequest -> {
                if (message.ballotNum == ballotNumber)
                    accepted[message.payload.slot] = message.payload
                send(message.fromId, PhaseTwoResponse(id, ballotNumber, message.payload))
            }
        }
    }
}

/**
 * Represents pvalue <b, s, c> of Multi-Paxos protocol.
 *
 * See [Paxos Made Moderately Complex]
 * [http://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf]
 */
public data class AcceptProposal(val ballotNum: Ballot, val slot: Int, val command: OperationDescriptor) {
    override fun toString(): String = "$ballotNum $slot $command"

    companion object {
        public fun parse(parts: Array<String>): AcceptProposal =
                AcceptProposal(Ballot.parse(parts[0]), parts[1].toInt(), OperationDescriptor.parse(parts[2..parts.lastIndex]))
    }
}