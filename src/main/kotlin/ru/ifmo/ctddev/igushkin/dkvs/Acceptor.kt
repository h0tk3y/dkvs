package ru.ifmo.ctddev.igushkin.dkvs

/**
 * Created by Sergey on 21.05.2015.
 */

public class Acceptor(val id: Int,
                      val send: (leaderId: Int, Message) -> Unit
) {
    private volatile var ballotNumber = Ballot(-1, -1)

    /** Slot -> most recent AcceptProposal */
    private val accepted = hashMapOf<Int, AcceptProposal>()

    public fun receiveMessage(message: AcceptorMessage) {
        when (message) {
            is PhaseOneRequest -> {
                if (message.ballotNum > ballotNumber)
                    ballotNumber = message.ballotNum
                send(message.fromId, PhaseOneResponse(id, ballotNumber, accepted.values()))
            }
            is PhaseTwoRequest -> {
                if (message.ballotNum == ballotNumber)
                    accepted[message.payload.slot] = message.payload
                send(message.fromId, PhaseTwoResponse(id, ballotNumber))
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
public data class AcceptProposal(val ballotNum: Ballot, val slot: Int, val command: ClientRequest) {
    override fun toString(): String = "$ballotNum $slot $command"

    companion object {
        public fun parse(parts: Array<String>): AcceptProposal =
                AcceptProposal(Ballot.parse(parts[0]), parts[1].toInt(), ClientRequest.parse(0, parts[2..parts.lastIndex]))
    }
}