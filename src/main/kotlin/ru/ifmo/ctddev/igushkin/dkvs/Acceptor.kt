package ru.ifmo.ctddev.igushkin.dkvs

/**
 * Created by Sergey on 21.05.2015.
 */

public class Acceptor(val id: Int,
                      val send: (Int, Message) -> Unit
) {
    private volatile var ballotNumber = -1
    private val accepted = setOf<AcceptProposal>()

    public fun receiveMessage(message: AcceptorMessage) {
        when (message) {
            is PhaseOneRequest -> {
                if (message.ballotNum > ballotNumber)
                    ballotNumber = message.ballotNum
                //todo response
            }
            is PhaseTwoRequest -> {

            }
        }
    }
}

public data class AcceptProposal(val ballotNum: Int, val slot: Int, val command: ClientRequest) {
    override fun toString(): String = "$ballotNum $slot $command"

    companion object {
        public fun parse(parts: Array<String>): AcceptProposal =
                AcceptProposal(parts[0].toInt(), parts[1].toInt(), ClientRequest.parse(0, parts[2..parts.lastIndex]))
    }
}