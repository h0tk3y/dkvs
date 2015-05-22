package ru.ifmo.ctddev.igushkin.dkvs

import kotlin.test.todo

/**
 * Created by Sergey on 21.05.2015.
 */

fun isInitialLeader(l: Leader) = l.id == 1

public class Leader(val id: Int,
                    val send: (Int, Message) -> Unit,
                    val replicaIds: List<Int>,
                    val acceptorIds: List<Int>
) {
    public volatile var active: Boolean = isInitialLeader(this); private set
    public volatile var ballot: Ballot = Ballot(-1, id); private set

    private val proposals = hashMapOf<Int, ClientRequest>()

    public fun receiveMessage(message: LeaderMessage) {
        when (message) {
            is ProposeMessage -> {
                if (message.slot !in proposals) {
                    proposals[message.slot] = message.request
                    if (active) {
                        command(AcceptProposal(ballot, message.slot, message.request))
                    }
                }
            }
            is PhaseOneResponse -> {
                //todo
            }
            is PhaseTwoResponse -> {
                //todo
            }
        }
    }

    private fun command(proposal: AcceptProposal) {
        //todo
    }
}

public data class Ballot(val ballotNum: Int, val leaderId: Int): Comparable<Ballot> {
    override fun compareTo(other: Ballot): Int {
        val result = ballotNum.compareTo(other.ballotNum)
        return if (result != 0) result else leaderId.compareTo(other.leaderId)
    }

    override fun toString() = "${ballotNum}_${leaderId}"

    companion object {
        public fun parse(s: String): Ballot = s.split('_').let { Ballot(it[0].toInt(), it[1].toInt()) }
    }
}