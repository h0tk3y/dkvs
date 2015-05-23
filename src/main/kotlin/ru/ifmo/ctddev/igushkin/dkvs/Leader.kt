package ru.ifmo.ctddev.igushkin.dkvs

import java.util.HashSet
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
    public volatile var currentBallot: Ballot = Ballot(1, id); private set

    //todo reduce state
    private val proposals = hashMapOf<Int, ClientRequest>()

    public fun receiveMessage(message: LeaderMessage) {
        when (message) {
            is ProposeMessage   -> {
                if (message.slot !in proposals) {
                    proposals[message.slot] = message.request
                    if (active) {
                        command(AcceptProposal(currentBallot, message.slot, message.request))
                    }
                }
            }
            is PhaseOneResponse -> {
                val ballot = message.ballotNum
                val scout = scouts[ballot]
                scout?.receiveResonse(message)
            }
            is PhaseTwoResponse -> {
                val proposal = message.proposal
                val commander = commanders[proposal]
                commander?.receiveResponse(message)
            }
        }
    }

    private fun preempted(b: Ballot) {
        if (b > currentBallot) {
            active = false
            currentBallot = Ballot(b.ballotNum + 1, id)
            onFault = { faulty ->
                if (b.leaderId in faulty) {
                    scouting(currentBallot)
                    onFault = null
                }
            }
        }
    }

    private fun adopted(ballot: Ballot, pvalues: Map<Int, AcceptProposal>) {
        for ((slot, pval) in pvalues) {
            proposals[slot] = pval.command
        }
        active = true
        for ((s, c) in proposals) {
            command(AcceptProposal(ballot, s, c))
        }
    }

    //----- Commander -----

    inner class Commander(val proposal: AcceptProposal) {
        val waitFor = HashSet(acceptorIds)

        fun receiveResponse(response: PhaseTwoResponse) {
            if (response.ballot != currentBallot)
                preempted(response.ballot)
            else {
                waitFor remove response.fromId
                if (waitFor.isEmpty() || waitFor.size() < acceptorIds.size() / 2) {
                    replicaIds.forEach {
                        send(it, DecisionMessage(response.proposal.slot,
                                                 response.proposal.command))
                    }
                    commanders remove proposal
                }
            }
        }
    }

    private val commanders = hashMapOf<AcceptProposal, Commander>()

    private fun command(proposal: AcceptProposal) {
        commanders[proposal] = Commander(proposal)
        acceptorIds.forEach { send(it, PhaseTwoRequest(id, proposal)) }
    }

    //------ Scout ------

    inner class Scout() {
        val waitFor = HashSet(acceptorIds)
        val proposals = hashMapOf<Int, AcceptProposal>()

        fun receiveResonse(response: PhaseOneResponse) {
            if (response.ballotNum != currentBallot)
                preempted(response.ballotNum)
            else {
                response.pvalues.forEach {
                    if (it.slot !in proposals || it.ballotNum > proposals[it.slot].ballotNum)
                        proposals[it.slot] = it
                }
                waitFor remove response.fromId
                if (waitFor.size() < acceptorIds.size() / 2) {
                    adopted(currentBallot, proposals)
                    scouts remove currentBallot
                }
            }
        }
    }

    private val scouts = hashMapOf<Ballot, Scout>()

    private fun scouting(ballot: Ballot) {
        scouts[ballot] = Scout()
        acceptorIds.forEach { send(it, PhaseOneRequest(id, ballot)) }
    }

    //------ Fault detection -------

    private volatile var onFault: ((HashSet<Int>) -> Unit)? = null

    public  fun notifyFault(nodes: HashSet<Int>) {
        if (onFault != null)
            onFault!!(nodes)
    }

    public fun afterRun() {
        if (!active)
            scouting(currentBallot)
    }
}

public data class Ballot(val ballotNum: Int, val leaderId: Int) : Comparable<Ballot> {
    override fun compareTo(other: Ballot): Int {
        val result = ballotNum.compareTo(other.ballotNum)
        return if (result != 0) result else leaderId.compareTo(other.leaderId)
    }

    override fun toString() = "${ballotNum}_${leaderId}"

    companion object {
        public fun parse(s: String): Ballot = s.split('_').let { Ballot(it[0].toInt(), it[1].toInt()) }
    }
}