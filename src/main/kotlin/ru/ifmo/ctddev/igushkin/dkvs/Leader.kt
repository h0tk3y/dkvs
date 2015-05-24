package ru.ifmo.ctddev.igushkin.dkvs

import java.util.HashMap
import java.util.HashSet
import kotlin.test.todo

/**
 * Created by Sergey on 21.05.2015.
 */

public class Leader(val id: Int,
                    val send: (Int, Message) -> Unit,
                    val replicaIds: List<Int>,
                    val acceptorIds: List<Int>,
                    val persistence: Persistence
) {
    public volatile var active: Boolean = true; private set
    public volatile var currentBallot: Ballot = Ballot(persistence.lastBallotNum, id);
        private set

    /**
     * Should be called by a [Leader]'s container, when everything else is ready.
     */
    public fun afterRun() {
        scouting(currentBallot)
    }

    //todo Garbage collection haven't been implemented yet
    private val proposals = HashMap<Int, OperationDescriptor>()

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
                val ballot = message.originalBallot
                val scout = scouts[ballot]
                scout?.receiveResponse(message)
            }
            is PhaseTwoResponse -> {
                val proposal = message.proposal
                val commander = commanders[proposal]
                commander?.receiveResponse(message)
            }
        }
    }

    private fun preempted(b: Ballot) {
        NodeLogger.logProtocol("PREEMPTED: there's ballot $b")
        if (b > currentBallot) {
            active = false
            currentBallot = Ballot(persistence.nextBallotNum(), id)
            persistence.saveToDisk("ballot $currentBallot")
            NodeLogger.logProtocol("WAITING for ${b.leaderId} to fail")
            onFault = { faulty ->
                if (b.leaderId in faulty) {
                    NodeLogger.logProtocol("SCOUT started for ballot $currentBallot")
                    scouting(currentBallot)
                    onFault = null
                }
            }
        }
    }

    private fun adopted(ballot: Ballot, pvalues: Map<Int, AcceptProposal>) {
        NodeLogger.logProtocol("ADOPTED with ballot $ballot")
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
            if (response.ballot != proposal.ballotNum) {
                preempted(response.ballot)
                commanders remove proposal
            } else {
                waitFor remove response.fromId
                if (waitFor.size() < (acceptorIds.size()+1) / 2) {
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
        NodeLogger.logProtocol("COMMANDER started for $proposal")
        commanders[proposal] = Commander(proposal)
        acceptorIds.forEach { send(it, PhaseTwoRequest(id, proposal)) }
    }

    //------ Scout ------

    inner class Scout(val b: Ballot) {
        val waitFor = HashSet(acceptorIds)
        val proposals = hashMapOf<Int, AcceptProposal>()

        fun receiveResponse(response: PhaseOneResponse) {
            if (response.ballotNum != b) {
                scouts remove b
                preempted(response.ballotNum)
            }
            else {
                response.pvalues.forEach {
                    if (it.slot !in proposals || it.ballotNum > proposals[it.slot].ballotNum)
                        proposals[it.slot] = it
                }
                waitFor remove response.fromId
                if (waitFor.size() < (acceptorIds.size()+1) / 2) {
                    scouts remove b
                    adopted(b, proposals)
                }
            }
        }
    }

    private val scouts = hashMapOf<Ballot, Scout>()

    private fun scouting(ballot: Ballot) {
        scouts[ballot] = Scout(currentBallot)
        acceptorIds.forEach { send(it, PhaseOneRequest(id, ballot)) }
    }

    //------ Fault detection -------

    private volatile var onFault: ((HashSet<Int>) -> Unit)? = null

    public  fun notifyFault(nodes: HashSet<Int>) {
        onFault?.invoke(nodes)
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