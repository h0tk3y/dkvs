package ru.ifmo.ctddev.igushkin.dkvs

/**
 * Created by Sergey on 21.05.2015.
 */

public class Leader(val id: Int,
                    val send: (Int, Message) -> Unit,
                    val replicaIds: List<Int>,
                    val acceptorIds: List<Int>
) {
    public volatile var active: Boolean = false; private set
    public volatile var ballot: Ballot = Ballot(-1, id); private set

    public fun receiveMessage(message: LeaderMessage) {

    }
}

public data class Ballot(val ballotNum: Int, val leaderId: Int): Comparable<Ballot> {
    override fun compareTo(other: Ballot): Int {
        val result = ballotNum.compareTo(other.ballotNum)
        return if (result != 0) result else leaderId.compareTo(other.leaderId)
    }

    public fun receiveMessage(message: LeaderMessage) {

    }
}