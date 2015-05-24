package ru.ifmo.ctddev.igushkin.dkvs

import java.io.File
import java.io.FileWriter
import java.util.ArrayList
import java.util.HashMap
import kotlin.properties.Delegates

/**
 * Holds the disk storage for node.
 *
 * Created by Sergey on 24.05.2015.
 */

public class Persistence(val nodeId: Int) {

    public val fileName: String = "dkvs_$nodeId.log"
    private val writer = FileWriter(fileName, true).buffered()

    public volatile var lastBallotNum: Int = 0; private set
    public volatile var keyValueStorage: HashMap<String, String>? = null

    public fun nextBallotNum(): Int {
        return ++lastBallotNum
    }

    init {
        val file = File(fileName)
        if (file.exists()) {
            val reader = file.reader().buffered()
            val lines = ArrayList<String>()
            for (l in reader.lines())
                lines add l

            val storage = hashMapOf<String, String>()
            val removedKeys = hashSetOf<String>()

            for (l in lines.reverse()) {
                val parts = l.split(' ')
                val key = if (1 in parts.indices) parts[1] else null
                when (parts[0]) {
                    "set"    -> {
                        storage[key] = parts[2..parts.lastIndex].join(" ")
                    }
                    "delete" -> {
                        removedKeys add key
                    }
                }
            }

            keyValueStorage = storage

            for (l in lines.reverse()) {
                val parts = l.split(' ')
                if (parts[0] == "ballot") {
                    lastBallotNum = Ballot.parse(parts[1]).ballotNum
                    break
                }
            }
        }
    }

    public fun saveToDisk(s: String) {
        with(writer) {
            write(s)
            newLine()
            flush()
        }
    }
}