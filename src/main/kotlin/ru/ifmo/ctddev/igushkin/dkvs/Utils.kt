package ru.ifmo.ctddev.igushkin.dkvs

import java.io.BufferedReader

/**
 * Useful utils and extensions.
 */

fun forSplittedLines(reader: BufferedReader, f: (Array<String>) -> Unit) {
    var line: String? = null
    while ({line = reader.readLine(); line != null}()) {
        f(line!!.split(' '))
    }
}

fun <T> Array<T>.get(range: IntRange) = this.copyOfRange(range.start, range.end + 1)