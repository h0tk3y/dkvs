package ru.ifmo.ctddev.igushkin.dkvs

import java.io.BufferedReader

/**
 * Useful utils and extensions.
 */

fun BufferedReader.forEachLine(f: (String) -> Unit) = lines().forEach(f)

fun dispatch(reader: BufferedReader, f: (Array<String>) -> Unit) = reader.forEachLine { f(it.split(" ")) }

fun forever(f: () -> Unit) {
    while (true) f()
}

fun <T> Array<T>.get(range: IntRange) = this.copyOfRange(range.start, range.end + 1)