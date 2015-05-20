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

fun joined(items: Collection<Any>, separator: String = ", "): String = StringBuilder {
    append(items.first())
    for (i in items.drop(1)) {
        append(separator)
        append(i)
    }
}.toString()