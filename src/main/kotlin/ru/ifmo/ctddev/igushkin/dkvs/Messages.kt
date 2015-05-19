package ru.ifmo.ctddev.igushkin.dkvs

/**
 * Messages between [Node]s and Clients.
 */

public open class Message(val fromId: Int)

/**
 * It means, literally:
 * "Hello, I am node.[fromId]."
 */
public class NodeMessage(fromId: Int): Message(fromId) {
    override fun toString() = "node $fromId"
}