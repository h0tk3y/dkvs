package ru.ifmo.ctddev.igushkin.dkvs

/**
 * Created by Sergey on 18.05.2015.
 */

public open class Message(val fromId: Int)

/**
 * It means, literally:
 * "Hello, I am node.[fromId]."
 */
public class HelloMessage(fromId: Int): Message(fromId) {
    override fun toString(): String {
        return "node $fromId"
    }
}