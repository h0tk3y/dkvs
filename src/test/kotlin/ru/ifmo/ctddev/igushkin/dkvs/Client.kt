package ru.ifmo.ctddev.igushkin.dkvs

import java.io.BufferedReader
import java.io.OutputStreamWriter
import java.io.PrintWriter
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketException
import kotlin.properties.Delegates

/**
 * Created by Sergey on 25.05.2015.
 */

val timeout = 10000

public class Client(val address: String, val port: Int) {

    private var socket: Socket by Delegates.notNull()
    private var reader: BufferedReader by Delegates.notNull()
    private var writer: OutputStreamWriter by Delegates.notNull()

    private val endpoint = InetSocketAddress(address, port)

    public synchronized fun connect() {
        socket = Socket()
        socket.setSoTimeout(timeout)
        socket.connect(endpoint)
        reader = socket.getInputStream().reader(Configuration.charset).buffered()
        writer = socket.getOutputStream().writer(Configuration.charset)
    }

    public synchronized fun disconnect() {
        socket.close()
        socket = Socket()
    }

    public synchronized fun get(key: String): String? {
        try {
            writer.write("get $key\n")
            writer.flush()
            val responseParts = reader.readLine().split(' ')
            return when (responseParts[0]) {
                "NOT_FOUND" -> null
                "VALUE"     -> responseParts.copyOfRange(2, responseParts.size()).join(" ")
                else        -> throw RuntimeException("Incorrect response: ${responseParts.join(" ")}");
            }
        } catch (e: SocketException) {
            return null
        }
    }

    public synchronized fun set(key: String, value: String): Boolean {
        try {
            writer.write("set $key $value\n")
            writer.flush()
            val response = reader.readLine()
            return response == "STORED"
        } catch (e: SocketException) {
            return false
        }
    }

    public synchronized fun delete(key: String, value: String): Boolean {
        try {
            writer.write("delete $key\n")
            writer.flush()
            val response = reader.readLine()
            return response == "DELETED"
        } catch (e: SocketException) {
            return false
        }
    }

    public synchronized fun sleep() {
        try {
            writer.write("sleep 8000\n")
            writer.flush()
        } catch (e: SocketException) {
        }
    }

}