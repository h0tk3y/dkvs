package ru.ifmo.ctddev.igushkin.dkvs

import java.util.Collections
import java.util.Properties
import kotlin.properties.Delegates

/**
 * Stores nodes configuration which consists of:
 * addresses: mapping of node ids to network addresses on which the nodes can be found
 * timeout: idleness limit for nodes communication
 */

public data class Config(val addresses: Map<Int, String>,
                         val timeout: Long
) {
    public fun port(id: Int): Int {
        if (id !in addresses)
            return -1
        val parts = addresses[id]!!.split(":")
        return parts[1].toInt()
    }

    public fun address(id: Int): String? {
        if (id !in addresses)
            return null
        val parts = addresses[id]!!.split(":")
        return parts[0]
    }

    public val nodesCount: Int
        get() = addresses.size()

    public val ids: List<Int>
        get() = (0..nodesCount-1).toList()
}

val globalConfig by Delegates.lazy { readDkvsProperties() }
val CHARSET = "UTF-8"

public fun readDkvsProperties(): Config {

    val CONFIG_PROPERTIES_NAME = "dkvs.properties"
    val NODE_ADDRESS_PREFIX = "node"

    val input = javaClass<Config>().getClassLoader().getResourceAsStream(CONFIG_PROPERTIES_NAME)
    val props = Properties()
    props.load(input)

    val timeout = (props["timeout"] as String).toLong()
    val addresses = hashMapOf<Int, String>()
    for ((k, v) in props.entrySet()) {
        if (k !is String || v !is String)
            continue
        if (k.startsWith(NODE_ADDRESS_PREFIX)) {
            val parts = k.split("\\.")
            val id = parts[1].toInt()
            addresses[id] = v
        }
    }

    return Config(Collections.unmodifiableMap(addresses), timeout)
}