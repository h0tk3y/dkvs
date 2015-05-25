package ru.ifmo.ctddev.igushkin.dkvs

/**
 * Created by Sergey on 25.05.2015.
 */

import org.junit.Test as test
import org.junit.BeforeClass as prepare
import org.junit.AfterClass as afterall
import kotlin.platform.platformStatic
import org.junit.Assert.*
import java.io.File
import java.io.FilenameFilter
import kotlin.concurrent.thread

public class TestHighload {


    fun cleanLogs(c: Configuration) {
        for (i in c.ids) {
            File("dkvs_$i.log").delete()
        }
    }


    val keys = 10;
    val iterationsPerKey = 20;

    val config = Configuration.readDkvsProperties()

    var run = false
    private fun startup() {
        if (!run) {
            cleanLogs(config)
            Runner.main(array())
            run = true
        }
    }

    private fun runClient(toNode: Int, keys: Iterable<Int>, withSleep: Boolean = false) {
        val client = Client(config.address(toNode), config.port(toNode))
        client.connect()

        for (key in keys) {
            for (v in 1..iterationsPerKey) {
                if (client.set("$key", "$v")) {
                    val storedValue = client["$key"]
                    assertTrue(storedValue?.equals("$v") ?: true)
                }
            }
            if (withSleep)
                client.sleep()
        }
    }

    test fun singleClient() {
        startup()

        thread {
            runClient(1, 1..keys, false)
        }.join()

    }

    test fun multipleClients() {
        startup()

        val threads = Array(config.ids.size(),
                            { thread { runClient(it, keys * it..keys * (it + 1) - 1, false) } });

        threads.forEach { it.join() }
    }

    test fun withSleep() {
        startup()

        thread {
            runClient(1, 1..keys, true)
        }.join()
    }

    test fun multipleClientsHalfWithSleep() {
        startup()

        val threads = Array(config.ids.size(),
                            { thread {
                                runClient(it,
                                          keys * it..keys * (it + 1) - 1,
                                          it <= (config.ids.size()+1)/2) } });

        threads.forEach { it.join() }
    }


}