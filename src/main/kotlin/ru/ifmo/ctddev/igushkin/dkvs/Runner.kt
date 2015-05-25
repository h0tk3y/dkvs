package ru.ifmo.ctddev.igushkin.dkvs

import kotlin.concurrent.*
import kotlin.platform.platformStatic

/**
 * Created by Sergey on 23.05.2015.
 *
 * Runs a DKVS instance according to the [globalConfig]
 */

object Runner {
    platformStatic fun main(args: Array<String>) {
        if (args.size() == 0)
            for (i in globalConfig.ids) {
                thread { Node(i).run() }
            }
        else {
            val id = args[0].toInt()
            Node(id).run()
        }
    }
}