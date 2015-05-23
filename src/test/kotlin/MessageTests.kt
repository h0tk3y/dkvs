/**
 * Created by Sergey on 22.05.2015.
 *
 * Tests for messages toString & parsing.
 */

import org.junit.Test as test
import org.junit.Assert.*
import ru.ifmo.ctddev.igushkin.dkvs.Message
import ru.ifmo.ctddev.igushkin.dkvs.payloadSplitter

public class MessageTests {

    private fun checkParseAndToString(message: String) {
        assertEquals(message, Message.parse(message).toString())
    }

    test fun testParsing() {
        checkParseAndToString("node 5")
        checkParseAndToString("ping")
        checkParseAndToString("pong")
        checkParseAndToString("decision 5 get abcAbc")
        checkParseAndToString("propose 12 34 set aaa aaaaa  aa  aa")
        checkParseAndToString("p1a 4 5_555")
        checkParseAndToString("p2a 3 4_123 5 delete abccc")
        checkParseAndToString("p1b 345 333_15 23_4 1_2 2 get a!!b${payloadSplitter}3_123 4 set a b c${payloadSplitter}5_555 6 delete a")
        checkParseAndToString("p2b 1 3_333 1_1 1 get a")
    }

}