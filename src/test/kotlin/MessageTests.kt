/**
 * Created by Sergey on 22.05.2015.
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
        checkParseAndToString("p1a 4 5")
        checkParseAndToString("p2a 3 4 5 delete abccc")
        checkParseAndToString("p1b 345 333 1 2 get a!!b${payloadSplitter}3 4 set a b c${payloadSplitter}5 6 delete a")
        checkParseAndToString("p2b 1 3")
    }

}