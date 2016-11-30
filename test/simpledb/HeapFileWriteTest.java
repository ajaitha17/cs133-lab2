package simpledb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import junit.framework.JUnit4TestAdapter;

public class HeapFileWriteTest extends TestUtil.CreateHeapFile {
    private TransactionId tid;

    /**
     * Set up initial resources for each unit test.
     */
    @Before public void setUp() throws Exception {
        super.setUp();
        tid = new TransactionId();
    }

    @After public void tearDown() throws Exception {
        Database.getBufferPool().transactionComplete(tid);
    }

    /**
     * Unit test for HeapFile.addTuple()
     */
    @Test public void addTuple() throws Exception {
        // we should be able to add 504 tuples on an empty page.
        for (int i = 0; i < 504; ++i) {
            empty.insertTuple(tid, Utility.getHeapTuple(i, 2));
            assertEquals(1, empty.numPages());
        }

        // the next 512 additions should live on a new page
        // System.out.println("Trying this now");
        for (int i = 0; i < 504; ++i) {
            // System.out.println(i);
            empty.insertTuple(tid, Utility.getHeapTuple(i, 2));
            assertEquals(2, empty.numPages());
        }

        // and one more, just for fun...
        // System.out.println("Here");
        empty.insertTuple(tid, Utility.getHeapTuple(0, 2));
        assertEquals(3, empty.numPages());
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(HeapFileWriteTest.class);
    }
}

