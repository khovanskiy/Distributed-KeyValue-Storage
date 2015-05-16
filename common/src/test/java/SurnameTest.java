import org.junit.Assert;
import org.junit.Test;

public class SurnameTest {
    /**
     * Tests variant
     *
     * @see <a href="http://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf">1 - Multi Paxos</a>
     * @see <a href="http://pmg.csail.mit.edu/papers/vr-revisited.pdf">2 - Viewstamped Replication</a>
     * @see <a href="https://ramcloud.stanford.edu/raft.pdf">3 - Raft</a>
     */
    @Test
    public void test() {
        String s = "ХОВАНСКИЙ";
        Assert.assertEquals((s.hashCode() & 0x7fffffff) % 3 + 1, 2);
    }
}
