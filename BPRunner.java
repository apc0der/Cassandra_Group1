import com.datastax.oss.driver.api.core.CqlSession;

import java.io.*;
import java.util.*;

public class BPRunner {
    public static void main(String[] args) throws IOException {
        CassandraConnector connector = new CassandraConnector();
        connector.connect("127.0.0.1", 9042, "datacenter1");
        CqlSession session = connector.getSession();

        Runtime.getRuntime().exec("cmd /c cd C:\\Cassandra\\apache-cassandra-3.11.10\\bin");
        Process p = Runtime.getRuntime().exec("cmd /c call nodetool cfstats testkeyspace.videos");
        Scanner sc = new Scanner(new InputStreamReader(p.getInputStream()));
        BatchamusPart bpaaxe = new BatchamusPart(session);
        List<String> bpOut = bpaaxe.bp(sc);
        for (String s: bpOut){
            System.out.print(s);
        }
        connector.close();
    }
}
