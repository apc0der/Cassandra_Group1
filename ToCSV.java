import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class ToCSV {
    public static void main(String[] args) throws IOException {
        CassandraConnector connector = new CassandraConnector();
        connector.connect("127.0.0.1", 9042, "datacenter1");
        PrintWriter pw = new PrintWriter("testfile.csv");
        pw.println("Keyspace Name\tTable Name\tNum Partitions\tNum Rows Per Partition\tColumn Definitions\tTable Size\tEstimated Size of Partitions");
        KeyspaceRepository kR = new KeyspaceRepository(connector.getSession());
        //Batchamus bababoi = new Batchamus(connector.getSession(), null, null);
        List<String> keyspaces = kR.getKeyspaceList();
        boolean systemkeyspaces = false;
        if(!systemkeyspaces)
        {
            keyspaces.remove("system_auth");
            keyspaces.remove("system_schema");
            keyspaces.remove("system_distributed");
            keyspaces.remove("system");
            keyspaces.remove("system_traces");
        }
        for (String keyspace : keyspaces)
        {
            //bababoi.setKeyspace(keyspace);
            List<String> tables = kR.getTableList(keyspace);
            for(String table: tables)
            {
                //bababoi.setTable(table);
                //Some threading stuff
                /*KeyThread thread = new KeyThread(kR, pw,keyspace,table);
                thread.start();*/
                pw.println(keyspace + "\t" + table + "\t" + kR.getPartitionList(keyspace, table).size() + "\t" + kR.getRowsPerPartition(keyspace, table)
                        + "\t" + kR.getColDefs(keyspace, table));
                /*
                pw.print("\t" + bababoi.b() + "\t");
                List<String> partitionSizes = bababoi.bp(kR.getPartitionList(keyspace, table), kR.getRowsPerPartition(keyspace, table));
                for(int i = 0; i < partitionSizes.size(); i++)
                {
                    pw.print(partitionSizes.get(i));
                    if(i!=partitionSizes.size()-1)
                    {
                        pw.print(", ");
                    }
                }
                pw.println();

                 */
            }
        }
        pw.close();
        connector.close();
    }
}
class KeyThread extends Thread {
    KeyspaceRepository kR;
    PrintWriter pw;
    String keyspace, table;
    public KeyThread(KeyspaceRepository a, PrintWriter pw, String keyspace, String table)
    {
        this.kR = a;
        this.pw = pw;
        this.keyspace = keyspace;
        this.table = table;
    }
    @Override
    public void run() {
        synchronized (kR) {
            pw.print(keyspace + "\t" + table + "\t" + kR.getPartitionList(keyspace, table).size() + "\t" + kR.getRowsPerPartition(keyspace, table)
                    + "\t" + kR.getColDefs(keyspace, table));
            System.out.println("Boi");
        }
    }
}
