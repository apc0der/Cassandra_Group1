import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class ToCSV {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("What node would you like to connect to?");
        String node = br.readLine();
        System.out.println("What port would you like to connect to?");
        int portNum = -1;
        boolean portsuccess = false;
        do {
            try {
                String port = br.readLine();
                portNum = Integer.parseInt(port);
                portsuccess = true;
            } catch (Exception e) {
                System.out.println("Enter a valid port, baka");
            }
        } while (!portsuccess);
        System.out.println("What is the name of the datacenter you wish to connect to?");
        String dataCenter = br.readLine();
        CassandraConnector connector = new CassandraConnector();
        //try {
            connector.connect(node, portNum, dataCenter);
            PrintWriter pw = new PrintWriter("table_data.csv");
            pw.println("Keyspace Name\tTable Name\tNum Partitions\tPartition Row Stats\tColumn Definitions\tTable Size\tPartition Size Stats");
            KeyspaceRepository kR = new KeyspaceRepository(connector.getSession());
            //Batchamus bababoi = new Batchamus(connector.getSession(), null, null);
            List<String> keyspaces = kR.getKeyspaceList();
            boolean systemkeyspaces = false;
            if (!systemkeyspaces) {
                keyspaces.remove("system_auth");
                keyspaces.remove("system_schema");
                keyspaces.remove("system_distributed");
                keyspaces.remove("system");
                keyspaces.remove("system_traces");
            }
            Map<String, String> tS = kR.getTableSizes();
            for (String keyspace : keyspaces) {
                //bababoi.setKeyspace(keyspace);
                List<String> tables = kR.getTableList(keyspace);
                for (String table : tables) {
                    //bababoi.setTable(table);
                    //Some threading stuff
                /*KeyThread thread = new KeyThread(kR, pw,keyspace,table);
                thread.start();*/
                    List<String> pL = kR.getPartitionList(keyspace, table);
                    Map<String, Integer> rPP = kR.getRowsPerPartition(keyspace, table);
                    String colDefs = kR.getColDefs(keyspace, table);
                    pw.println(keyspace + "\t" + table + "\t" + pL.size() + "\t" + kR.statsTable(rPP)
                            + "\t" + colDefs + "\t" + tS.get(keyspace+"."+table) + "\t" + kR.statsPart(rPP, tS.get(keyspace+"."+table)));
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
        /*} catch (Exception e) {
            connector.close();
            System.out.println("You screwed up! Try again bro.");
        }*/
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
