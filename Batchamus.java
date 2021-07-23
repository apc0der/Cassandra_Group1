import com.datastax.oss.driver.api.core.CqlSession;

import java.math.BigDecimal;
import java.util.*;
import java.io.*;

public class Batchamus {
    private CqlSession session;
    private String keyspace;
    private String table;

    public Batchamus(CqlSession session, String keyspace, String table){
        this.session = session;
        this.keyspace = keyspace;
        this.table = table;
    }

    public CqlSession getSession() {
        return session;
    }

    public void setSession(CqlSession session) {
        this.session = session;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String b() throws IOException{

        Runtime.getRuntime().exec("cmd /c cd %CASSANDRA_HOME%\\bin");
        //Runtime.getRuntime().exec("cmd /c cd .\\bin");
        String yee = "cmd /c call nodetool cfstats";
        if (keyspace != null && table != null) {
            yee += " " + keyspace + "." + table;
        } else if (keyspace != null && table == null){
            yee += " " + keyspace;
        } else if (keyspace == null && table == null){
            yee += "";
        }

        Process p = Runtime.getRuntime().exec(yee);
        Scanner sc = new Scanner(new InputStreamReader(p.getInputStream()));

        long keyspaceSize = 0;
        while (sc.hasNextLine()){
            String line = sc.nextLine();
            if (line.contains("live)")){
                keyspaceSize += Long.parseLong(line.substring(21));
            }
        }
        double y = (double) keyspaceSize;
        ArrayList<String> xd = new ArrayList<>();
        xd.add("B");
        xd.add("KB");
        xd.add("MB");
        xd.add("GB");
        xd.add("TB");
        xd.add("PB");
        xd.add("EB");
        xd.add("ZB");
        int d = 0;
        while(y>1)
        {
            y /= 1000;
            d++;
        }
        if(y<1)
        {
            y *= 1000;
            d--;
        }
        return String.format("%.3f %s\n", y, xd.get(d));
    }

    public List<String> bp() throws IOException{
        Runtime.getRuntime().exec("cmd /c cd %CASSANDRA_HOME%\\bin");
        //Runtime.getRuntime().exec("cmd /c cd .\\bin");

        Process p = Runtime.getRuntime().exec("cmd /c call nodetool cfstats " + keyspace + "." + table);
        Scanner sc = new Scanner(new InputStreamReader(p.getInputStream()));

        KeyspaceRepository keyspaceRepository = new KeyspaceRepository(session);

        long keyspaceSize = 0;
        while (sc.hasNextLine()){
            String line = sc.nextLine();
            if (line.contains("live)")){
                keyspaceSize += Long.parseLong(line.substring(21));
            }
        }

        List<String> pS = keyspaceRepository.getPartitionList(keyspace, table);
        Map<String, Integer> rPP = keyspaceRepository.getNumRowsByPart(keyspace, table);
        List<Double> fracs = new ArrayList<Double>();
        List<String> out = new ArrayList<String>();

        double totRow = 0;
        for (String s: pS){
            totRow += rPP.get(s);
        }
        for (String s: pS){
            fracs.add(rPP.get(s)/totRow);
        }

        double y = (double) keyspaceSize;
        ArrayList<String> xd = new ArrayList<>();
        xd.add("B");
        xd.add("KB");
        xd.add("MB");
        xd.add("GB");
        xd.add("TB");
        xd.add("PB");
        xd.add("EB");
        xd.add("ZB");

        for (int j = 0; j < fracs.size(); j++){
            double x = y*fracs.get(j);
            int d = 0;
            while(x>1)
            {
                x /= 1000;
                d++;
            }
            if(x<1)
            {
                x *= 1000;
                d--;
            }
            out.add(String.format(pS.get(j) + " ≈ %.3f %s\n", x, xd.get(d)));
        }
        return out;
    }
}
