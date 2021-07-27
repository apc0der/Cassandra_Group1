import com.datastax.oss.driver.api.core.CqlSession;

import java.math.BigDecimal;
import java.util.*;
import java.io.*;

public class Batchamus {

    private String keyspace;
    private String table;

    public Batchamus(String keyspace, String table){

        this.keyspace = keyspace;
        this.table = table;
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

    public String cSize() throws IOException{

        Runtime.getRuntime().exec("cmd /c cd %CASSANDRA_HOME%\\bin");
        //Runtime.getRuntime().exec("cmd /c cd .\\bin");
        String yee = "cmd /c call nodetool cfstats";
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
        return String.format("%.3f %s", y, xd.get(d));
    }

    public ArrayList<String> pSize(List<String> pS, Map<String, Integer> rPP) throws IOException{
        Runtime.getRuntime().exec("cmd /c cd %CASSANDRA_HOME%\\bin");
        //Runtime.getRuntime().exec("cmd /c cd .\\bin");

        Process p = Runtime.getRuntime().exec("cmd /c call nodetool cfstats " + keyspace + "." + table);
        Scanner sc = new Scanner(new InputStreamReader(p.getInputStream()));

        long keyspaceSize = 0;
        while (sc.hasNextLine()){
            String line = sc.nextLine();
            if (line.contains("live)")){
                keyspaceSize += Long.parseLong(line.substring(21));
            }
        }


        List<Double> fracs = new ArrayList<Double>();
        fracs.add(1.0);
        ArrayList<String> out = new ArrayList<String>();

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

        for (int j = 0; j < fracs.size(); j++) {
            double x = y * fracs.get(j);
            int d = 0;
            while (x > 1) {
                x /= 1000;
                d++;
            }
            if (d > 0 && x < 1) {
                x *= 1000;
                d--;
            }
            out.add((j == 0) ? String.format("tot" + " = %.3f %s", x, xd.get(d)) : String.format(pS.get(j - 1) + " = %.3f %s", x, xd.get(d)));
        }
        return out;
    }
    public List<String> getTableVPart(ArrayList<String> tableSizes, boolean total){
        if (total){
            List<String> output = new ArrayList<String>();
            output.add(tableSizes.get(0));
            return output;
        } else {
            ArrayList<String> output = (ArrayList<String>) tableSizes.clone();
            output.remove(0);
            return output;
        }
    }
}
