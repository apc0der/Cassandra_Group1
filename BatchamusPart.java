import com.datastax.oss.driver.api.core.CqlSession;

import java.math.BigDecimal;
import java.util.*;
import java.io.*;

public class BatchamusPart {
    private CqlSession session;

    public BatchamusPart(CqlSession session) {
        this.session = session;
    }

    public List<String> bp(Scanner sc) throws IOException{

        KeyspaceRepository keyspaceRepository = new KeyspaceRepository(session);

        String keyspaceName = "", tableName = "";
        long keyspaceSize = 0;
        while (sc.hasNextLine()){
            String line = sc.nextLine();
            if (line.contains("Keyspace : ")){
                keyspaceName = line.substring(11);
            }
            if (line.contains("Table:")){
                tableName = line.substring(9);
            }
            if (line.contains("live)")){
                keyspaceSize += Long.parseLong(line.substring(21));
            }
        }

        List<String> pS = keyspaceRepository.getPartitionList(keyspaceName, tableName);
        Map<String, Integer> rPP = keyspaceRepository.getNumRowsByPart(keyspaceName, tableName);
        List<Double> fracs = new ArrayList<Double>();
        List<String> out = new ArrayList<String>();

        double totRow = 0;
        for (String s: pS){
            totRow += rPP.get(s);
        }
        for (String s: pS){
            fracs.add(rPP.get(s)/totRow);
        }

        BigDecimal y = new BigDecimal((double) keyspaceSize);
        String a = "B";
        ArrayList<String> xd = new ArrayList<>();
        xd.add("B");
        xd.add("KB");
        xd.add("MB");
        xd.add("GB");
        xd.add("TB");
        xd.add("PB");
        xd.add("EB");
        xd.add("ZB");
        ArrayList<String> xd2 = new ArrayList<>();
        xd2.add("B");
        xd2.add("KiB");
        xd2.add("MiB");
        xd2.add("GiB");
        xd2.add("TiB");
        xd2.add("PiB");
        xd2.add("EiB");
        xd2.add("ZiB");

        for (int j = 0; j < fracs.size(); j++){
            BigDecimal x = y.multiply(new BigDecimal(fracs.get(j)));
            for(int i = 0; i< xd.indexOf(a)+1;i++)
                x = x.multiply(new BigDecimal((double)1000/(double)1024));
            int d = 0;
            while(x.compareTo(new BigDecimal(1))<0)
            {
                x = x.multiply(new BigDecimal(1024));
                d--;
            }
            if(x.compareTo(new BigDecimal(1024))>0)
            {
                x = x.divide(new BigDecimal(1024));
                d++;
            }
            out.add(String.format(pS.get(j) + " ≈ %.3f %s\n", x.doubleValue(), xd2.get(xd.indexOf(a)+d)));
        }
        return out;
    }
}
