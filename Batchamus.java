import java.math.BigDecimal;
import java.util.*;
import java.io.*;

public class Batchamus {
        public static void main(String[] args) throws IOException{
                Scanner sc = new Scanner(new File("C:\\Cassandra\\apache-cassandra-3.11.10\\bin\\yeet\\beans2.txt"));
                
                long keyspaceSize = 0;
                while (sc.hasNextLine()){
                        String line = sc.nextLine();
                        if (line.contains("live)")){
                                keyspaceSize += Long.parseLong(line.substring(21));
                        }
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
                for(int i = 0; i< xd.indexOf(a)+1;i++)
                        y = y.multiply(new BigDecimal((double)1000/(double)1024));
                int d = 0;
                while(y.compareTo(new BigDecimal(1))<0)
                {
                        y = y.multiply(new BigDecimal(1024));
                        d--;
                }
                if(y.compareTo(new BigDecimal(1024))>0)
                {
                        y = y.divide(new BigDecimal(1024));
                        d++;
                }
                System.out.printf("%.3f %s\n", y.doubleValue(), xd2.get(xd.indexOf(a)+d));
        }
}
