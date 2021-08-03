import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class KeyspaceRepository {
    private CqlSession session;
    private ArrayList<String> xd = new ArrayList<>();
    public KeyspaceRepository(CqlSession session) {
        this.session = session;
        xd.add("B");
        xd.add("KB");
        xd.add("MB");
        xd.add("GB");
        xd.add("TB");
        xd.add("PB");
        xd.add("EB");
        xd.add("ZB");
    }
    public List<String> getKeyspaceList() {
        Select select = QueryBuilder.selectFrom("system_schema", "keyspaces").all();
        ResultSet rs = session.execute(select.build());
        List<String> result = new ArrayList<>();
        rs.forEach(x -> result.add(x.getString("keyspace_name")));
        return result;
    }

    public List<String> getTableList(String keyspace)
    {
        Select select = QueryBuilder.selectFrom("system_schema", "tables").all();
        if (keyspace != null) {
            keyspace = keyspace.toLowerCase();
            select = select.where(Relation.column("keyspace_name").isEqualTo(QueryBuilder.literal(keyspace)));
        }
        ResultSet rs = session.execute(select.build());
        List<String> result = new ArrayList<>();
        rs.forEach(x -> result.add(x.getString("table_name")));
        return result;
    }

    public String getColDefs(String keyspace, String table)
    {
        try {
            Map<CqlIdentifier, ColumnMetadata> map = session.getMetadata().getKeyspace(keyspace).get().getTable(table).get().getColumns();
            Set<CqlIdentifier> set = map.keySet();
            StringBuilder s1 = new StringBuilder("");
            for (CqlIdentifier cqlIdentifier : set) {
                String s = map.get(cqlIdentifier).toString();
                s1.append(s.substring(s.indexOf("(") + 1, s.length() - 1) + " | ");
            }
            String out = s1.substring(0, s1.length() - 2).replaceAll(keyspace+"."+table+".", "");
            return (out);
        }catch (Exception e)
        {
            return "";
        }
    }

    public List<String> getPartitionVarList(String keyspace, String table)
    {
        try {
            List<ColumnMetadata> ace = session.getMetadata().getKeyspace(keyspace).get().getTable(table).get().getPartitionKey();
            List<String> colNames = new ArrayList<>();
            for (ColumnMetadata base : ace) {
                colNames.add(base.getName().toString());
            }
            return colNames;
        }
        catch (Exception e) {
            return(new ArrayList<String>());
        }
    }
    public List<DataType> getPartitionVarTypeList(String keyspace, String table)
    {
        try {
            List<ColumnMetadata> ace = session.getMetadata().getKeyspace(keyspace).get().getTable(table).get().getPartitionKey();
            List<DataType> colTypes = new ArrayList<>();
            for (ColumnMetadata base : ace) {
                colTypes.add(base.getType());
            }
            return colTypes;
        }
        catch (Exception e) {
            return(new ArrayList<DataType>());
        }
    }
    public List<String> getPartitionList(String keyspace, String table)
    {
        List<String> colNames = getPartitionVarList(keyspace, table);
        List<DataType> colTypes = getPartitionVarTypeList(keyspace, table);
        if(colNames.isEmpty())
            return new ArrayList<>();
        keyspace = keyspace.toLowerCase();
        table = table.toLowerCase();
        Select select = QueryBuilder.selectFrom(keyspace, table).columns(colNames);
        ResultSet y = session.execute(select.build());
        Set<String> diffStrings = new TreeSet<>();
        y.forEach(x -> {StringBuilder sb = new StringBuilder();
            sb.append('(');
            for(int i=0; i < colNames.size(); i++)
            {
                sb.append(conversion(colNames.get(i), colTypes.get(i), x) + ", ");
            }
            sb.delete(sb.length()-2, sb.length());
            sb.append(')');
            diffStrings.add(sb.toString());
        });
        List<String> parts = new ArrayList<>();
        for(String x: diffStrings)
        {
            parts.add(x);
        }
        return parts;
    }
    public Map<String, Integer> getRowsPerPartition(String keyspace, String table)
    {
        List<String> colNames = getPartitionVarList(keyspace, table);
        List<DataType> colTypes = getPartitionVarTypeList(keyspace, table);
        if(colNames.isEmpty())
            return new TreeMap<String, Integer>();
        keyspace = keyspace.toLowerCase();
        table = table.toLowerCase();
        Select select = QueryBuilder.selectFrom(keyspace, table).columns(colNames);
        ResultSet y = session.execute(select.build());
        Map<String, Integer> partitionKeysTONumInPartition = new HashMap<>();
        y.forEach(x -> {StringBuilder sb = new StringBuilder();
            sb.append('(');
            for(int i=0; i < colNames.size(); i++)
            {
                sb.append(conversion(colNames.get(i), colTypes.get(i), x) + ", ");
            }
            sb.delete(sb.length()-2, sb.length());
            sb.append(')');
            if(partitionKeysTONumInPartition.containsKey(sb.toString()))
            {
                partitionKeysTONumInPartition.put(sb.toString(), partitionKeysTONumInPartition.get(sb.toString())+1);
            }
            else
            {
                partitionKeysTONumInPartition.put(sb.toString(), 1);
            }
        });
        return partitionKeysTONumInPartition;
    }
    public String clusterName() throws IOException
    {
        BufferedReader br = new BufferedReader(new FileReader("cluster_info.txt"));
        br.readLine();
        return br.readLine().substring("\tName: ".length());
    }
    public String statsTable(Map<String, Integer> rPP)
    {
        if(rPP.size()==0)
            return "";
        Set<String> partitions = rPP.keySet();
        int min = Integer.MAX_VALUE, max = -1;
        double avg = 0.0;
        for(String part : partitions)
        {
            int num = rPP.get(part);
            min = Math.min(num, min);
            max = Math.max(num, max);
            avg+= num;
        }
        avg/=rPP.size();
        return "Min: " + min + " | Max: " + max + " | Average: " + avg;
    }
    public String statsPart(Map<String, Integer> rPP, String size)
    {
        int i = size.indexOf("B");
        int maxRows = -1;
        int totalRows = 0;
        Set<String> keyset = rPP.keySet();
        for(String bababoi : keyset)
        {
            maxRows = Math.max(maxRows, rPP.get(bababoi));
            totalRows += rPP.get(bababoi);
        }
        int d;
        double bites;
        if(size.charAt(i-2)!=' ')
        {
            i++;
        }
        d = xd.indexOf(size.substring(i-1));
        bites = Double.parseDouble(size.substring(0, i-2)) * maxRows / totalRows;
        while(bites>1)
        {
            bites /= 1000;
            d++;
        }
        if(d>0&&bites<1)
        {
            bites *= 1000;
            d--;
        }
        return "Max Partition Size: " + String.format("%.3f %s", bites, xd.get(d));
    }
    public Map<String, String> getTableSizes() throws IOException
    {
        BufferedReader br = new BufferedReader(new FileReader("table_info.txt"));
        Map<String, String> freShaVaCaDo = new TreeMap<>();
        br.readLine();
        String reader = br.readLine();
        String keyspace = "", table, size;
        long bytes;
        long total = 0;
        while(reader != null)
        {
            if(reader.equals("----------------")) {
                reader = br.readLine();
                if (reader == null)
                    break;
                keyspace = reader.substring("Keyspace : ".length());
                for(int i = 0; i<5;i++)
                    br.readLine();
            }
            else
            {
                table = reader.substring("\t\tTable: ".length());
                reader = br.readLine();
                while(!reader.substring(0,Math.min(reader.length(), 21)).equals("\t\tSpace used (live): "))
                    reader = br.readLine();
                bytes = Integer.parseInt(reader.substring("\t\tSpace used (live): ".length()));
                total+=bytes;
                double bites = (double) bytes;
                int d = 0;
                while(bites>1)
                {
                    bites /= 1000;
                    d++;
                }
                if(d>0&&bites<1)
                {
                    bites *= 1000;
                    d--;
                }
                size = String.format("%.3f %s", bites, xd.get(d));
                freShaVaCaDo.put(keyspace+"."+table, size);
                for(int i = 0; i<30; i++)
                    br.readLine();
            }
            reader = br.readLine();
        }
        double totalbytes = (double) total;
        int d = 0;
        while(totalbytes>1)
        {
            totalbytes /= 1000;
            d++;
        }
        if(d>0&&totalbytes<1)
        {
            totalbytes *= 1000;
            d--;
        }
        size = String.format("%.3f %s", totalbytes, xd.get(d));
        freShaVaCaDo.put("Cluster", size);
        if(freShaVaCaDo.size()!=0)
            return freShaVaCaDo;
        return null;
    }

    private String conversion(String col, DataType a, Row b){
        if(a.equals(DataTypes.ASCII))
        {
            return b.getString(col);
        }
        else if(a.equals(DataTypes.BIGINT))
        {
            return String.valueOf(b.getLong(col));
        }
        else if(a.equals(DataTypes.BLOB))
        {
            return "String.valueOf(b.getLong(col))";// TO BE IMPLEMENTED
        }
        else if(a.equals(DataTypes.BOOLEAN))
        {
            return String.valueOf(b.getBoolean(col));
        }
        else if(a.equals(DataTypes.COUNTER))
        {
            return "String.valueOf(b.getLong(col))";// TO BE IMPLEMENTED
        }
        else if(a.equals(DataTypes.BIGINT))
        {
            return String.valueOf(b.getLong(col));
        }
        else if(a.equals(DataTypes.DATE))
        {
            return "String.valueOf(b.getString(col))"; // TO BE IMPLEMENTED
        }
        else if(a.equals(DataTypes.DECIMAL))
        {
            return b.getBigDecimal(col).toString();
        }
        else if(a.equals(DataTypes.DOUBLE))
        {
            return String.valueOf(b.getDouble(col));
        }
        else if(a.equals(DataTypes.FLOAT))
        {
            return String.valueOf(b.getFloat(col));
        }
        else if(a.equals(DataTypes.INET))
        {
            return b.getInetAddress(col).toString();
        }
        else if(a.equals(DataTypes.INT))
        {
            return String.valueOf(b.getInt(col));
        }
        else if(a.equals(DataTypes.TIMESTAMP))
        {
            return b.getInstant(col).toString();
        }
        else if(a.equals(DataTypes.TEXT))
        {
            return b.getString(col);
        }
        // SOME TYPES YET TO BE IMPLEMENTED
        return "";
    }
}
