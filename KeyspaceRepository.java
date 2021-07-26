import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.select.Select;

import java.util.*;

public class KeyspaceRepository {
    private CqlSession session;

    public KeyspaceRepository(CqlSession session) {
        this.session = session;
    }

    public void createKeyspace(String keyspaceName, int numberOfReplicas) {
        CreateKeyspace cK = SchemaBuilder.createKeyspace(keyspaceName)
                .ifNotExists()
                .withSimpleStrategy(numberOfReplicas);

        session.execute(cK.build());
    }

    public void useKeyspace(String keyspace) {
        session.execute("USE " + CqlIdentifier.fromCql(keyspace));
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
        if (keyspace != null)
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
        Map<CqlIdentifier,ColumnMetadata> map = session.getMetadata().getKeyspace(keyspace).get().getTable(table).get().getColumns();
        Set<CqlIdentifier> set = map.keySet();
        String s1 = "";
        for (CqlIdentifier cqlIdentifier: set) {
            String s = map.get(cqlIdentifier).toString();
            s1 += s.substring(s.indexOf("(")+1,s.length()-1) + ", ";
        }
        s1 = s1.substring(0,s1.length()-2);
        return (s1);
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
