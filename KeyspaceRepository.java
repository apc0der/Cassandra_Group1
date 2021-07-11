import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import org.relaxng.datatype.Datatype;

import java.util.*;

public class KeyspaceRepository {
    private CqlSession session;
    private static final String TABLE_NAME = "videos";


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

    public List<Video> selectAll(String keyspace) {
        Select select = QueryBuilder.selectFrom(TABLE_NAME).all();

        ResultSet resultSet = executeStatement(select.build(), keyspace);

        List<Video> result = new ArrayList<>();

        resultSet.forEach(x -> result.add(
                new Video(x.getUuid("video_id"), x.getString("title"), x.getInstant("creation_date"))
        ));

        return result;
    }
    private ResultSet executeStatement(SimpleStatement statement, String keyspace) {
        if (keyspace != null) {
            statement.setKeyspace(CqlIdentifier.fromCql(keyspace));
        }
        return session.execute(statement);
    }


    public List<String> getKeyspaceList()
    {
        Select select = QueryBuilder.selectFrom("system_schema", "keyspaces").all();
        ResultSet y = session.execute(select.build());
        List<String> result = new ArrayList<>();
        y.forEach(x -> result.add(x.getString("keyspace_name")));
        return result;
    }

    public Set<String> getPartitions(String keyspace, String table)
    {
        /*Select select1 = QueryBuilder.selectFrom("testKeyspace","videos").columns("video_id","title");
        ResultSet y1 = session.execute(select1.build());
        List<String> result1 = new ArrayList<>();
        y1.forEach(x -> result1.add(x.getString("video_id")));
        return result1;
         */
        List<ColumnMetadata> ace = session.getMetadata().getKeyspace(keyspace).get().getTable(table).get().getPartitionKey();
        List<String> colNames = new ArrayList<>();
        List<DataType> colTypes = new ArrayList<>();
        for(ColumnMetadata base : ace)
        {
            colNames.add(base.getName().toString());
            colTypes.add(base.getType());
        }

        Select select = QueryBuilder.selectFrom(keyspace,table).columns(colNames);
        ResultSet rs = session.execute(select.build());
        Set<String> result = new TreeSet<String>();
        rs.forEach(x -> {StringBuilder sb = new StringBuilder();
            sb.append("(");
            for(int i=0; i<colNames.size(); i++){
                sb.append(conversion(colNames.get(i),colTypes.get(i),x) + ", ");
            }
            sb.delete(sb.length()-2,sb.length());
            sb.append(")");

            result.add(sb.toString());
        });
        return  result;
    }

    private String conversion(String col, DataType a, Row b) {
        if(a.equals(DataTypes.ASCII))
            return b.getString(col);

        else if(a.equals(DataTypes.BIGINT))
            return String.valueOf(b.getLong(col));

        else if(a.equals(DataTypes.BLOB))
            return "String.valueOf(b.getLong(col))";// TO BE IMPLEMENTED

        else if(a.equals(DataTypes.BOOLEAN))
            return String.valueOf(b.getBoolean(col));

        else if(a.equals(DataTypes.COUNTER))
            return "String.valueOf(b.getLong(col))";// TO BE IMPLEMENTED

        else if(a.equals(DataTypes.BIGINT))
            return String.valueOf(b.getLong(col));

        else if(a.equals(DataTypes.DATE))
            return "String.valueOf(b.getString(col))"; // TO BE IMPLEMENTED

        else if(a.equals(DataTypes.DECIMAL))
            return b.getBigDecimal(col).toString();

        else if(a.equals(DataTypes.DOUBLE))
            return String.valueOf(b.getDouble(col));

        else if(a.equals(DataTypes.FLOAT))
            return String.valueOf(b.getFloat(col));

        else if(a.equals(DataTypes.INET))
            return b.getInetAddress(col).toString();

        else if(a.equals(DataTypes.INT))
            return String.valueOf(b.getInt(col));

        else if(a.equals(DataTypes.TIMESTAMP))
            return b.getInstant(col).toString();

        else if(a.equals(DataTypes.TEXT))
            return b.getString(col);

        else if (a.equals(DataTypes.UUID))
                return b.getUuid(col).toString();
        // SOME TYPES YET TO BE IMPLEMENTED
        return "";
    }


    public List<String> getTableList(String keyspace)
    {
        Select select = QueryBuilder.selectFrom("system_schema", "tables").all();
        if (keyspace != null){
            keyspace = keyspace.toLowerCase();
            select = select.where(Relation.column("keyspace_name").isEqualTo(QueryBuilder.literal(keyspace)));
        }
        ResultSet y = session.execute(select.build());
        List<String> result = new ArrayList<>();
        y.forEach(x -> result.add(x.getString("table_name")));
        return result;
    }

}
