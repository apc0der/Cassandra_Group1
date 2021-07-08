import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.select.Select;

import java.util.ArrayList;
import java.util.List;

public class KeyspaceRepository {
    private CqlSession session;
    public KeyspaceRepository(CqlSession session)
    {
        this.session = session;
    }
    public void createKeyspace(String keyspaceName, int numberOfReplicas) {
        CreateKeyspace createKeyspace = SchemaBuilder.createKeyspace(keyspaceName)
                .ifNotExists()
                .withSimpleStrategy(numberOfReplicas);

        session.execute(createKeyspace.build());
    }
    public void useKeyspace(String keyspace) {
        session.execute("USE " + CqlIdentifier.fromCql(keyspace));
    }

    /**
     * Gets the list of Keyspaces on the Cluster.
     * @return The list of all keyspaces as a List Object
     */
    public List<String> getKeyspaceList()
    {
        Select select = QueryBuilder.selectFrom("system_schema", "keyspaces").all();
        ResultSet y = session.execute(select.build());
        List<String> result = new ArrayList<>();
        y.forEach(x -> result.add(x.getString("keyspace_name")));
        return result;
    }
    /**
     * Gets the list of Tables within a given Keyspace, or all tables
     * @param keyspace The keyspace from which the list of tables is to be retrieved.
     *                 If null, all tables will be returned
     * @return The list of all keyspaces within the specified keyspace.
     */
    public List<String> getTableList(String keyspace)
    {
        Select select = QueryBuilder.selectFrom("system_schema", "tables").all();
        if (keyspace != null) {
            keyspace = keyspace.toLowerCase();
            select = select.where(Relation.column("keyspace_name").isEqualTo(QueryBuilder.literal(keyspace)));
        }
        ResultSet y = session.execute(select.build());
        List<String> result = new ArrayList<>();
        y.forEach(x -> result.add(x.getString("table_name")));
        return result;
    }
}
