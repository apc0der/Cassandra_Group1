import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.truncate.Truncate;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class VideoRepository {
    private static final String TABLE_NAME = "bozos";
    private CqlSession session;

    public VideoRepository(CqlSession sessions) {
        session = sessions;
    }
    public void createTable() {
        createTable(null);
    }

    public void createTable(String keyspace) {
        CreateTable createTable = SchemaBuilder.createTable(TABLE_NAME)
                .ifNotExists()
                .withPartitionKey("title", DataTypes.TEXT)
                .withColumn("video_id", DataTypes.UUID)
                .withColumn("creation_date", DataTypes.TIMESTAMP);
        SimpleStatement create = createTable.build();
        executeStatement(create, keyspace);
    }
    public void dropTable(String keyspace){
        SimpleStatement drop = SchemaBuilder.dropTable(TABLE_NAME).ifExists().build();
        executeStatement(drop, keyspace);
    }

    public void truncateTable(String keyspace){
        SimpleStatement truncate = QueryBuilder.truncate(keyspace, TABLE_NAME).build();
        executeStatement(truncate, keyspace);
    }

    private ResultSet executeStatement(SimpleStatement statement, String keyspace) {
        statement.setKeyspace(CqlIdentifier.fromCql(keyspace));

        return session.execute(statement);
    }

    public UUID insertVideo(Video video, String keyspace) {
        UUID videoId = UUID.randomUUID();

        video.setId(videoId);

        RegularInsert insertInto = QueryBuilder.insertInto(TABLE_NAME)
                .value("video_id", QueryBuilder.bindMarker())
                .value("title", QueryBuilder.bindMarker())
                .value("creation_date", QueryBuilder.bindMarker());

        SimpleStatement insertStatement = insertInto.build();
        PreparedStatement preparedStatement = session.prepare(insertStatement);

        BoundStatement statement = preparedStatement.bind()
                .setUuid(0, video.getId())
                .setString(1, video.getTitle())
                .setInstant(2, video.getCreationDate());

        session.execute(statement);

        return videoId;
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
}
