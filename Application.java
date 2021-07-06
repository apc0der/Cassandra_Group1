import com.datastax.oss.driver.api.core.CqlSession;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class Application {

    public static void main(String args[]) {
        CassandraConnector connector = new CassandraConnector();
        connector.connect("127.0.0.1", 9042, "Cassandra");
        CqlSession session = connector.getSession();

        KeyspaceRepository keyspaceRepository = new KeyspaceRepository(session);

        // keyspaceRepository.createKeyspace("testKeyspace", 1);
        keyspaceRepository.useKeyspace("testKeyspace");

        VideoRepository videoRepository = new VideoRepository(session);

        videoRepository.createTable();
    for(int i = 0; i < 10; i++)
        videoRepository.insertVideo(new Video("Video Title " + (i+1), Instant.now()),"testKeyspace");
        /*videoRepository.insertVideo(new Video("Video Title 1", Instant.now()),"testKeyspace");
        videoRepository.insertVideo(new Video("Video Title 2", Instant.now().minus(1, ChronoUnit.DAYS)), "testKeyspace");*/

        List<Video> videos = videoRepository.selectAll(session.getName());
        connector.close();
    }
}
