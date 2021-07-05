import com.datastax.oss.driver.api.core.CqlSession;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class Application {

    public static void main(String args[]) {
        CassandraConnector connector = new CassandraConnector();
        connector.connect("127.0.0.1", 9042, "datacenter1");
        CqlSession session = connector.getSession();

        KeyspaceRepository keyspaceRepository = new KeyspaceRepository(session);

        keyspaceRepository.createKeyspace("testKeyspace", 1);
        keyspaceRepository.useKeyspace("testKeyspace");

        VideoRepository videoRepository = new VideoRepository(session);

        videoRepository.createTable("testKeyspace");

        videoRepository.insertVideo(new Video("Video Title 1", Instant.now()), "testKeyspace");
        videoRepository.insertVideo(new Video("Video Title 2", Instant.now().minus(1, ChronoUnit.DAYS)), "testKeyspace");
        
        connector.close();
    }
}
