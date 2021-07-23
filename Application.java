import com.datastax.oss.driver.api.core.CqlSession;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class Application {

    public static void main(String args[]) throws IOException {
        CassandraConnector connector = new CassandraConnector();
        connector.connect("127.0.0.1", 9042, "datacenter1");
        CqlSession session = connector.getSession();

        KeyspaceRepository keyspaceRepository = new KeyspaceRepository(session);
        System.out.println(keyspaceRepository.getKeyspaceList());
        System.out.println(keyspaceRepository.getTableList(null));
        System.out.println(keyspaceRepository.getTableList("testKeyspace"));
        System.out.println(keyspaceRepository.getPartitionList("testKeyspace", "bozos").size() + " " + keyspaceRepository.getPartitionList("testKeyspace", "bozos"));
        System.out.println(keyspaceRepository.getNumRowsByPart("testKeyspace", "bozos"));
        System.out.println(keyspaceRepository.getColDefs("testKeyspace", "bozos"));

        Batchamus bpaaxeDos = new Batchamus(null, null, null);
        String bpOutDos = bpaaxeDos.b();
        System.out.print(bpOutDos);
        Batchamus bpaaxeTres = new Batchamus(null, "testkeyspace", null);
        String bpOutTres = bpaaxeTres.b();
        System.out.print(bpOutTres);
        Batchamus bpaaxeCuatro = new Batchamus(session, "testkeyspace", "bozos");
        List<String> bpOutCuatro = bpaaxeCuatro.bp();
        for (String s: bpOutCuatro) {
            System.out.print(s);
        }


        //keyspaceRepository.createKeyspace("testKeyspace", 1);
        //keyspaceRepository.useKeyspace("testKeyspace");

        //VideoRepository videoRepository = new VideoRepository(session);
        //videoRepository.dropTable("testKeyspace", "bozos");
        //videoRepository.createTable("testKeyspace", "videos");
        //videoRepository.truncateTable("testKeyspace", "videos");

        /*for (int i = 0; i < 5; i++){
            String TITLE = "";
            switch (i) {
                case 0:
                    TITLE += "Amogha";
                    break;
                case 1:
                    TITLE += "Hemant";
                    break;
                case 2:
                    TITLE += "Keshav";
                    break;
                case 3:
                    TITLE += "Anuj";
                    break;
                case 4:
                    TITLE += "Rishindra";
                    break;
            }
            videoRepository.insertVideo(new Video(TITLE, Instant.now().minus(i, ChronoUnit.DAYS)), "testKeyspace", "videos");
        }*/


        connector.close();
    }
}