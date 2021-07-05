import java.time.Instant;
import java.util.UUID;

public class Video {
    private UUID id;
    private String title;
    private Instant creationDate;

    public Video(UUID id, String title, Instant creationDate) {
        this.id = id;
        this.title = title;
        this.creationDate = creationDate;
    }

    public Video(String title, Instant creationDate) {
        this.title = title;
        this.creationDate = creationDate;
    }

    public UUID getId(){
        return id;
    }
    public String getTitle(){
        return title;
    }
    public Instant getCreationDate(){
        return creationDate;
    }

    public void setId(UUID id){
        this.id = id;
    }
    public void setTitle(String title){
        this.title = title;
    }
    public void setCreationDate(Instant creationDate){
        this.creationDate = creationDate;
    }
}