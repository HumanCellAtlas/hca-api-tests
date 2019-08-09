package ingest.humancellatlas.org.mockuploadservice.model;

import lombok.Data;

import java.time.Instant;
import java.time.temporal.TemporalField;
import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

@Data
public class MockValidationJob implements Delayed {
    private static final int jobDurationSeconds = 1;
    private String jobId;
    private String fileName;
    private long startTime;

    public MockValidationJob(String jobId, String fileName) {
        this.jobId = jobId;
        this.fileName = fileName;
        this.startTime = Instant.now().plusSeconds(jobDurationSeconds).toEpochMilli();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(startTime - Instant.now().toEpochMilli(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        MockValidationJob other = (MockValidationJob) o;
        return Long.compare(this.getStartTime(), other.getStartTime());
    }
}
