package ingest.humancellatlas.org.mockuploadservice;

import ingest.humancellatlas.org.mockuploadservice.model.MockValidationJob;
import lombok.AllArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.DelayQueue;

@Service
@AllArgsConstructor
public class MockFileValidator {
    private final MessageSender messageSender;
    private final Queue<MockValidationJob> queuedJobs = new DelayQueue<>();

    public void addJob(String jobId, String fileName) {
        this.queuedJobs.add(new MockValidationJob(jobId, fileName));
    }

    @Scheduled(fixedRate = 1000)
    private void run() {
        boolean exhausted = false;

        while(! exhausted) {
            try {
                Optional.ofNullable(queuedJobs.poll())
                        .ifPresentOrElse(messageSender::sendResultsForJob, () -> {
                            throw new NoMoreToConsume();
                        });
            } catch (NoMoreToConsume e) {
                exhausted = true;
            }
        }
    }

    private static class NoMoreToConsume extends RuntimeException {

    }
}
