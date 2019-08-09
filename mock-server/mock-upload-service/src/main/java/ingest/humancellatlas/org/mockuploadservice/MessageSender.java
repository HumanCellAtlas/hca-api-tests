package ingest.humancellatlas.org.mockuploadservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ingest.humancellatlas.org.mockuploadservice.configuration.Configuration;
import ingest.humancellatlas.org.mockuploadservice.model.MockValidationJob;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;

import static java.lang.String.format;

@Service
@DependsOn("configuration")
public class MessageSender {
    private final @NonNull Configuration configuration;

    private RestTemplate restTemplate;
    private ObjectMapper objectMapper;
    private URI ingestUri;
    private URI validationResultsUri;
    private URI fileStagedNotificationUri;

    private static final String DEFAULT_VALIDATION_OUTPUT = "{\"validation_errors\": [], \"validation_state\": \"VALID\"}";

    public MessageSender(@Autowired Configuration configuration) {
        this.configuration = configuration;

        this.restTemplate = new RestTemplate();
        this.objectMapper = new ObjectMapper();
        this.ingestUri = configureIngestUri();
        this.validationResultsUri = configureValidationResultsUri();
        this.fileStagedNotificationUri = configureFileStagedNotificationUri();
    }

    public void sendResultsForJob(MockValidationJob mockValidationJob) {
        ObjectNode validationResults = objectMapper.createObjectNode();
        validationResults
                .put("validation_id", mockValidationJob.getJobId())
                .put("stdout", DEFAULT_VALIDATION_OUTPUT);
        restTemplate.postForEntity(validationResultsUri, validationResults, ObjectNode.class);
    }

    public void sendFileStagedNotification(String areaUuid, String fileName, String contentType) {
        ObjectNode fileStagedEvent = objectMapper.createObjectNode();
        fileStagedEvent
                .put("url", format("s3://sample-bucket/%s/%s", areaUuid, fileName))
                .put("name", fileName)
                .put("upload_area_id", areaUuid)
                .put("content_type", contentType);
        restTemplate.postForEntity(fileStagedNotificationUri, fileStagedEvent, ObjectNode.class);

    }

    private URI configureIngestUri() {
        return UriComponentsBuilder.newInstance()
                .scheme(configuration.getIngestApiScheme())
                .host(configuration.getIngestApiHost())
                .port(configuration.getIngestApiPort())
                .build().toUri();
    }

    private URI configureValidationResultsUri() {
        return UriComponentsBuilder.fromUri(this.ingestUri)
                .pathSegment("messaging")
                .pathSegment("fileValidationResult")
                .build().toUri();
    }

    private URI configureFileStagedNotificationUri() {
        return UriComponentsBuilder.fromUri(this.ingestUri)
                .pathSegment("messaging")
                .pathSegment("fileUploadInfo")
                .build().toUri();
    }
}
