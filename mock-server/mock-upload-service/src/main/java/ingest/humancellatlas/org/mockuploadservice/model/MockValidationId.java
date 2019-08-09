package ingest.humancellatlas.org.mockuploadservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class MockValidationId {
    @JsonProperty(value = "validation_id")
    private String jobId;
}
