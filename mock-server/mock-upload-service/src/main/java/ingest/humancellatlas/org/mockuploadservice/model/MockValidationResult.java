package ingest.humancellatlas.org.mockuploadservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class MockValidationResult {
    private String stdout;
    @JsonProperty(value = "validation_id")
    private String validationId;
}
