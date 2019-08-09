package ingest.humancellatlas.org.mockuploadservice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ingest.humancellatlas.org.mockuploadservice.model.MockValidationId;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.UUID;

import static java.lang.String.format;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping("v1/area")
@AllArgsConstructor
public class AreaController {

    private final @NonNull MessageSender messageSender;
    private final @NonNull MockFileValidator mockFileValidator;

    private static final Logger LOGGER = LoggerFactory.getLogger(AreaController.class);

    @PostMapping(value="/{uuid}", produces=APPLICATION_JSON_VALUE)
    public ResponseEntity<String> createUploadArea(
            @PathVariable("uuid") String submissionUuid) {
        LOGGER.info(format("Upload Area creation requested for [%s]...", submissionUuid));
        JSONObject response = json();
        String uploadAreaUuid = UUID.randomUUID().toString();
        String uploadAreaUri = format("s3://org-humancellatlas-upload-dev/%s/", uploadAreaUuid);

        response.put("uri", uploadAreaUri);

        return ResponseEntity.created(URI.create(uploadAreaUri)).body(response.toString());
    }

    @PutMapping("/{areaUuid}/{fileName}/validate")
    public ResponseEntity validateFile(@PathVariable("areaUuid") String areaUuid,
            @PathVariable("fileName") String fileName) {
        LOGGER.info(format("File validation requested for [%s in %s]...", fileName, areaUuid));
        String jobId = UUID.randomUUID().toString();
        mockFileValidator.addJob(jobId, fileName);
        return ResponseEntity.ok().body(new MockValidationId(jobId));
    }

    @PutMapping(value="/{areaUuid}/files", consumes=APPLICATION_JSON_VALUE)
    public void uploadFile(@PathVariable("areaUuid") String areaUuid,
            @RequestBody FileMetadata file) {
        String fileName = file.getFileName();
        String contentType = file.getContentType();
        LOGGER.info(format("Uploading file [%s] with content type [%s] to [%s]...", fileName, contentType, areaUuid));
        messageSender.sendFileStagedNotification(areaUuid, fileName, contentType);
    }

    private JSONObject json() {
        return new JSONObject();
    }
}
