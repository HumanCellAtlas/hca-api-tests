package ingest.humancellatlas.org.mockuploadservice.configuration;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@RequiredArgsConstructor
@Getter
@Service("configuration")
public class Configuration implements InitializingBean {
    private final @NonNull Environment environment;

    private String ingestApiScheme;
    private String ingestApiHost;
    private int ingestApiPort;

    @Override
    public void afterPropertiesSet() throws Exception {
        ingestApiScheme = environment.getProperty("INGEST_API_SCHEME", "http");
        ingestApiHost = environment.getProperty("INGEST_API_HOST", "localhost");
        ingestApiPort = Integer.valueOf(environment.getProperty("INGEST_API_PORT", "8080"));
    }
}
