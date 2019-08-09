package ingest.humancellatlas.org.mockuploadservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class MockUploadServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(MockUploadServiceApplication.class, args);
	}

}
