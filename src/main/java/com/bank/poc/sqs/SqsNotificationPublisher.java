package com.bank.poc.sqs;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.annotation.Value;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.net.URI;
import java.util.Map;

/**
 * Notification publisher for AWS SQS.
 *
 * In development: points to LocalStack at http://localhost:4566
 * In production: uses IAM Role credentials (no @Value key/secret)
 *
 * Analogy: SQS is the recipient's "inbox" — they will receive the
 * "you got a payment of $X" message asynchronously and durably.
 */
@Singleton
public class SqsNotificationPublisher {

    private static final Logger log = LoggerFactory.getLogger(SqsNotificationPublisher.class);
    private static final String QUEUE_NAME = "notifications";

    @Value("${aws.sqs.endpoint:http://localhost:4566}")
    private String sqsEndpoint;

    @Value("${aws.region:us-east-1}")
    private String awsRegion;

    private SqsClient sqsClient;
    private String queueUrl;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void initialize() {
        log.info("Initializing SQS Publisher | endpoint={}", sqsEndpoint);

        sqsClient = SqsClient.builder()
            .endpointOverride(URI.create(sqsEndpoint))
            .region(Region.of(awsRegion))
            // LocalStack accepts any credentials — use an IAM Role in production
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create("localstack", "localstack")))
            .build();

        this.queueUrl = resolveUrl();
        log.info("SQS Publisher ready | queueUrl={}", queueUrl);
    }

    /**
     * Publishes a notification to the SQS queue.
     * Message is serialized as JSON.
     */
    public String publish(Map<String, Object> notification) {
        try {
            String body = objectMapper.writeValueAsString(notification);

            SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(body)
                .build();

            SendMessageResponse response = sqsClient.sendMessage(request);
            log.info("SQS notification published | messageId={}", response.messageId());
            return response.messageId();

        } catch (Exception e) {
            log.error("Error publishing to SQS queue | queue={}", QUEUE_NAME, e);
            throw new RuntimeException("Failed to send SQS notification", e);
        }
    }

    private String resolveUrl() {
        try {
            GetQueueUrlResponse resp = sqsClient.getQueueUrl(
                GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build());
            return resp.queueUrl();
        } catch (QueueDoesNotExistException e) {
            log.warn("Queue '{}' not found, creating...", QUEUE_NAME);
            return createQueue();
        }
    }

    private String createQueue() {
        CreateQueueResponse resp = sqsClient.createQueue(
            CreateQueueRequest.builder()
                .queueName(QUEUE_NAME)
                .build());
        log.info("SQS queue created | url={}", resp.queueUrl());
        return resp.queueUrl();
    }
}
