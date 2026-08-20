package io.github.marcschmidt1999.reactive.sqs;

import java.util.Map;
import java.util.Objects;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

/**
 * A mapped payload together with the metadata of the SQS delivery that carried it.
 *
 * @param payload mapped message body
 * @param queueUrl queue from which the message was received
 * @param rawMessage immutable AWS SDK message
 * @param <T> mapped payload type
 */
public record SqsMessage<T>(T payload, String queueUrl, Message rawMessage) {

    public SqsMessage {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(queueUrl, "queueUrl");
        Objects.requireNonNull(rawMessage, "rawMessage");
    }

    /**
     * @return SQS message identifier
     */
    public String messageId() {
        return rawMessage.messageId();
    }

    /**
     * @return immutable view of SQS system attributes
     */
    public Map<MessageSystemAttributeName, String> systemAttributes() {
        return rawMessage.attributes();
    }

    /**
     * @return immutable view of application-defined message attributes
     */
    public Map<String, MessageAttributeValue> messageAttributes() {
        return rawMessage.messageAttributes();
    }
}
