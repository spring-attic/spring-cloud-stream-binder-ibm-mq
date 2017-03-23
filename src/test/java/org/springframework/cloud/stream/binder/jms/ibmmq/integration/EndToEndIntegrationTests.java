package org.springframework.cloud.stream.binder.jms.ibmmq.integration;

import java.util.List;

import org.junit.Test;

import org.springframework.cloud.stream.binder.jms.config.JmsConsumerProperties;
import org.springframework.cloud.stream.binder.jms.ibmmq.IBMMQProvisioningProvider;
import org.springframework.cloud.stream.binder.jms.ibmmq.IBMMQTestUtils;
import org.springframework.cloud.stream.binder.jms.utils.Base64UrlNamingStrategy;
import org.springframework.cloud.stream.binder.jms.utils.DestinationNameResolver;
import org.springframework.cloud.stream.binder.test.integration.receiver.ReceiverApplication;
import org.springframework.cloud.stream.binder.test.integration.sender.SenderApplication;
import org.springframework.messaging.Message;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.springframework.cloud.stream.binder.test.TestUtils.waitFor;

/**
 * @author Donovan Muller
 */
public class EndToEndIntegrationTests extends
		org.springframework.cloud.stream.binder.test.integration.EndToEndIntegrationTests {

	public EndToEndIntegrationTests() throws Exception {
		super(new IBMMQProvisioningProvider(IBMMQTestUtils.createConnectionFactory(),
				IBMMQTestUtils.getIBMMQProperties(),
				new DestinationNameResolver(new Base64UrlNamingStrategy("anonymous."))),
				IBMMQTestUtils.createConnectionFactory());
	}

	@Test
	public void testBindingDestinationsWithInvalidCharacters() throws Exception {
		final SenderApplication.Sender sender = createSender(
				String.format(REQUIRED_GROUPS_FORMAT,
						"th!s->>-is-a-g-r$up-that-is-r&eally-r3*lly-inV@lid"));
		ReceiverApplication.Receiver receiver = createReceiver(String.format(
				INPUT_GROUP_FORMAT, "th!s->>is-a-g-r$up-that-is-r&eally-r3*lly-inV@lid"));

		for (String messageText : MESSAGE_TEXTS) {
			sender.send(messageText);
		}

		final List<Message> messages = receiver.getHandledMessages();

		waitFor(new Runnable() {
			@Override
			public void run() {
				assertThat(messages, hasSize(4));
			}
		});
	}

	@Override
	protected void deprovisionDLQ() throws Exception {
		IBMMQTestUtils.deprovisionDLQ(JmsConsumerProperties.DEFAULT_DLQ_NAME);
	}

}
