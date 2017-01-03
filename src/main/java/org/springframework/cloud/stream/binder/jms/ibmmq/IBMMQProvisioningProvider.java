package org.springframework.cloud.stream.binder.jms.ibmmq;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.Topic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.jms.config.JmsBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.jms.ibmmq.config.IBMMQConfigurationProperties;
import org.springframework.cloud.stream.binder.jms.provisioning.JmsConsumerDestination;
import org.springframework.cloud.stream.binder.jms.provisioning.JmsProducerDestination;
import org.springframework.cloud.stream.binder.jms.utils.DestinationNameResolver;
import org.springframework.cloud.stream.binder.jms.utils.DestinationNames;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;

import com.ibm.mq.MQException;

/**
 * {@link ProvisioningProvider} for IBM MQ.
 *
 * @author Donovan Muller
 */
public class IBMMQProvisioningProvider
		implements ProvisioningProvider<ConsumerProperties, ProducerProperties> {

	private static final Logger logger = LoggerFactory
			.getLogger(IBMMQProvisioningProvider.class);

	private final IBMMQRequests ibmMQRequests;

	private final DestinationNameResolver destinationNameResolver;

	private final JmsBinderConfigurationProperties jmsBinderConfigurationProperties;

	public IBMMQProvisioningProvider(ConnectionFactory connectionFactory,
			IBMMQConfigurationProperties configurationProperties,
			DestinationNameResolver destinationNameResolver,
			JmsBinderConfigurationProperties jmsBinderConfigurationProperties)
			throws MQException {
		this.destinationNameResolver = destinationNameResolver;
		this.jmsBinderConfigurationProperties = jmsBinderConfigurationProperties;

		this.ibmMQRequests = new IBMMQRequests(connectionFactory,
				configurationProperties);
	}

	@Override
	public ProducerDestination provisionProducerDestination(String name,
			ProducerProperties properties) throws ProvisioningException {
		logger.info("Provisioning producer destination: '{}'", name);

		Collection<DestinationNames> topicAndQueueNames = this.destinationNameResolver
				.resolveTopicAndQueueNameForRequiredGroups(name, properties);

		final Map<Integer, Topic> partitionTopics = new HashMap<>();

		for (DestinationNames destinationNames : topicAndQueueNames) {
			String sanitisedTopicName = sanitiseObjectName(
					destinationNames.getTopicName());
			Topic topic = ibmMQRequests.createTopic(sanitisedTopicName);
			for (String queue : destinationNames.getGroupNames()) {
				// format for the subscribing queue name is: 'topic'.'queue'
				String sanitisedQueueName = sanitiseObjectName(
						String.format("%s.%s", sanitisedTopicName, queue));
				ibmMQRequests.createQueue(sanitisedQueueName);
				ibmMQRequests.subcribeQueueToTopic(sanitisedTopicName,
						sanitisedQueueName);
			}

			if (destinationNames.getPartitionIndex() != null) {
				partitionTopics.put(destinationNames.getPartitionIndex(), topic);
			}
			else {
				partitionTopics.put(-1, topic);
			}
		}

		return new JmsProducerDestination(partitionTopics);
	}

	@Override
	public ConsumerDestination provisionConsumerDestination(String name, String group,
			ConsumerProperties properties) throws ProvisioningException {
		logger.info("Provisioning consumer destination: '{}.{}'", name, group);

		ibmMQRequests
				.createQueue(jmsBinderConfigurationProperties.getDeadLetterQueueName());

		String queueName = this.destinationNameResolver
				.resolveQueueNameForInputGroup(group, properties);
		String topicName = sanitiseObjectName(this.destinationNameResolver
				.resolveQueueNameForInputGroup(name, properties));

		String sanitisedQueueName = sanitiseObjectName(
				String.format("%s.%s", topicName, queueName));
		Queue queue = ibmMQRequests.createQueue(sanitisedQueueName);
		ibmMQRequests.subcribeQueueToTopic(topicName, sanitisedQueueName);

		return new JmsConsumerDestination(queue);
	}

	/**
	 * Objects referring to MQ Objects. See naming convention rules here:
	 * https://www.ibm.com/support/knowledgecenter/SSFKSJ_9.0.0/com.ibm.mq.pro.doc/q003340_.htm
	 */
	private String sanitiseObjectName(String object) {
		String sanitisedObjectName = object.replaceAll("[^A-Za-z0-9._/%]", "").trim();
		if (sanitisedObjectName.length() > 48) {
			// strip characters from the start of the object name
			sanitisedObjectName = sanitisedObjectName
					.substring(sanitisedObjectName.length() - 48);
		}

		return sanitisedObjectName;
	}
}
