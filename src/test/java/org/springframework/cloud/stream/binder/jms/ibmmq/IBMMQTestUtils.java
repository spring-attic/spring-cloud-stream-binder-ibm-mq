package org.springframework.cloud.stream.binder.jms.ibmmq;

import java.util.Map;

import javax.jms.ConnectionFactory;

import org.springframework.beans.factory.config.YamlMapFactoryBean;
import org.springframework.cloud.stream.binder.jms.config.JmsBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.jms.ibmmq.config.IBMMQConfigurationProperties;
import org.springframework.cloud.stream.binder.jms.ibmmq.config.IBMMQJmsConfiguration;
import org.springframework.core.io.ClassPathResource;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.pcf.PCFMessage;
import com.ibm.mq.pcf.PCFMessageAgent;

/**
 * @author Donovan Muller
 */
public class IBMMQTestUtils {

	@SuppressWarnings("unchecked")
	public static IBMMQConfigurationProperties getIBMMQProperties() throws Exception {
		YamlMapFactoryBean factoryBean = new YamlMapFactoryBean();
		factoryBean.setResources(new ClassPathResource("application.yml"));

		Map<String, Object> mapObject = factoryBean.getObject();
		Map<String, Object> ibmMQPropertyMap = (Map<String, Object>) mapObject
				.get("ibmmq");

		IBMMQConfigurationProperties configurationProperties = new IBMMQConfigurationProperties();
		configurationProperties.setHost((String) ibmMQPropertyMap.get("host"));
		configurationProperties.setPort((Integer) ibmMQPropertyMap.get("port"));
		configurationProperties
				.setUsername((String) ibmMQPropertyMap.get("username"));
		configurationProperties
				.setPassword((String) ibmMQPropertyMap.get("password"));
		configurationProperties
				.setQueueManager((String) ibmMQPropertyMap.get("queueManager"));
		configurationProperties
				.setChannel((String) ibmMQPropertyMap.get("channel"));

		return configurationProperties;
	}

	public static ConnectionFactory createConnectionFactory() throws Exception {
		return new IBMMQJmsConfiguration(getIBMMQProperties())
				.connectionFactory(getIBMMQProperties());
	}

	public static void deprovisionDLQ() throws Exception {
		MQQueueManager queueManager = new MQQueueManager(
				getIBMMQProperties().getQueueManager());
		PCFMessageAgent pcfMessageAgent = new PCFMessageAgent(queueManager);

		try {
			String deadLetterQueueName = new JmsBinderConfigurationProperties().getDeadLetterQueueName();
			PCFMessage request = new PCFMessage(MQConstants.MQCMD_CLEAR_Q);
			request.addParameter(MQConstants.MQCA_Q_NAME, deadLetterQueueName);
			pcfMessageAgent.send(request);

			request = new PCFMessage(MQConstants.MQCMD_DELETE_Q);
			request.addParameter(MQConstants.MQCA_Q_NAME, deadLetterQueueName);
			pcfMessageAgent.send(request);
		}
		catch (MQException e) {
			if (e.getReason() != 2085) {
				throw new RuntimeException("Cannot deprovision DLQ", e);
			}
		}

		pcfMessageAgent.disconnect();
		queueManager.disconnect();
	}
}
