package org.springframework.cloud.stream.binder.jms.ibmmq.config;

import javax.jms.ConnectionFactory;

import com.ibm.mq.MQEnvironment;
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;

import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.jms.JndiConnectionFactoryAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.jms.config.JmsBinderAutoConfiguration;
import org.springframework.cloud.stream.binder.jms.ibmmq.IBMMQProvisioningProvider;
import org.springframework.cloud.stream.binder.jms.utils.DestinationNameResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.util.StringUtils;

/**
 * IBM MQ specific configuration.
 *
 * Creates the connection factory and the infrastructure provisioner.
 *
 * @author Donovan Muller
 */
@Configuration
@Import(JmsBinderAutoConfiguration.class)
@AutoConfigureAfter({ JndiConnectionFactoryAutoConfiguration.class })
@EnableConfigurationProperties(IBMMQConfigurationProperties.class)
public class IBMMQJmsConfiguration {

	private IBMMQConfigurationProperties configurationProperties;

	public IBMMQJmsConfiguration(
			final IBMMQConfigurationProperties configurationProperties) {
		this.configurationProperties = configurationProperties;
	}

	@ConditionalOnMissingBean(ConnectionFactory.class)
	@Bean
	public MQConnectionFactory connectionFactory(
			IBMMQConfigurationProperties configurationProperties) throws Exception {
		// see http://stackoverflow.com/a/33135633/2408961
		MQEnvironment.hostname = configurationProperties.getHost();
		MQEnvironment.port = configurationProperties.getPort();
		MQEnvironment.channel = configurationProperties.getChannel();

		if (!StringUtils.isEmpty(configurationProperties.getUsername())) {
			MQEnvironment.userID = configurationProperties.getUsername();
			MQEnvironment.password = configurationProperties.getPassword();
		}

		MQConnectionFactory connectionFactory = new MQConnectionFactory();
		connectionFactory.setHostName(configurationProperties.getHost());
		connectionFactory.setPort(configurationProperties.getPort());
		connectionFactory.setQueueManager(configurationProperties.getQueueManager());
		connectionFactory.setChannel(configurationProperties.getChannel());
		connectionFactory.setTransportType(configurationProperties.getTransportType());

		if (!StringUtils.isEmpty(configurationProperties.getUsername())) {
			connectionFactory.setStringProperty(WMQConstants.USERID,
					configurationProperties.getUsername());
			connectionFactory.setStringProperty(WMQConstants.PASSWORD,
					configurationProperties.getPassword());
		}

		return connectionFactory;
	}

	@Bean
	public IBMMQProvisioningProvider ibmMQQueueProvisioner(
			MQConnectionFactory connectionFactory,
			DestinationNameResolver destinationNameResolver)
			throws Exception {
		return new IBMMQProvisioningProvider(connectionFactory, configurationProperties,
				destinationNameResolver);
	}

}
