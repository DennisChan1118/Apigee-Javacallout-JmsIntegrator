package com.jms.activemq;

import com.apigee.flow.execution.ExecutionContext;

import com.apigee.flow.execution.ExecutionResult;
import com.apigee.flow.execution.spi.Execution;
import com.apigee.flow.message.MessageContext;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
import javax.jms.*;

import java.io.StringWriter;
import java.io.PrintWriter;

import java.util.Map;

public class JmsProducerIntegrator implements Execution {

	protected static Logger logger = Logger.getLogger(JmsProducerIntegrator.class);

	private Map<String, String> properties;

	public JmsProducerIntegrator (Map<String, String> properties) {
		this.properties = properties;
	}
	public JmsProducerIntegrator() {
		
	}
	
	public ExecutionResult execute(MessageContext messageContext, ExecutionContext executionContext) {
		Connection connection = null;
		Session session = null;
		MessageProducer producer = null;

		String host = null;
		String port = null;
		String queue = null;
		String content = null;
		String replytoqueue = "";

		String debug = "";
		try {
			if (logger.isDebugEnabled())
                logger.debug("JMSIngressIntegrator.execute() Method entry");

			// Get Apigee flow variable value
			host = (String)messageContext.getVariable("cv.mqconfigs.host");
			port = (String)messageContext.getVariable("cv.mqconfigs.port");
			queue = (String)messageContext.getVariable("cv.mqconfigs.queue");
			replytoqueue = (String)messageContext.getVariable("cv.mqconfigs.replytoqueue");
			debug += "JMS Integrator\n";
			debug += "  Host: " + host + " \n";
			debug += "  Port: " + port + " \n";
			debug += "  Queue: " + queue + " \n";
			debug += "  replytoqueue: " + replytoqueue + " \n";

			String failoverBokerURL = "failover:(tcp://" + host + ":" + Integer.parseInt(port) + ")?Randomize=false";
			ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
				ActiveMQConnectionFactory.DEFAULT_USER, 
				ActiveMQConnectionFactory.DEFAULT_PASSWORD, 
				failoverBokerURL
			);

			// Creating connection to message broker
			connection = connectionFactory.createConnection();
			connection.start();
			debug += "Connected to " + host + ":" + port + " \n";

			// Creating session for sending messages
			session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);

			// Getting the queue
			Destination destination = session.createQueue(queue);

			// MessageProducer is used for sending messages (as opposed
			// to MessageConsumer which is used for receiving them)
			producer = session.createProducer(destination);

			// Creating queue message
			content = messageContext.getMessage().getContent();
			TextMessage message = session.createTextMessage(content);
			// Set the reply queue
			if (replytoqueue != null) {
				Destination replToQ = session.createQueue(replytoqueue);
				message.setJMSReplyTo(replToQ);
				debug += "ReplyToQueue set to " + replytoqueue + " \n";

			}
			debug += content + "\n";

			// Send queue message to destination queue
			producer.send(message);
			debug += "Message sent\n";

			// Set Apigee flow variable value
			messageContext.setVariable("javaDebug", debug);            
		} catch (Exception exception) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);

			exception.printStackTrace(pw);
			logger.error(exception);

			String sStackTrace = sw.toString();
			messageContext.setVariable("javaStackTrace", sStackTrace);
			messageContext.setVariable("javaDebug", debug);

			return ExecutionResult.ABORT;
		} finally {
			destroyQueueTransport(session, producer, connection);
		}

		return ExecutionResult.SUCCESS;
	}
	
	protected void destroyQueueTransport(Session session, MessageProducer producer, Connection connection) {
		if (producer != null) {
			try {
				producer.close();
			} catch (Exception e) {}

			producer = null;
		}
		if (session != null) {
			try {
				session.close();
			} catch (Exception e) {}

			session = null;
		}
		if (connection !=null) {
			try {
				connection.close();
			} catch (Exception e) {}

			connection=null;
		}
	}
}