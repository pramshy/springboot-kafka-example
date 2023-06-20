package com.example.producer;

import com.fasterxml.jackson.databind.JsonSerializable;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.internals.Topic;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;

import java.sql.SQLTransactionRollbackException;
import java.util.Map;
import java.util.Random;
import java.util.function.BiConsumer;

@SpringBootApplication
public class ProducerApplication {

	public static void main(String[] args) {
		SpringApplication.run(ProducerApplication.class, args);
	}

	final static String PAGE_VIEW_TOPIC = "pv_topic";
}

@Configuration
class  RunnerConfiguration
{

	void kafka(KafkaTemplate<Object, Object> kafkaTemplate) {
		var pageView = (PageView) random("kafka");
		kafkaTemplate.send(ProducerApplication.PAGE_VIEW_TOPIC, pageView);
	}

	private Object random(String source) {
		var names = "axs,ghhjj,kjkj,adsjh,skajs".split(",");
		var pages = "blog,about,index,news,exit".split(",");
		var random = new Random();
		var name = names[random.nextInt(names.length)];
		var page = pages[random.nextInt(pages.length)];
		var pageView = new PageView(page, Math.random() > .5 ? 100 : 1000, name, source);
		return pageView;
	}

	void stream(StreamBridge streamBridge) {
		streamBridge.send("pageViews-out-0", random("stream"));
	}

	void integration(MessageChannel messageChannel) {
		var message = MessageBuilder
				.withPayload(random("integration"))
			//	.copyHeadersIfAbsent(Map.of(KafkaHeaders.TOPIC, ProducerApplication.PAGE_VIEW_TOPIC))
				.build();
		messageChannel.send(message);
	}

	@Bean
	ApplicationListener<ApplicationReadyEvent> runnerListener(KafkaTemplate<Object, Object> kafkaTemplate,
															  @Qualifier("messageChannel") MessageChannel channel,
															  StreamBridge streamBridge) {
			return event -> {

				for (int i=0; i<=1000; i++) {
					kafka(kafkaTemplate);
					integration(channel);
					stream(streamBridge);
				}
			};
		}

}

@Configuration
class  IntegrationConfiguration {

	@Bean
	IntegrationFlow flow(@Qualifier("messageChannel") MessageChannel channel, KafkaTemplate<Object, Object> template) {
		var kafka = Kafka
				.outboundChannelAdapter(template)
				.topic(ProducerApplication.PAGE_VIEW_TOPIC);

		return IntegrationFlow
				.from(channel)
				.handle(kafka)
				.get();
	}

	@Bean
	MessageChannel messageChannel() {
		return MessageChannels.direct().getObject();
	}
}

@Configuration
class KafkaConfiguration{

	@KafkaListener (topics = ProducerApplication.PAGE_VIEW_TOPIC, groupId = "pvt_topic_group")
	public void onNewPageView(Message<PageView> pageView) {
		System.out.println("------------------------------------------------------");
		System.out.println("new page view "+pageView.getPayload());
		pageView.getHeaders().forEach((s, o) -> System.out.println(s + " = " + o));
	}

	@Bean
	NewTopic newTopic() {
		return TopicBuilder
				.name(ProducerApplication.PAGE_VIEW_TOPIC)
				.partitions(1)
				.replicas(1)
				.build();
	}

	@Bean
	JsonMessageConverter jsonMessageConverter() {
		return  new JsonMessageConverter();
	}

	@Bean
	KafkaTemplate<Object, Object> kafkaTemplate(ProducerFactory<Object, Object> producerFactory) {
		return new KafkaTemplate<>(producerFactory, Map.of(
				ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class
		));
	}

}

record PageView(String page, long duration, String userId, String source){}
