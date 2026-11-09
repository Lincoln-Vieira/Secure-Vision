package com.jvseguranca.jvsegurancaapp.config;

import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Classe de configuração global do RabbitMQ para o Spring Boot.
 * Define conversores de mensagens e factories personalizadas para os listeners.
 */
@Configuration
public class RabbitMQConfig {

    /**
     * Define o conversor de mensagens baseado em JSON utilizando a biblioteca Jackson.
     * Facilita a conversão automática entre bytes do RabbitMQ e objetos Java (DTOs).
     */
    @Bean
    public MessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    /**
     * Configura a fábrica de containers para os ouvintes (listeners) do RabbitMQ.
     * Associa a conexão padrão e o conversor de mensagens JSON configurado acima,
     * permitindo que os containers dinâmicos utilizem essa mesma infraestrutura.
     */
    @Bean
    public RabbitListenerContainerFactory<?> rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory, 
            MessageConverter messageConverter
    ) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(messageConverter);
        return factory;
    }
}