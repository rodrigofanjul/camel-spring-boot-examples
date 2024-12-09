/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sample.camel;

import java.util.Map;

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.endpoint.LambdaEndpointRouteBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

//CHECKSTYLE:OFF
/**
 * A sample Spring Boot application that starts the Camel routes.
 */
@SpringBootApplication
public class MyCamelApplication {

    /**
     * A main method to start this application.
     */
    public static void main(String[] args) {
        SpringApplication.run(MyCamelApplication.class, args);
    }

    // embed route directly using type-safe endpoint-dsl using java lambda style
    @Bean
    public LambdaEndpointRouteBuilder myRoute() {
        return rb -> {
            // Ruta combinada que maneja ambas APIs
            rb.from(rb.direct("combinedApi"))
                .log("Calling first API with param: ${header.param}")
                .setHeader("param", rb.simple("${header.param}"))
                .toD("https://jsonplaceholder.typicode.com/users/${header.param}?connectTimeout=5000&responseTimeout=10000") // Llamada a la primera API
                .log("Response from first API: ${body}") // Log de la respuesta de la primera API
                .setHeader("param", rb.simple("${header.param}"))
                .setBody(rb.constant(null))
                .toD("https://jsonplaceholder.typicode.com/posts/${header.param}?connectTimeout=5000&responseTimeout=10000") // Llamada a la segunda API
                .log("Response from second API: ${body}") // Log de la respuesta de la segunda API
                .process(exchange -> {
                    // Procesa solo la respuesta de la segunda API
                    String secondApiResponse = exchange.getMessage().getBody(String.class);
                    exchange.getMessage().setBody(secondApiResponse); // Actualiza el cuerpo con la respuesta de la segunda API
                })
                .log("Final response (second API): ${body}"); // Log de la respuesta final (solo segunda API)

                // Ruta combinada que maneja ambas APIs y publica en Kafka
                rb.from(rb.direct("apiKafka"))
                    .log("Calling first API with param: ${header.param}")
                    .setHeader("param", rb.simple("${header.param}"))
                    .toD("https://jsonplaceholder.typicode.com/users/${header.param}?connectTimeout=5000&responseTimeout=10000") // Llamada a la primera API
                    .log("Response from first API: ${body}")
                    .setHeader("param", rb.simple("${header.param}"))
                    .setBody(rb.constant(null))
                    .log("Calling second API with param: ${header.param}")
                    .toD("https://jsonplaceholder.typicode.com/posts/${header.param}?connectTimeout=5000&responseTimeout=10000") // Llamada a la segunda API
                    .log("Response from second API: ${body}")
                    .process(exchange -> {
                        // Procesa solo la respuesta de la segunda API
                        String secondApiResponse = exchange.getMessage().getBody(String.class);
                        exchange.getMessage().setBody(secondApiResponse); // Actualiza el cuerpo con la respuesta de la segunda API
                    })
                    .log("Final response (second API): ${body}") // Log de la respuesta final (solo segunda API)
                    .to("kafka:my-topic?brokers=localhost:29092"); // Enviar la respuesta al tópico de Kafka

                // Ruta combinada que consume de Kafka y llama ambas APIs
                rb.from("kafka:my-topic?brokers=localhost:29092&groupId=my-group&autoOffsetReset=earliest") // Lee mensajes desde Kafka
                    .log("Received message from Kafka: ${body}") // Log del mensaje recibido
                    .process(exchange -> {
                        // Extraer el id desde el mensaje (suponiendo que es JSON)
                        String kafkaMessage = exchange.getMessage().getBody(String.class);
                        try {
                            com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();
                            // Parsear el mensaje JSON
                            Map<String, Object> messageMap = objectMapper.readValue(kafkaMessage, Map.class);
                            // Extraer el campo "id"
                            String id = messageMap.getOrDefault("id", "").toString();
                            exchange.getMessage().setHeader("param", id); // Configura "param" con el id extraído
                        } catch (Exception e) {
                            throw new IllegalStateException("Failed to parse Kafka message as JSON: " + kafkaMessage, e);
                        }
                    })
                    .log("Calling first API with param: ${header.param}")
                    .toD("https://jsonplaceholder.typicode.com/users/${header.param}?connectTimeout=5000&responseTimeout=10000") // Llama a la primera API
                    .setHeader("param", rb.simple("${header.param}")) // Vuelve a usar el parámetro
                    .setBody(rb.constant(null)) // Limpia el cuerpo antes de la segunda llamada
                    .log("Calling second API with param: ${header.param}")
                    .toD("https://jsonplaceholder.typicode.com/posts/${header.param}?connectTimeout=5000&responseTimeout=10000") // Llama a la segunda API
                    .process(exchange -> {
                        // Procesa la respuesta de la segunda API
                        String secondApiResponse = exchange.getMessage().getBody(String.class);
                        exchange.getMessage().setBody(secondApiResponse); // Actualiza el cuerpo con la respuesta de la segunda API
                    })
                    .log("Final response (second API): ${body}");
        };

        
    }

}
// CHECKSTYLE:ON