package sample.camel.controllers;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class RouteController {

    private final org.apache.camel.ProducerTemplate producerTemplate;

    public RouteController(org.apache.camel.ProducerTemplate producerTemplate) {
        this.producerTemplate = producerTemplate;
    }

    @GetMapping("/start-combined-route/{param}")
    public String startCombinedRoute(@PathVariable int param) {
        return producerTemplate.requestBodyAndHeader("direct:combinedApi", null, "param", param, String.class);
    }

    @GetMapping("/start-api-kafka-route/{param}")
    public String startApiKafkaRoute(@PathVariable int param) {
        return producerTemplate.requestBodyAndHeader("direct:apiKafka", null, "param", param, String.class);
    }
}