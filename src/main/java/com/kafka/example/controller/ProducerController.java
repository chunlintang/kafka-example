package com.kafka.example.controller;

import com.google.gson.Gson;
import com.kafka.example.common.ErrorCode;
import com.kafka.example.common.MessageEntity;
import com.kafka.example.common.Response;
import com.kafka.example.producer.SimpleProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/kafka")
public class ProducerController {
    @Autowired
    private SimpleProducer simpleProducer;

    @Value("${kafka.topic.default}")
    private String topic;

    private Gson gson = new Gson();

    @RequestMapping(value = "/hello", method = RequestMethod.GET, produces = {"application/json"})
    public Response sendKafka() {
        return new Response(ErrorCode.SUCCESS, "OK");
    }

    @RequestMapping(value = "/send", method = RequestMethod.POST, produces = {"application/json"})
    public Response sendKafka(@RequestBody MessageEntity messageEntity) {
        try {
            simpleProducer.send(topic, "key", messageEntity);
            return new Response(ErrorCode.SUCCESS, "SEND SUCCESS");
        } catch (Exception e) {
            return new Response(ErrorCode.EXCEPTION, "SEND ERROR");
        }
    }
}
