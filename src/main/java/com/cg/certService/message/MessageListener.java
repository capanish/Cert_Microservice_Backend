package com.cg.certService.message;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.cg.certService.controller.CertController;
import com.cg.certService.domain.Cert;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.messaging.simp.SimpMessagingTemplate;


@Component
@EnableBinding(Sink.class)
@EnableIntegration
public class MessageListener {

	@Autowired
	  private MessageSender messageSender;

	@Autowired
	private SimpMessagingTemplate template;

	@StreamListener(target = Sink.INPUT,
			condition="headers['message']=='tramitada'")
		  @Transactional
		  public void retrieveTramitadaStatus(String messageJson) throws JsonParseException, JsonMappingException, IOException {
		System.out.println("String 1 :"+messageJson);
		Message<Cert> message = new ObjectMapper().readValue(messageJson, new TypeReference<Message<Cert>>(){});
		message.getPayload().setStatus("Tramitada");
		message.setLabel("Tramitada");
		this.template.convertAndSend("/topic/status", message.getPayload());



	}

	@StreamListener(target = Sink.INPUT,
			condition="headers['message']=='certdenegada'")
		  @Transactional
		  public void retriveDeniedStatus(String messageJson) throws JsonParseException, JsonMappingException, IOException {
		System.out.println("String 2 :"+messageJson);
		Message<Cert> message = new ObjectMapper().readValue(messageJson, new TypeReference<Message<Cert>>(){});
		message.getPayload().setStatus("Denegada");
		message.setLabel("Denegada-9");
		this.template.convertAndSend("/topic/status", message.getPayload());

	}

}
