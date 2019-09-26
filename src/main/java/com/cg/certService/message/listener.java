package com.cg.certService.message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.transaction.annotation.Transactional;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.cg.certService.domain.Cert;
//import io.opentracing.Span;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import io.opentracing.Span;
import io.opentracing.Tracer;

@EnableBinding(KStreamProcessorX.class)
public class listener {
	
	
	@Autowired
	  private MessageSender messageSender;
    @Autowired
	private SimpMessagingTemplate template;
	
	@Autowired
       private Tracer tracer;
	
	@StreamListener(target = "input1", 
  		  condition="headers['message']=='tramitada'")
	public void retrieveTramitadaStatus(String messageJson) throws JsonParseException, JsonMappingException, IOException {
		System.out.println("String 1 :"+messageJson);
		Message<Cert> message = new ObjectMapper().readValue(messageJson, new TypeReference<Message<Cert>>(){});
		message.getPayload().setStatus("Tramitada");
		message.setLabel("Tramitada");
	      Span span = tracer.buildSpan("Receiving Tramitada Event from microservice 1 in microservice 3").start();
	      span.finish();
		this.template.convertAndSend("/topic/status", message.getPayload());

     }

	}
