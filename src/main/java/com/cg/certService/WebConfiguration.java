package com.cg.certService;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfiguration implements WebMvcConfigurer {
	public void addCorsMapping(CorsRegistry register) {
		register
			.addMapping("/**")
			.allowedMethods("*")
			.allowedOrigins("*");
	}
}
