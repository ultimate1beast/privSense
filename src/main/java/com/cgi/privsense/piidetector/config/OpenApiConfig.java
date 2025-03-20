package com.cgi.privsense.piidetector.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.tags.Tag;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("PrivSense API")
                        .version("1.0")
                        .description("API pour l'analyse de bases de données et la détection de PII")
                        .contact(new Contact().name("CGI").url("https://www.cgi.com")))
                .addTagsItem(new Tag().name("Database Scanner").description("API pour l'analyse de structures de bases de données"))
                .addTagsItem(new Tag().name("PII Detector").description("API pour la détection d'informations personnelles identifiables"));
    }
}