package edu.um.civil.pfas;

import org.apache.log4j.Logger;
import org.apache.coyote.http11.AbstractHttp11Protocol;
import org.apache.log4j.PropertyConfigurator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.embedded.tomcat.TomcatConnectorCustomizer;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportResource;
import org.springframework.web.bind.annotation.CrossOrigin;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

@CrossOrigin
@SpringBootApplication
public class PfasApplication {
	private final static Logger logger = Logger.getLogger(PfasApplication.class);

	public static void main(String[] args) throws Exception {
//		System.out.println(new File("a.txt").getAbsolutePath());
		Properties props = new Properties();
		props.load(new FileInputStream("config/log4j.properties"));
		PropertyConfigurator.configure(props);

		SpringApplication.run(PfasApplication.class, args);
	}
	@Bean
	public TomcatServletWebServerFactory tomcatEmbedded() {
		TomcatServletWebServerFactory tomcat = new TomcatServletWebServerFactory();
		tomcat.addConnectorCustomizers((TomcatConnectorCustomizer) connector -> {
			if ((connector.getProtocolHandler() instanceof AbstractHttp11Protocol<?>)) {
				//-1 means unlimited
				((AbstractHttp11Protocol<?>) connector.getProtocolHandler()).setMaxSwallowSize(-1);
			}
		});
		return tomcat;
	}
}
