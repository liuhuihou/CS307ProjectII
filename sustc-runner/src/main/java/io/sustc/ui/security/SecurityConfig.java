package io.sustc.ui.security;

import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SecurityConfig {

    @Bean
    public JwtUtil jwtUtil() {
        return new JwtUtil("change_me_in_real_project");
    }

    @Bean
    public FilterRegistrationBean<AuthFilter> authFilter(JwtUtil jwtUtil) {
        FilterRegistrationBean<AuthFilter> bean = new FilterRegistrationBean<>();
        bean.setFilter(new AuthFilter(jwtUtil));
        bean.addUrlPatterns("/ui/*");
        bean.setOrder(1);
        return bean;
    }
}
