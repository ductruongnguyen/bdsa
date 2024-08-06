package truongnd.cp.web_server.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;
import truongnd.cp.web_server.dto.CustomOAuth2User;
import truongnd.cp.web_server.service.CustomOAuth2UserService;
import truongnd.cp.web_server.service.UserService;

import static org.springframework.security.config.Customizer.withDefaults;

@Configuration
@EnableWebSecurity
public class SecurityConfig {
    @Autowired
    UserService userService;

    @Autowired
    CustomOAuth2UserService oAuth2UserService;

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                .authorizeHttpRequests(authorize -> authorize
                                .requestMatchers("/login**", "/logout**", "/error**", "/oauth/**").permitAll()
                                .requestMatchers("/images/**", "/css/**").permitAll()
                                .anyRequest().authenticated()
                )
                .logout(logout -> logout
                        .logoutUrl("/logout")
                        .logoutSuccessUrl("/login?logout")
                        .invalidateHttpSession(true)
                        .deleteCookies("JSESSIONID")
                        .permitAll()
                )
                .oauth2Login(oauth2Login -> oauth2Login
                        .loginPage("/login")
                        .userInfoEndpoint(userInfoEndpoint ->
                                userInfoEndpoint.userService(oAuth2UserService)
                        )
                        .successHandler(
                                (request, response, authentication) -> {
                                    CustomOAuth2User oauthUser = (CustomOAuth2User) authentication.getPrincipal();
                                    userService.processOAuthPostLogin(oauthUser);
                                    response.sendRedirect("/");
                                }
                        )
                )
                .csrf(withDefaults());
        return http.build();
    }
}

