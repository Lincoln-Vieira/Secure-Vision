
package com.jvseguranca.jvsegurancaapp.config;

import com.jvseguranca.jvsegurancaapp.security.CustomUserDetailsService;
import com.jvseguranca.jvsegurancaapp.security.LegacyPasswordEncoder;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.security.web.csrf.CsrfTokenRequestAttributeHandler;

@Configuration
public class SecurityConfig {

    private final CustomUserDetailsService userDetailsService;
    private final LegacyPasswordEncoder passwordEncoder;

    public SecurityConfig(
            CustomUserDetailsService userDetailsService,
            LegacyPasswordEncoder passwordEncoder) {
        this.userDetailsService = userDetailsService;
        this.passwordEncoder = passwordEncoder;
    }

    @Bean
    public DaoAuthenticationProvider authenticationProvider() {

        DaoAuthenticationProvider provider =
                new DaoAuthenticationProvider(userDetailsService);

        provider.setPasswordEncoder(passwordEncoder);
        provider.setUserDetailsPasswordService(userDetailsService);

        return provider;
    }

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http)
            throws Exception {

        http
            .authenticationProvider(authenticationProvider())

            .csrf(csrf -> csrf
                .csrfTokenRepository(
                    CookieCsrfTokenRepository.withHttpOnlyFalse()
                )
                .csrfTokenRequestHandler(
                    new CsrfTokenRequestAttributeHandler()
                )
            )

            .authorizeHttpRequests(auth -> auth

                // Páginas públicas
                .requestMatchers(
                    "/",
                    "/index.html",
                    "/imagem_secure.png",
                    "/login.html",
                    "/cadastro.html",
                    "/api/cadastro",
                    "/sobre.html",
                    "/contato.html",
                    "/csrf",
                    "/api/health",
                    "/status-container",
                    "/fundo-monitoramento.jpg",
                    "/favicon.ico",
                    "/css/**",
                    "/js/**",
                    "/images/**"
                ).permitAll()

                // Páginas que exigem login
                .requestMatchers(
                    "/dashboard.html",
                    "/cameras.html",
                    "/monitoramento.html",
                    "/configuracoes.html",
                    "/alertas.html"
                ).authenticated()

                // Rotas administrativas
                .requestMatchers(
                    "/api/admin/**"
                ).hasRole("ADMINISTRATOR")

                // Demais rotas exigem autenticação
                .anyRequest().authenticated()
            )

            .formLogin(form -> form
                .loginPage("/login.html")
                .loginProcessingUrl("/login")
                .usernameParameter("email")
                .passwordParameter("senha")
                .defaultSuccessUrl("/dashboard.html", true)
                .failureUrl("/login.html?erro=true")
                .permitAll()
            )

            .logout(logout -> logout
                .logoutUrl("/logout")
                .logoutSuccessUrl("/login.html?logout=true")
                .invalidateHttpSession(true)
                .deleteCookies("JSESSIONID")
                .permitAll()
            );

        return http.build();
    }
}
