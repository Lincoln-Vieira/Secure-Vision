package com.jvseguranca.jvsegurancaapp;

import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.net.InetAddress;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

@RestController
public class HomeController {

    private final DataSource dataSource;

    // Injeta o DataSource para checar a saúde do banco de dados
    public HomeController(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @GetMapping("/status-container")
    @NonNull
    public String validarContainer() {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            return "Container validado com sucesso! Rodando no host/container ID: " + hostname;
        } catch (Exception e) {
            return "Erro ao validar container: " + e.getMessage();
        }
    }

    @GetMapping("/api/health")
    @NonNull
    public Map<String, Object> healthCheck() {
        Map<String, Object> status = new HashMap<>();
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            status.put("container_id", hostname);
            status.put("status", "UP");

            // Testa a conexão com o PostgreSQL
            try (Connection conn = dataSource.getConnection()) {
                status.put("database", conn.isValid(2) ? "CONNECTED" : "UNRESPONSIVE");
            } catch (Exception dbEx) {
                status.put("database", "ERROR: " + dbEx.getMessage());
            }

        } catch (Exception e) {
            status.put("status", "DOWN");
            status.put("error", e.getMessage());
        }
        return status;
    }
}