package com.jvseguranca.jvsegurancaapp.service;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/cameras")
public class CameraController {

    private final WhatsAppConsumerService whatsAppConsumerService;
    private final JdbcTemplate jdbcTemplate;

    public CameraController(
            WhatsAppConsumerService whatsAppConsumerService,
            JdbcTemplate jdbcTemplate) {
        this.whatsAppConsumerService = whatsAppConsumerService;
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping
    public ResponseEntity<?> listarCameras(Authentication authentication) {

        String email = authentication.getName();

        String sql = """
            SELECT
                c.nu_seg_camera,
                c.nu_seg_status_servico,
                c.ts_seg_camera_criado
            FROM seg_db.segtb002_cad_camera c
            INNER JOIN seg_db.segtb001_usuario u
                ON c.nu_seg_usuario = u.nu_seg_usuario
            WHERE LOWER(u.no_seg_email) = LOWER(?)
            ORDER BY c.nu_seg_camera
            """;

        List<Map<String, Object>> cameras =
                jdbcTemplate.queryForList(sql, email);

        return ResponseEntity.ok(cameras);
    }

    @PostMapping("/reload")
    public ResponseEntity<String> reloadCameras() {
        try {
            int novas = whatsAppConsumerService.recarregarFilasAtivas();

            return ResponseEntity.ok(
                "Filas de alerta e listeners sincronizados com sucesso! Novos listeners iniciados: " + novas
            );
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                .body("Erro ao recarregar câmeras: " + e.getMessage());
        }
    }
}