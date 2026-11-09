package com.jvseguranca.jvsegurancaapp.service;

import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/alertas")
public class AlertController {

    private final JdbcTemplate jdbcTemplate;

    public AlertController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping
    public ResponseEntity<List<Map<String, Object>>> listarAlertas(
            Authentication authentication) {

        String email = authentication.getName();

        String sql = """
            SELECT
                h.nu_seg_alerta,
                h.nu_seg_camera,
                h.no_seg_mensagem_alerta,
                h.ts_seg_alerta_criado
            FROM seg_db.segtb006_historico_alerta h
            INNER JOIN seg_db.segtb001_usuario u
                ON h.nu_seg_usuario = u.nu_seg_usuario
            WHERE LOWER(u.no_seg_email) = LOWER(?)
            ORDER BY h.ts_seg_alerta_criado DESC
            LIMIT 100
            """;

        List<Map<String, Object>> alertas =
                jdbcTemplate.queryForList(sql, email);

        return ResponseEntity.ok(alertas);
    }
}
