
package com.jvseguranca.jvsegurancaapp.service;

import java.util.Map;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/cadastro")
public class CadastroPublicoController {

    private final JdbcTemplate jdbcTemplate;
    private final PasswordEncoder passwordEncoder;

    public CadastroPublicoController(
            JdbcTemplate jdbcTemplate,
            PasswordEncoder passwordEncoder) {
        this.jdbcTemplate = jdbcTemplate;
        this.passwordEncoder = passwordEncoder;
    }

    public record CadastroRequest(
            String nome,
            String email,
            String senha
    ) {}

    @PostMapping
    public ResponseEntity<Map<String, String>> cadastrar(
            @RequestBody CadastroRequest dados) {

        if (dados == null
                || dados.nome() == null || dados.nome().isBlank()
                || dados.email() == null || dados.email().isBlank()
                || dados.senha() == null || dados.senha().length() < 8) {

            return ResponseEntity.badRequest().body(
                    Map.of("mensagem", "Verifique os dados informados.")
            );
        }

        String nome = dados.nome().trim();
        String email = dados.email().trim();

        if (nome.length() > 100
                || email.length() > 150
                || !email.matches("^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$")) {

            return ResponseEntity.badRequest().body(
                    Map.of("mensagem", "Nome ou e-mail inválido.")
            );
        }

        String senhaHash = passwordEncoder.encode(dados.senha());

        try {
            String sql = """
                INSERT INTO seg_db.segtb001_usuario
                    (no_seg_nome, no_seg_email,
                     no_seg_senha_usuario, nu_seg_tipo_usuario)
                VALUES (?, ?, ?, ?)
                """;

            jdbcTemplate.update(
                    sql,
                    nome,
                    email,
                    senhaHash,
                    2
            );

            return ResponseEntity.status(HttpStatus.CREATED).body(
                    Map.of("mensagem", "Usuário cadastrado com sucesso.")
            );

        } catch (DuplicateKeyException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(
                    Map.of("mensagem", "Este e-mail já está cadastrado.")
            );
        }
    }
}
