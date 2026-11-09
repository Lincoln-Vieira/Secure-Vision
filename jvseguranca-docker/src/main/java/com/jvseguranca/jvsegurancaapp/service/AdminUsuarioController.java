
package com.jvseguranca.jvsegurancaapp.service;

import java.util.List;
import java.util.Map;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/admin/usuarios")
public class AdminUsuarioController {

    private final JdbcTemplate jdbcTemplate;
    private final PasswordEncoder passwordEncoder;

    public AdminUsuarioController(
            JdbcTemplate jdbcTemplate,
            PasswordEncoder passwordEncoder) {
        this.jdbcTemplate = jdbcTemplate;
        this.passwordEncoder = passwordEncoder;
    }

    public record CadastroUsuarioRequest(
            String nome,
            String email,
            String senha,
            Integer tipoUsuario
    ) {}

    public record UsuarioResponse(
            Integer id,
            String nome,
            String email,
            String tipoUsuario
    ) {}

    @PostMapping
    public ResponseEntity<Map<String, String>> cadastrar(
            @RequestBody CadastroUsuarioRequest dados) {

        if (dados.nome() == null || dados.nome().isBlank()
                || dados.email() == null || dados.email().isBlank()
                || dados.senha() == null || dados.senha().length() < 8
                || dados.tipoUsuario() == null
                || (dados.tipoUsuario() != 1 && dados.tipoUsuario() != 2)) {

            return ResponseEntity.badRequest().body(
                Map.of("mensagem", "Verifique os dados informados.")
            );
        }

        String nome = dados.nome().trim();
        String email = dados.email().trim();
        String senhaHash = passwordEncoder.encode(dados.senha());

        if (nome.length() > 100 || email.length() > 150
                || !email.matches("^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$")) {

            return ResponseEntity.badRequest().body(
                Map.of("mensagem", "Nome ou e-mail inválido.")
            );
        }

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
                dados.tipoUsuario()
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

    @GetMapping
    public List<UsuarioResponse> listar() {

        String sql = """
            SELECT
                u.nu_seg_usuario,
                u.no_seg_nome,
                u.no_seg_email,
                t.no_seg_tipo_usuario
            FROM seg_db.segtb001_usuario u
            INNER JOIN seg_db.segtb005_tipo_usuario t
                ON u.nu_seg_tipo_usuario = t.nu_seg_tipo_usuario
            ORDER BY u.nu_seg_usuario
            """;

        return jdbcTemplate.query(sql, (rs, rowNum) ->
            new UsuarioResponse(
                rs.getInt("nu_seg_usuario"),
                rs.getString("no_seg_nome"),
                rs.getString("no_seg_email"),
                rs.getString("no_seg_tipo_usuario")
            )
        );
    }
}
