
package com.jvseguranca.jvsegurancaapp.security;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UserDetailsPasswordService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class CustomUserDetailsService implements UserDetailsService, UserDetailsPasswordService {

    private final JdbcTemplate jdbcTemplate;

    public CustomUserDetailsService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public UserDetails loadUserByUsername(String email)
            throws UsernameNotFoundException {

        String sql = """
            SELECT
                u.no_seg_email,
                u.no_seg_senha_usuario,
                t.no_seg_tipo_usuario
            FROM seg_db.segtb001_usuario u
            INNER JOIN seg_db.segtb005_tipo_usuario t
                ON u.nu_seg_tipo_usuario = t.nu_seg_tipo_usuario
            WHERE LOWER(u.no_seg_email) = LOWER(?)
            """;

        return jdbcTemplate.query(sql, resultSet -> {

            if (!resultSet.next()) {
                throw new UsernameNotFoundException(
                    "Usuário não encontrado."
                );
            }

            String emailUsuario =
                    resultSet.getString("no_seg_email");

            String senha =
                    resultSet.getString("no_seg_senha_usuario");

            String tipoUsuario =
                    resultSet.getString("no_seg_tipo_usuario");

            return User.withUsername(emailUsuario)
                    .password(senha)
                    .roles(tipoUsuario.toUpperCase())
                    .build();

        }, email);
    }

    @Override
    @Transactional
    public UserDetails updatePassword(
            UserDetails user,
            String newPassword
    ) {

        String sql = """
            UPDATE seg_db.segtb001_usuario
            SET no_seg_senha_usuario = ?
            WHERE LOWER(no_seg_email) = LOWER(?)
            """;

        jdbcTemplate.update(
            sql,
            newPassword,
            user.getUsername()
        );

        return User.withUserDetails(user)
                .password(newPassword)
                .build();
    }
}
