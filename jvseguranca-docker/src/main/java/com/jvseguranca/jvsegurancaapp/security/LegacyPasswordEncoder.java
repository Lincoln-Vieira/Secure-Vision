
package com.jvseguranca.jvsegurancaapp.security;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

@Component
public class LegacyPasswordEncoder implements PasswordEncoder {

    private final BCryptPasswordEncoder bcrypt =
            new BCryptPasswordEncoder();

    @Override
    public String encode(CharSequence rawPassword) {
        return bcrypt.encode(rawPassword);
    }

    @Override
    public boolean matches(
            CharSequence rawPassword,
            String storedPassword
    ) {
        if (storedPassword == null) {
            return false;
        }

        if (storedPassword.startsWith("$2a$")
                || storedPassword.startsWith("$2b$")
                || storedPassword.startsWith("$2y$")) {
            return bcrypt.matches(rawPassword, storedPassword);
        }

        return rawPassword.toString().equals(storedPassword);
    }

    @Override
    public boolean upgradeEncoding(String encodedPassword) {
        return encodedPassword == null
                || !encodedPassword.startsWith("$2");
    }
}
