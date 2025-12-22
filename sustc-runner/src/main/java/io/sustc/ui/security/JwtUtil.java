package io.sustc.ui.security;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

public final class JwtUtil {
    private static final String HMAC_ALG = "HmacSHA256";
    private final byte[] secret;

    public JwtUtil(String secret) {
        this.secret = secret.getBytes(StandardCharsets.UTF_8);
    }

    public String issueToken(long userId, long ttlSeconds) {
        long exp = Instant.now().getEpochSecond() + ttlSeconds;
        String payload = userId + ":" + exp;
        String sig = hmac(payload);
        return base64Url(payload) + "." + base64Url(sig);
    }

    public Long verifyAndGetUserId(String token) {
        if (token == null) return null;
        String[] parts = token.split("\\.");
        if (parts.length != 2) return null;

        String payload = new String(Base64.getUrlDecoder().decode(parts[0]), StandardCharsets.UTF_8);
        String sig = new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);

        if (!hmac(payload).equals(sig)) return null;

        String[] p = payload.split(":");
        if (p.length != 2) return null;

        long userId = Long.parseLong(p[0]);
        long exp = Long.parseLong(p[1]);
        if (Instant.now().getEpochSecond() > exp) return null;

        return userId;
    }

    private String hmac(String data) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALG);
            mac.init(new SecretKeySpec(secret, HMAC_ALG));
            byte[] raw = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(raw);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String base64Url(String s) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }
}