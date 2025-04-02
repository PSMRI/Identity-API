package com.iemr.common.identity.utils;

import com.iemr.common.identity.utils.exception.IEMRException;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.security.Key;
import java.util.Date;
import java.util.function.Function;

@Component
public class JwtUtil {

    @Value("${jwt.secret}")
    private String SECRET_KEY;

    private static final long EXPIRATION_TIME = 24L * 60 * 60 * 1000; // 1 day in milliseconds

    // Generate a key using the secret
    private Key getSigningKey() {
        if (SECRET_KEY == null || SECRET_KEY.isEmpty()) {
            throw new IllegalStateException("JWT secret key is not set in application.properties");
        }
        return Keys.hmacShaKeyFor(SECRET_KEY.getBytes());
    }

    // Generate JWT Token
    public String generateToken(String username, String userId) {
        Date now = new Date();
        Date expiryDate = new Date(now.getTime() + EXPIRATION_TIME);

        // Include the userId in the JWT claims
        return Jwts.builder().setSubject(username).claim("userId", userId) // Add userId as a claim
                .setIssuedAt(now).setExpiration(expiryDate).signWith(getSigningKey(), SignatureAlgorithm.HS256)
                .compact();
    }

    // Validate and parse JWT Token
    public Claims validateToken(String token) {
        try {
            return Jwts.parser().setSigningKey(getSigningKey()).build().parseClaimsJws(token).getBody();
        } catch (Exception e) {
            return null; // Handle token parsing/validation errors
        }
    }


    public String extractUsername(String token) {
        return extractClaim(token, Claims::getSubject);
    }

    public Integer extractUserId(String  jwtToken) throws IEMRException {
        try {
            // Validate JWT token and extract claims
            Claims claims = validateToken(jwtToken);

            if (claims == null) {
                throw new IEMRException("Invalid JWT token.");
            }

            String userId = claims.get("userId", String.class);

            return  Integer.parseInt(userId);

        } catch (Exception e) {
            throw new IEMRException("Validation error: " + e.getMessage(), e);
        }


    }


    public <T> T extractClaim(String token, Function<Claims, T> claimsResolver) {
        final Claims claims = extractAllClaims(token);
        return claimsResolver.apply(claims);
    }

    private Claims extractAllClaims(String token) {
        return Jwts.parser().setSigningKey(getSigningKey()).build().parseClaimsJws(token).getBody();
    }
}