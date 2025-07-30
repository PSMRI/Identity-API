package com.iemr.common.identity.utils;

import java.util.Date;
import java.util.UUID;
import java.util.function.Function;

import javax.crypto.SecretKey;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.UnsupportedJwtException;
import io.jsonwebtoken.security.Keys;
import io.jsonwebtoken.security.SignatureException;
@Component
public class JwtUtil {
	@Value("${jwt.secret}")
    private String SECRET_KEY;

    @Value("${jwt.access.expiration}")
    private long ACCESS_EXPIRATION_TIME;

    @Value("${jwt.refresh.expiration}")
    private long REFRESH_EXPIRATION_TIME;
    
    @Autowired
    private TokenDenylist tokenDenylist;  

    private SecretKey getSigningKey() {
        if (SECRET_KEY == null || SECRET_KEY.isEmpty()) {
            throw new IllegalStateException("JWT secret key is not set in application.properties");
        }
        return Keys.hmacShaKeyFor(SECRET_KEY.getBytes());
    }

    /**
     * Generate an access token.
     *
     * @param username the username of the user
     * @param userId   the user ID
     * @return the generated JWT access token
     */
    public String generateToken(String username, String userId) {
        return buildToken(username, userId, "access", ACCESS_EXPIRATION_TIME);
    }

    /**
     * Generate a refresh token.
     *
     * @param username the username of the user
     * @param userId   the user ID
     * @return the generated JWT refresh token
     */
    public String generateRefreshToken(String username, String userId) {
        return buildToken(username, userId, "refresh", REFRESH_EXPIRATION_TIME);
    }

    /**
     * Build a JWT token with the specified parameters.
     *
     * @param username      the username of the user
     * @param userId        the user ID
     * @param tokenType     the type of the token (access or refresh)
     * @param expiration    the expiration time of the token in milliseconds
     * @return the generated JWT token
     */
	private String buildToken(String username, String userId, String tokenType, long expiration) {
		if (username == null || username.trim().isEmpty()) {
			throw new IllegalArgumentException("Username cannot be null or empty");
		}
		if (userId == null || userId.trim().isEmpty()) {
			throw new IllegalArgumentException("User ID cannot be null or empty");
		}
		return Jwts.builder().subject(username).claim("userId", userId).claim("token_type", tokenType)
				.id(UUID.randomUUID().toString()).issuedAt(new Date())
				.expiration(new Date(System.currentTimeMillis() + expiration)).signWith(getSigningKey()).compact();
	}

    /**
     * Validate the JWT token, checking if it is expired and if it's blacklisted
     * @param token the JWT token
     * @return Claims if valid, null if invalid (expired or denylisted)
     */
    public Claims validateToken(String token) {
        try {
            Claims claims = Jwts.parser().verifyWith(getSigningKey()).build().parseSignedClaims(token).getPayload();
            String jti = claims.getId();
            
            // Check if token is denylisted (only if jti exists)
            if (jti != null && tokenDenylist.isTokenDenylisted(jti)) {
                return null;
            }
            return claims;
        } catch (ExpiredJwtException ex) {

            return null;  // Token is expired, so return null
        } catch (UnsupportedJwtException | MalformedJwtException | SignatureException | IllegalArgumentException ex) {
            return null;  // Return null for any other JWT-related issue (invalid format, bad signature, etc.)
        }
    }

    /**
     * Extract claims from the token
     * @param token the JWT token
     * @return all claims from the token
     */
	public Claims getAllClaimsFromToken(String token) {
		Claims claims = validateToken(token);
		if (claims == null) {
			throw new IllegalArgumentException("Invalid or denylisted token");
		}
		return claims;

	}

    /**
     * Extract a specific claim from the token using a function
     * @param token the JWT token
     * @param claimsResolver the function to extract the claim
     * @param <T> the type of the claim
     * @return the extracted claim
     */
    public <T> T getClaimFromToken(String token, Function<Claims, T> claimsResolver) {
        final Claims claims = getAllClaimsFromToken(token);
        return claimsResolver.apply(claims);
    }

    /**
     * Get the JWT ID (JTI) from the token
     * @param token the JWT token
     * @return the JWT ID
     */
    public String getJtiFromToken(String token) {
        return getAllClaimsFromToken(token).getId();
    }

    /**
     * Get the username from the token
     * @param token the JWT token
     * @return the username
     */
    public String getUsernameFromToken(String token) {
        return getAllClaimsFromToken(token).getSubject();
    }

    /**
     * Get the user ID from the token
     * @param token the JWT token
     * @return the user ID
     */
    public String getUserIdFromToken(String token) {
        return getAllClaimsFromToken(token).get("userId", String.class);
    }

    /**
     * Get the expiration time of the refresh token
     * @return the expiration time in milliseconds
     */
    public long getRefreshTokenExpiration() {
        return REFRESH_EXPIRATION_TIME;
    }
}
