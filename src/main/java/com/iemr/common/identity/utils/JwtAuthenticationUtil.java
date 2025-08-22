package com.iemr.common.identity.utils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.iemr.common.identity.domain.User;
import com.iemr.common.identity.exception.IEMRException;

import io.jsonwebtoken.Claims;
import jakarta.servlet.http.HttpServletRequest;

@Component
public class JwtAuthenticationUtil {

    private final CookieUtil cookieUtil;
    private final JwtUtil jwtUtil;
    private final RedisTemplate<String, Object> redisTemplate;
    private final JdbcTemplate jdbcTemplate;

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    // ✅ Constructor injection for all dependencies
    @Autowired
    public JwtAuthenticationUtil(
            CookieUtil cookieUtil,
            JwtUtil jwtUtil,
            RedisTemplate<String, Object> redisTemplate,
            JdbcTemplate jdbcTemplate) {

        this.cookieUtil = cookieUtil;
        this.jwtUtil = jwtUtil;
        this.redisTemplate = redisTemplate;
        this.jdbcTemplate = jdbcTemplate;
    }

    public ResponseEntity<String> validateJwtToken(HttpServletRequest request) {
        Optional<String> jwtTokenOpt = cookieUtil.getCookieValue(request, "Jwttoken");

        if (jwtTokenOpt.isEmpty()) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body("Error 401: Unauthorized - JWT Token is not set!");
        }

        String jwtToken = jwtTokenOpt.get();

        // Validate the token
        Claims claims = jwtUtil.validateToken(jwtToken);
        if (claims == null) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Error 401: Unauthorized - Invalid JWT Token!");
        }

        // Extract username from token
        String usernameFromToken = claims.getSubject();
        if (usernameFromToken == null || usernameFromToken.isEmpty()) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body("Error 401: Unauthorized - Username is missing!");
        }

        // Return the username if valid
        return ResponseEntity.ok(usernameFromToken);
    }

    public boolean validateUserIdAndJwtToken(String jwtToken) throws IEMRException {
        try {
            Claims claims = jwtUtil.validateToken(jwtToken);
            if (claims == null) {
                throw new IEMRException("Invalid JWT token.");
            }
            String userId = claims.get("userId", String.class);
            User user = getUserFromCache(userId);
            if (user == null) {
                user = fetchUserFromDB(userId);
            }
            if (user == null) {
                throw new IEMRException("Invalid User ID.");
            }

            return true; // Valid userId and JWT token
        } catch (Exception e) {
            logger.error("Validation failed: " + e.getMessage(), e);
            throw new IEMRException("Validation error: " + e.getMessage(), e);
        }
    }

    private User getUserFromCache(String userId) {
        String redisKey = "user_" + userId; // The Redis key format
        User user = (User) redisTemplate.opsForValue().get(redisKey);

        if (user == null) {
            logger.warn("User not found in Redis. Will try to fetch from DB.");
        } else {
            logger.info("User fetched successfully from Redis.");
        }

        return user; // Returns null if not found
    }

    private User fetchUserFromDB(String userId) {
        String redisKey = "user_" + userId; // Redis key format
        List<User> users = jdbcTemplate.query(
                "SELECT * FROM db_iemr.m_user WHERE UserID = ? AND Deleted = false",
                new BeanPropertyRowMapper<>(User.class), userId);

        if (users.isEmpty()) {
            logger.warn("User not found for userId: " + userId);
            return null;
        }

        User user = users.get(0);
        redisTemplate.opsForValue().set(redisKey, user, 30, TimeUnit.MINUTES);
        logger.info("User stored in Redis with key: " + redisKey);
        return user;
    }
}
