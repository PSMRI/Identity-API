package com.iemr.common.identity.utils;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
@Component
public class TokenDenylist {
	 private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	    
	    private static final String PREFIX = "denied_";

	    @Autowired
	    private RedisTemplate<String, Object> redisTemplate;

	    private String getKey(String jti) {
	        return PREFIX + jti;
	    }  
	    
	    public void addTokenToDenylist(String jti, Long expirationTime) {
	        if (jti == null || jti.trim().isEmpty()) {
	            return;
	        }
	        if (expirationTime == null || expirationTime <= 0) {
	            throw new IllegalArgumentException("Expiration time must be positive");
	        }
	        try {
	            String key = getKey(jti);  // Use helper method to get the key
	            redisTemplate.opsForValue().set(key, " ", expirationTime, TimeUnit.MILLISECONDS);
	        } catch (Exception e) {
	            throw new RuntimeException("Failed to denylist token", e);
	        }
	    }

	    public boolean isTokenDenylisted(String jti) {
	        if (jti == null || jti.trim().isEmpty()) {
	            return false;
	        }
	        try {
	        	String key = getKey(jti);  // Use helper method to get the key
	            return Boolean.TRUE.equals(redisTemplate.hasKey(key));
	        } catch (Exception e) {
	            logger.error("Failed to check denylist status for jti: " + jti, e);
	            return false;
	        }
	    }

	    // Remove a token's jti from the denylist (Redis)
	    public void removeTokenFromDenylist(String jti) {
	        if (jti != null && !jti.trim().isEmpty()) {
	        	String key = getKey(jti);
	            redisTemplate.delete(key);
	        }
	    }
}
