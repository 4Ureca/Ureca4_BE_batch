package com.uplus.batch.domain.summary.service;

import java.time.Duration;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class SummaryProcessingLockService {

  private static final String SUMMARY_KEY = "summary:processing:";
  private static final String REINDEX_KEY = "es:reindex:";
  private static final Duration LOCK_TTL = Duration.ofMinutes(10);

  private final StringRedisTemplate redisTemplate;

  public boolean tryLock(Long consultId) {
    return Boolean.TRUE.equals(
        redisTemplate.opsForValue()
            .setIfAbsent(SUMMARY_KEY + consultId, "1", LOCK_TTL)
    );
  }

  public void unlock(Long consultId) {
    redisTemplate.delete(SUMMARY_KEY + consultId);
  }

  public boolean tryReindexLock(Long consultId) {
    return Boolean.TRUE.equals(
        redisTemplate.opsForValue()
            .setIfAbsent(REINDEX_KEY + consultId, "1", LOCK_TTL)
    );
  }

  public void reindexUnlock(Long consultId) {
    redisTemplate.delete(REINDEX_KEY + consultId);
  }
}