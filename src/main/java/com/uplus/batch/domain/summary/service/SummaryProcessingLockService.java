package com.uplus.batch.domain.summary.service;

import java.time.Duration;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class SummaryProcessingLockService {

  private final StringRedisTemplate redisTemplate;

  public boolean tryLock(Long consultId) {

    String key = "summary:processing:" + consultId;

    Boolean result =
        redisTemplate.opsForValue()
            .setIfAbsent(key, "1", Duration.ofMinutes(10));

    return Boolean.TRUE.equals(result);
  }

  public void unlock(Long consultId) {
    redisTemplate.delete("summary:processing:" + consultId);
  }
}