package com.uplus.batch.synthetic;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * FailureAlertService 기본 구현체.
 * ERROR 레벨 로그로 알람을 대체한다.
 *
 * <p>실제 운영 환경에서 Slack/이메일 등으로 교체하려면 이 Bean을 다른 구현체로 오버라이드할 것.
 */
@Slf4j
@Service
public class DefaultFailureAlertService implements FailureAlertService {

    @Override
    public void alert(String message, int failureCount) {
        log.error("[ALERT] {} | 누적 실패 건수: {}건 (retryCount >= 3 도달)", message, failureCount);
    }
}
