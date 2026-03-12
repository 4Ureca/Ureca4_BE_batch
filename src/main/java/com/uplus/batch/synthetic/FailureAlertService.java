package com.uplus.batch.synthetic;

/**
 * AI 요약 실패 알람 인터페이스.
 *
 * <p>기본 구현체 {@link DefaultFailureAlertService}는 ERROR 레벨 로그만 기록한다.
 * 실제 운영 환경에서는 Slack, 이메일, PagerDuty 등 채널에 맞는 구현체로 교체한다.
 */
public interface FailureAlertService {

    /**
     * @param message      알람 메시지
     * @param failureCount 현재 누적 실패 건수
     */
    void alert(String message, int failureCount);
}
