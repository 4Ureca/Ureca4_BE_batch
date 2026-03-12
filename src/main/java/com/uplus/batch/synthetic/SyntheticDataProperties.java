package com.uplus.batch.synthetic;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * application.yml의 synthetic-data 섹션과 바인딩되는 설정 클래스.
 *
 * <pre>
 * synthetic-data:
 *   enabled: true
 *   batch-size: 20
 *   fixed-delay: 300000
 *   failure-alert-threshold: 5
 * </pre>
 */
@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "synthetic-data")
public class SyntheticDataProperties {

    /** 스케줄러 활성화 여부 */
    private boolean enabled = true;

    /** 회차당 생성할 합성 상담 건수 */
    private int batchSize = 20;

    /** 스케줄러 실행 간격 (밀리초, 기본 5분) */
    private long fixedDelay = 300_000L;

    /** AI 요약 누적 실패(retryCount>=3) 건수가 이 값 이상이면 알람 발송 */
    private int failureAlertThreshold = 5;
}
