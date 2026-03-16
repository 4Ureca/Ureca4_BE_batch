package com.uplus.batch.historical;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

/**
 * 과거 상담 데이터 생성 배치 설정.
 *
 * <p>이 배치의 역할: 상담 결과서 + 원문 생성만 담당.
 * <ul>
 *   <li>AI 한 줄 요약 → ExtractionScheduler가 담당 (result_event_status=REQUESTED 감지)</li>
 *   <li>MongoDB 요약 문서 생성 → SummarySyncItemWriter가 담당 (summary_event_status=requested 감지)</li>
 * </ul>
 */
@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "historical-batch")
public class HistoricalBatchProperties {

    /** 과거 데이터 생성 시작일 (포함) */
    private LocalDate startDate = LocalDate.of(2026, 1, 1);

    /** 과거 데이터 생성 종료일 (포함) */
    private LocalDate endDate = LocalDate.of(2026, 3, 24);

    /** 일일 생성 건수 */
    private int dailyCount = 1000;

    /**
     * 트랜잭션 1회당 삽입 건수.
     * 너무 크면 단일 트랜잭션이 길어지고, 너무 작으면 트랜잭션 오버헤드가 커진다.
     * 기본값 100이 적정.
     */
    private int chunkSize = 100;

    /**
     * 아웃바운드 상담 비율 (0~100).
     * 예) 30 → 일일 생성 건수 중 30%는 아웃바운드, 나머지 70%는 인바운드로 생성.
     */
    private int outboundRatio = 30;
}
