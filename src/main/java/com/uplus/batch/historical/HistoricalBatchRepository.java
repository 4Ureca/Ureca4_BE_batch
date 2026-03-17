package com.uplus.batch.historical;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;

/**
 * 과거 배치 처리 체크포인트 관리 레포지토리.
 *
 * <p>DDL (실행 전 수동 적용 필요):
 * <pre>{@code
 * CREATE TABLE IF NOT EXISTS historical_batch_log (
 *     id              BIGINT AUTO_INCREMENT PRIMARY KEY,
 *     target_date     DATE        NOT NULL,
 *     status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
 *     total_count     INT         DEFAULT 0,
 *     mysql_done      INT         DEFAULT 0,
 *     ai_done         INT         DEFAULT 0,
 *     mongo_done      INT         DEFAULT 0,
 *     fail_reason     TEXT,
 *     started_at      DATETIME,
 *     completed_at    DATETIME,
 *     created_at      DATETIME    DEFAULT CURRENT_TIMESTAMP,
 *     updated_at      DATETIME    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 *     UNIQUE KEY uk_target_date (target_date)
 * );
 * }</pre>
 *
 * <p>status 전이:
 * {@code PENDING → IN_PROGRESS → COMPLETED}
 * {@code IN_PROGRESS → FAILED → IN_PROGRESS (재시도)}
 */
@Repository
@RequiredArgsConstructor
public class HistoricalBatchRepository {

    private final JdbcTemplate jdbcTemplate;

    /**
     * 날짜가 historical_batch_log에 없을 때만 PENDING으로 초기화.
     * INSERT IGNORE로 중복 실행 방지.
     */
    public void initDate(LocalDate date, int totalCount) {
        jdbcTemplate.update(
                "INSERT IGNORE INTO historical_batch_log (target_date, status, total_count) VALUES (?, 'PENDING', ?)",
                date, totalCount
        );
    }

    /** PENDING 또는 FAILED(재시도 필요) 날짜를 오래된 순으로 조회 */
    public List<LocalDate> findPendingOrFailedDates() {
        return jdbcTemplate.query(
                "SELECT target_date FROM historical_batch_log " +
                "WHERE status IN ('PENDING', 'FAILED') ORDER BY target_date ASC",
                (rs, rn) -> rs.getDate("target_date").toLocalDate()
        );
    }

    /** 해당 날짜가 이미 COMPLETED 처리됐는지 확인 */
    public boolean isCompleted(LocalDate date) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM historical_batch_log WHERE target_date = ? AND status = 'COMPLETED'",
                Integer.class, date
        );
        return count != null && count > 0;
    }

    public void markInProgress(LocalDate date) {
        jdbcTemplate.update(
                "UPDATE historical_batch_log SET status='IN_PROGRESS', started_at=NOW(), fail_reason=NULL, updated_at=NOW() WHERE target_date=?",
                date
        );
    }

    public void markCompleted(LocalDate date) {
        jdbcTemplate.update(
                "UPDATE historical_batch_log SET status='COMPLETED', completed_at=NOW(), updated_at=NOW() WHERE target_date=?",
                date
        );
    }

    public void markFailed(LocalDate date, String reason) {
        String trimmed = reason != null && reason.length() > 500 ? reason.substring(0, 500) + "..." : reason;
        jdbcTemplate.update(
                "UPDATE historical_batch_log SET status='FAILED', fail_reason=?, updated_at=NOW() WHERE target_date=?",
                trimmed, date
        );
    }

    /** 진행 상황 업데이트 (날짜 처리 중 주기적으로 호출) */
    public void updateProgress(LocalDate date, int mysqlDone, int aiDone, int mongoDone) {
        jdbcTemplate.update(
                "UPDATE historical_batch_log SET mysql_done=?, ai_done=?, mongo_done=?, updated_at=NOW() WHERE target_date=?",
                mysqlDone, aiDone, mongoDone, date
        );
    }

    /** COMPLETED 상태인 날짜들을 PENDING으로 초기화 (재실행 용도) */
    public int resetCompletedDates() {
        return jdbcTemplate.update(
                "UPDATE historical_batch_log SET status='PENDING', mysql_done=0, ai_done=0, mongo_done=0, " +
                "fail_reason=NULL, started_at=NULL, completed_at=NULL, updated_at=NOW() WHERE status='COMPLETED'"
        );
    }

    /**
     * 상담 관련 테이블 전체 TRUNCATE + historical_batch_log 초기화.
     * consultation_results auto_increment가 1로 리셋된다.
     */
    public void truncateAllConsultationData() {
        jdbcTemplate.execute("SET FOREIGN_KEY_CHECKS = 0");
        for (String table : new String[]{
                "retention_analysis",
                "result_event_status",
                "summary_event_status",
                "excellent_event_status",
                "client_review",
                "customer_risk_logs",
                "consult_product_logs",
                "consultation_raw_texts",
                "consultation_results",
                "historical_batch_log"
        }) {
            jdbcTemplate.execute("TRUNCATE TABLE " + table);
        }
        jdbcTemplate.execute("SET FOREIGN_KEY_CHECKS = 1");
    }

    /** 전체 진행 현황 조회 (컨트롤러 status 엔드포인트용) */
    public List<java.util.Map<String, Object>> findAllStatus() {
        return jdbcTemplate.queryForList(
                "SELECT target_date, status, total_count, mysql_done, ai_done, mongo_done, " +
                "started_at, completed_at, fail_reason FROM historical_batch_log ORDER BY target_date"
        );
    }
}
