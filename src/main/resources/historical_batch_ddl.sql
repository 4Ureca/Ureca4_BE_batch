-- ============================================================
-- 과거 상담 데이터 통합 배치 — 체크포인트 테이블 DDL
-- 배치 실행 전 MySQL에 수동으로 적용하세요.
-- ============================================================

CREATE TABLE IF NOT EXISTS historical_batch_log (
    id              BIGINT       AUTO_INCREMENT PRIMARY KEY,
    target_date     DATE         NOT NULL                  COMMENT '처리 대상 날짜',
    status          VARCHAR(20)  NOT NULL DEFAULT 'PENDING' COMMENT 'PENDING|IN_PROGRESS|COMPLETED|FAILED',
    total_count     INT          DEFAULT 0                  COMMENT '해당 날짜 총 생성 목표 건수',
    mysql_done      INT          DEFAULT 0                  COMMENT 'MySQL 삽입 완료 건수',
    ai_done         INT          DEFAULT 0                  COMMENT 'AI 처리 완료 건수',
    mongo_done      INT          DEFAULT 0                  COMMENT 'MongoDB 저장 완료 건수',
    fail_reason     TEXT                                    COMMENT '실패 사유 (최대 500자)',
    started_at      DATETIME                                COMMENT '처리 시작 시각',
    completed_at    DATETIME                                COMMENT '처리 완료 시각',
    created_at      DATETIME     DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_target_date (target_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='과거 상담 데이터 통합 배치 체크포인트 — 날짜별 처리 상태 추적';

-- status 전이:
--   PENDING     → 아직 처리 안 됨 (초기값)
--   IN_PROGRESS → 현재 처리 중
--   COMPLETED   → 정상 완료 (재실행 시 건너뜀)
--   FAILED      → 실패 (재실행 시 자동 재처리)
