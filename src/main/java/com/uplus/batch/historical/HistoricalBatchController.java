package com.uplus.batch.historical;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import co.elastic.clients.elasticsearch.core.DeleteByQueryResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 과거 상담 데이터 통합 배치 REST API.
 *
 * <ul>
 *   <li>{@code POST /api/historical/run}    — 배치 시작 (백그라운드 스레드)</li>
 *   <li>{@code GET  /api/historical/status} — 날짜별 처리 현황 조회</li>
 *   <li>{@code GET  /api/historical/health} — 실행 중 여부 확인</li>
 * </ul>
 */
@Slf4j
@RestController
@RequestMapping("/api/historical")
@RequiredArgsConstructor
public class HistoricalBatchController {

    private final HistoricalBatchService batchService;
    private final HistoricalBatchProperties properties;
    private final HistoricalBatchRepository checkpointRepo;
    private final MongoTemplate mongoTemplate;
    private final ElasticsearchClient elasticsearchClient;

    /**
     * 배치를 백그라운드 스레드로 실행한다.
     *
     * <p>이미 실행 중이면 409를 반환한다.
     */
    @PostMapping("/run")
    public ResponseEntity<Map<String, Object>> runBatch(
            @RequestParam(required = false) Integer dailyCount,
            @RequestParam(required = false) Integer outboundRatio,
            @RequestParam(required = false, defaultValue = "false") boolean reset) {

        int resolvedCount = (dailyCount != null) ? dailyCount : properties.getDailyCount();
        if (resolvedCount < 1 || resolvedCount > 100) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "invalid_parameter",
                    "message", "dailyCount는 1 이상 100 이하여야 합니다. (요청값: " + resolvedCount + ")"
            ));
        }

        int resolvedRatio = (outboundRatio != null) ? outboundRatio : properties.getOutboundRatio();
        if (resolvedRatio < 0 || resolvedRatio > 100) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "invalid_parameter",
                    "message", "outboundRatio는 0 이상 100 이하여야 합니다. (요청값: " + resolvedRatio + ")"
            ));
        }

        if (batchService.isRunning()) {
            return ResponseEntity.status(409).body(Map.of(
                    "status", "already_running",
                    "message", "배치가 이미 실행 중입니다. /api/historical/status 에서 진행 현황을 확인하세요."
            ));
        }

        final int finalCount = resolvedCount;
        final int finalRatio = resolvedRatio;
        int outboundPerDay = Math.round((float) finalCount * finalRatio / 100);
        int inboundPerDay  = finalCount - outboundPerDay;

        final boolean finalReset = reset;
        Thread batchThread = new Thread(() -> {
            try {
                log.info("[HistoricalBatch] 백그라운드 배치 시작");
                String result = batchService.runBatch(finalCount, finalRatio, finalReset);
                log.info("[HistoricalBatch] 배치 완료: {}", result);
            } catch (Exception e) {
                log.error("[HistoricalBatch] 배치 중 예외 발생", e);
            }
        }, "historical-batch");
        batchThread.setDaemon(true);
        batchThread.start();

        return ResponseEntity.accepted().body(Map.of(
                "status", "started",
                "message", "배치가 백그라운드에서 시작됐습니다. /api/historical/status 에서 진행 현황을 확인하세요.",
                "config", Map.of(
                        "startDate", properties.getStartDate().toString(),
                        "endDate", properties.getEndDate().toString(),
                        "dailyCount", finalCount,
                        "outboundRatio", finalRatio,
                        "inboundPerDay", inboundPerDay,
                        "outboundPerDay", outboundPerDay,
                        "chunkSize", properties.getChunkSize(),
                        "note", "인바운드·아웃바운드 모두 AI 추출(result_event_status)·요약(summary_event_status) 트리거 생성."
                )
        ));
    }

    /** 날짜별 처리 현황을 반환한다. */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        List<Map<String, Object>> rows = checkpointRepo.findAllStatus();

        long total      = rows.size();
        long completed  = rows.stream().filter(r -> "COMPLETED".equals(r.get("status"))).count();
        long failed     = rows.stream().filter(r -> "FAILED".equals(r.get("status"))).count();
        long inProgress = rows.stream().filter(r -> "IN_PROGRESS".equals(r.get("status"))).count();
        long pending    = rows.stream().filter(r -> "PENDING".equals(r.get("status"))).count();

        return ResponseEntity.ok(Map.of(
                "running", batchService.isRunning(),
                "summary", Map.of(
                        "total", total,
                        "completed", completed,
                        "inProgress", inProgress,
                        "failed", failed,
                        "pending", pending
                ),
                "details", rows
        ));
    }

    /** 배치 실행 중 여부만 빠르게 확인한다. */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of(
                "running", batchService.isRunning()
        ));
    }

    /**
     * 상담 데이터 전체 초기화 — MySQL 상담 관련 테이블 TRUNCATE + MongoDB consultation_summary 삭제 + ES 도큐먼트 삭제.
     * consultation_results auto_increment가 1로 리셋된다.
     * historical_batch_log도 초기화되므로 이후 /run 호출 시 전 기간 재생성된다.
     */
    @PostMapping("/reset")
    public ResponseEntity<Map<String, Object>> resetAll() {
        if (batchService.isRunning()) {
            return ResponseEntity.status(409).body(Map.of(
                    "status", "error",
                    "message", "배치가 실행 중입니다. 완료 후 reset을 호출하세요."
            ));
        }
        try {
            checkpointRepo.truncateAllConsultationData();
            log.info("[HistoricalBatch] MySQL 상담 테이블 TRUNCATE 완료");

            mongoTemplate.dropCollection("consultation_summary");
            log.info("[HistoricalBatch] MongoDB consultation_summary 삭제 완료");

            for (String index : new String[]{"consult-search-index", "consult-keyword-index"}) {
                boolean exists = elasticsearchClient.indices().exists(r -> r.index(index)).value();
                if (exists) {
                    DeleteByQueryResponse res = elasticsearchClient.deleteByQuery(
                            DeleteByQueryRequest.of(r -> r.index(index).query(q -> q.matchAll(m -> m))));
                    log.info("[HistoricalBatch] ES {} 도큐먼트 {}건 삭제 완료", index, res.deleted());
                }
            }

            return ResponseEntity.ok(Map.of(
                    "status", "ok",
                    "message", "상담 데이터 초기화 완료. consultation_results auto_increment = 1. /api/historical/run으로 재생성하세요."
            ));
        } catch (Exception e) {
            log.error("[HistoricalBatch] reset 실패", e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "status", "error",
                    "message", e.getMessage()
            ));
        }
    }
}
