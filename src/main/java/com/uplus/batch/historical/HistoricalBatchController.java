package com.uplus.batch.historical;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
}
