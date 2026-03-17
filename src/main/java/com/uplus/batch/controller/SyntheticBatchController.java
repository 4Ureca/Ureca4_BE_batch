package com.uplus.batch.controller;

import com.uplus.batch.synthetic.OutboundConsultationFactory;
import com.uplus.batch.synthetic.SyntheticConsultationFactory;
import com.uplus.batch.synthetic.SyntheticPersonMatcher;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * 합성 데이터 생성 배치 수동 실행 API.
 *
 * <p>POST /synthetic/batch/run
 *
 * <pre>
 * 파라미터
 *   outboundRatio  아웃바운드 비율 (0~100, default: 30)
 *                    예) 30 → batchSize 중 30%는 아웃바운드, 70%는 인바운드로 생성
 *   batchSize      회차당 생성 건수  (1~100,   default: 5)
 *   runCount       배치 실행 횟수    (1~20,    default: 1)
 *   intervalMs     실행 간격 (ms)    (0~60000, default: 0)
 * </pre>
 */
@Slf4j
@RestController
@RequestMapping("/synthetic/batch")
@RequiredArgsConstructor
@Tag(
    name = "합성 데이터 배치",
    description = """
        상담 합성 데이터를 수동으로 생성하는 테스트용 API입니다.
        `outboundRatio`로 인바운드·아웃바운드 비율을 조정합니다.

        **인바운드 생성 흐름**
        ```
        Step 1 (1 트랜잭션)
          consultation_results    — 상담 결과서
          consultation_raw_texts  — 상담 원문 (카테고리·채널별 템플릿)
          client_review / customer_risk_logs / consult_product_logs
        Step 2 (항상)
          result_event_status REQUESTED → ExtractionScheduler → Gemini API
        Step 3 (항상)
          summary_event_status REQUESTED → ConsultationSummaryGenerator → MongoDB
        ```

        **아웃바운드 생성 흐름**
        ```
        Step 1 (1 트랜잭션)
          consultation_results    — 아웃바운드 결과서 (CHN 카테고리, CALL 채널)
          consultation_raw_texts  — 아웃바운드 원문 (상담사가 먼저 전화)
          client_review / customer_risk_logs
        Step 2 (항상)
          result_event_status REQUESTED → ExtractionScheduler → Gemini API
        Step 3 (항상)
          summary_event_status REQUESTED → ConsultationSummaryGenerator → MongoDB
        ```
        """
)
public class SyntheticBatchController {

    private final SyntheticConsultationFactory factory;
    private final OutboundConsultationFactory outboundFactory;
    private final SyntheticPersonMatcher personMatcher;

    @Operation(summary = "상담사·고객 캐시 갱신", description = "SyntheticPersonMatcher의 상담사·고객 목록을 DB에서 다시 로드합니다. 서버 재시작 없이 employees 테이블 변경 사항을 반영할 때 사용합니다.")
    @PostMapping("/refresh-persons")
    public ResponseEntity<String> refreshPersons() {
        personMatcher.init();
        return ResponseEntity.ok("상담사 " + personMatcher.getAgents().size() + "명, 고객 "
                + personMatcher.getCustomers().size() + "명 재로드 완료");
    }

    @Operation(
        summary = "합성 상담 데이터 생성 (인바운드·아웃바운드 비율 지정)",
        description = """
            `outboundRatio`로 인바운드·아웃바운드 비율을 지정하여 합성 상담 데이터를 N회 생성합니다.

            **outboundRatio 동작**
            - `0`  : 전체 인바운드
            - `100`: 전체 아웃바운드
            - `30` : batchSize 중 30%는 아웃바운드, 나머지 70%는 인바운드 (기본값)

            인바운드·아웃바운드 모두 항상 Gemini AI 추출이 트리거됩니다.
            ExtractionScheduler(60초)가 result_event_status=REQUESTED를 감지하여 처리합니다.
            """
    )
    @ApiResponses({
        @ApiResponse(
            responseCode = "200",
            description = "생성 성공",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = BatchRunResult.class),
                examples = @ExampleObject(
                    name = "outboundRatio=30, batchSize=10, runCount=1",
                    value = """
                        {
                          "config": {
                            "outboundRatio": 30,
                            "batchSize": 10,
                            "runCount": 1,
                            "intervalMs": 0
                          },
                          "result": {
                            "totalInserted": 10,
                            "elapsedMs": 820,
                            "completedAt": "2026-03-14 11:22:33",
                            "runs": [
                              {
                                "runNo": 1,
                                "consultIds": [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010],
                                "inboundCount": 7,
                                "outboundCount": 3,
                                "inboundCategoryCodes": ["M_CHN_04", "M_FEE_01", "M_TRB_02", "M_CHN_06", "M_ADD_03", "M_ETC_01", "M_DEV_02"]
                              }
                            ]
                          }
                        }
                        """
                )
            )
        ),
        @ApiResponse(
            responseCode = "400",
            description = "파라미터 범위 오류",
            content = @Content(
                mediaType = "application/json",
                examples = @ExampleObject(
                    value = "{ \"error\": \"outboundRatio는 0~100 사이여야 합니다. 입력값: 150\" }"
                )
            )
        )
    })
    @PostMapping("/run")
    public ResponseEntity<?> run(
            @Parameter(
                description = "아웃바운드 비율 (0~100). 나머지는 인바운드로 생성됩니다. 기본값 30 = 아웃바운드 30% / 인바운드 70%.",
                example = "30",
                schema = @Schema(type = "integer", minimum = "0", maximum = "100", defaultValue = "30")
            )
            @RequestParam(defaultValue = "30") int outboundRatio,

            @Parameter(
                description = "회차당 생성 건수.",
                example = "5",
                schema = @Schema(type = "integer", minimum = "1", maximum = "100", defaultValue = "5")
            )
            @RequestParam(defaultValue = "5") int batchSize,

            @Parameter(
                description = "배치 실행 횟수.",
                example = "1",
                schema = @Schema(type = "integer", minimum = "1", maximum = "20", defaultValue = "1")
            )
            @RequestParam(defaultValue = "1") int runCount,

            @Parameter(
                description = "회차 간 대기 시간 (밀리초).",
                example = "0",
                schema = @Schema(type = "integer", minimum = "0", maximum = "60000", defaultValue = "0")
            )
            @RequestParam(defaultValue = "0") long intervalMs
    ) throws InterruptedException {

        // ── 입력값 검증 ──────────────────────────────────────────────────────
        if (outboundRatio < 0 || outboundRatio > 100) {
            return ResponseEntity.badRequest()
                    .body(ErrorResponse.of("outboundRatio는 0~100 사이여야 합니다. 입력값: " + outboundRatio));
        }
        if (batchSize < 1 || batchSize > 100) {
            return ResponseEntity.badRequest()
                    .body(ErrorResponse.of("batchSize는 1~100 사이여야 합니다. 입력값: " + batchSize));
        }
        if (runCount < 1 || runCount > 20) {
            return ResponseEntity.badRequest()
                    .body(ErrorResponse.of("runCount는 1~20 사이여야 합니다. 입력값: " + runCount));
        }
        if (intervalMs < 0 || intervalMs > 60_000) {
            return ResponseEntity.badRequest()
                    .body(ErrorResponse.of("intervalMs는 0~60000(ms) 사이여야 합니다. 입력값: " + intervalMs));
        }

        // ── 회차당 인바운드·아웃바운드 건수 계산 ────────────────────────────
        int outboundCount = Math.round((float) batchSize * outboundRatio / 100);
        int inboundCount  = batchSize - outboundCount;

        log.info("[SyntheticBatchController] 수동 실행 시작 — outboundRatio={}%, inbound={}, outbound={}, runCount={}",
                outboundRatio, inboundCount, outboundCount, runCount);

        long startTime = System.currentTimeMillis();
        List<RunDetail> runs = new ArrayList<>(runCount);

        for (int i = 0; i < runCount; i++) {
            int runNo = i + 1;
            log.info("[SyntheticBatchController] 회차 {}/{} 시작 (inbound={}, outbound={})",
                    runNo, runCount, inboundCount, outboundCount);

            List<Long>   allConsultIds        = new ArrayList<>(batchSize);
            List<String> inboundCategoryCodes = null;

            // ── 인바운드 생성 ──────────────────────────────────────────────
            if (inboundCount > 0) {
                SyntheticConsultationFactory.BatchResult inbound = factory.executeStep1(inboundCount);
                allConsultIds.addAll(inbound.consultIds());
                inboundCategoryCodes = inbound.categoryCodes();

                factory.triggerAiExtraction(inbound.consultIds(), inbound.categoryCodes());
                factory.triggerExcellentScoring(inbound.consultIds());
                factory.triggerSummaryGeneration(inbound.consultIds());
            }

            // ── 아웃바운드 생성 ────────────────────────────────────────────
            if (outboundCount > 0) {
                OutboundConsultationFactory.BatchResult outbound = outboundFactory.executeStep1(outboundCount);
                allConsultIds.addAll(outbound.consultIds());
                outboundFactory.triggerAiExtraction(outbound.consultIds(), outbound.categoryCodes());
                outboundFactory.triggerExcellentScoring(outbound.consultIds());
                outboundFactory.triggerSummaryGeneration(outbound.consultIds());
            }

            runs.add(new RunDetail(runNo, allConsultIds, inboundCount, outboundCount,
                    inboundCategoryCodes));
            log.info("[SyntheticBatchController] 회차 {}/{} 완료 — 총 {}건 생성", runNo, runCount, batchSize);

            if (i < runCount - 1 && intervalMs > 0) {
                Thread.sleep(intervalMs);
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        int total = runs.stream().mapToInt(r -> r.consultIds().size()).sum();

        log.info("[SyntheticBatchController] 전체 완료 — 총 {}건, 소요 {}ms", total, elapsed);

        return ResponseEntity.ok(new BatchRunResult(
                new RunConfig(outboundRatio, batchSize, runCount, intervalMs),
                new RunSummary(total, elapsed,
                        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                        runs)
        ));
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  응답 스키마
    // ─────────────────────────────────────────────────────────────────────────

    @Schema(description = "배치 실행 결과 전체")
    record BatchRunResult(

            @Schema(description = "실행 설정값")
            RunConfig config,

            @Schema(description = "실행 결과")
            RunSummary result
    ) {}

    @Schema(description = "실행 설정값")
    record RunConfig(

            @Schema(description = "아웃바운드 비율 (0~100)", example = "30")
            int outboundRatio,

            @Schema(description = "회차당 생성 건수", example = "10")
            int batchSize,

            @Schema(description = "배치 실행 횟수", example = "1")
            int runCount,

            @Schema(description = "회차 간 대기 시간 (ms)", example = "0")
            long intervalMs
    ) {}

    @Schema(description = "실행 결과 요약")
    record RunSummary(

            @Schema(description = "총 생성된 상담 건수", example = "10")
            int totalInserted,

            @Schema(description = "전체 소요 시간 (ms)", example = "820")
            long elapsedMs,

            @Schema(description = "완료 시각 (yyyy-MM-dd HH:mm:ss)", example = "2026-03-14 11:22:33")
            String completedAt,

            @Schema(description = "회차별 상세 결과")
            List<RunDetail> runs
    ) {}

    @Schema(description = "회차별 실행 상세")
    record RunDetail(

            @Schema(description = "회차 번호 (1부터 시작)", example = "1")
            int runNo,

            @Schema(description = "생성된 consult_id 전체 목록 (인바운드 + 아웃바운드)",
                    example = "[1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010]")
            List<Long> consultIds,

            @Schema(description = "이번 회차에서 생성된 인바운드 건수", example = "7")
            int inboundCount,

            @Schema(description = "이번 회차에서 생성된 아웃바운드 건수", example = "3")
            int outboundCount,

            @Schema(description = "인바운드 카테고리 코드 목록. outboundRatio=100이면 null.",
                    example = "[\"M_CHN_04\", \"M_FEE_01\"]", nullable = true)
            List<String> inboundCategoryCodes
    ) {}

    @Schema(description = "파라미터 오류 응답")
    record ErrorResponse(

            @Schema(description = "오류 메시지")
            String error
    ) {
        static ErrorResponse of(String msg) { return new ErrorResponse(msg); }
    }
}
