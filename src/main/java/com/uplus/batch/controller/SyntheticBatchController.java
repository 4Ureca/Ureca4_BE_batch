package com.uplus.batch.controller;

import com.uplus.batch.synthetic.SyntheticConsultationFactory;
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
 *   batchSize   회차당 생성 건수    (1~100,  default: 5)
 *   runCount    배치 실행 횟수      (1~20,   default: 1)
 *   intervalMs  실행 간격 (ms)      (0~60000, default: 0)
 *   useAiApi    AI API 사용 여부    (default: false)
 *                 true  → result_event_status REQUESTED 생성 → ExtractionScheduler가 Gemini 호출
 *                 false → result_event_status 미생성 → AI 호출 없이 summary만 PENDING 생성
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

        **생성 흐름 (1회 실행 기준)**

        ```
        Step 1 (항상 실행, 1 트랜잭션)
          consultation_results    — 상담 결과서 (실제 emp_id·customer_id 사용)
          consultation_raw_texts  — 상담 원문 (카테고리·채널별 템플릿 + 응대품질 8항목 무작위 주입)
          client_review           — 고객 만족도 평가 (~70% 확률)
          customer_risk_logs      — 위험 감지 로그 (~40% 확률)
          consult_product_logs    — 상품 변경 이력 (~80% 확률)

        Step 2 (useAiApi=true 일 때만)
          result_event_status REQUESTED 생성
          → ExtractionScheduler(60초 폴링)가 감지하여 Gemini API 호출
          → retention_analysis 저장

        Step 3 (항상 실행)
          summary_event_status REQUESTED 생성
          → ConsultationSummaryGenerator(1분 cron)가 감지하여 MongoDB 저장
          → AI 완료 시 summary.status=COMPLETED, 미완료 시 PENDING
        ```
        """
)
public class SyntheticBatchController {

    private final SyntheticConsultationFactory factory;

    @Operation(
        summary = "합성 상담 데이터 생성",
        description = """
            지정한 설정으로 합성 상담 데이터를 N회 생성합니다.

            **파라미터 조합 예시**

            | 목적 | batchSize | runCount | intervalMs | useAiApi |
            |------|-----------|----------|------------|----------|
            | 빠른 기능 테스트 | 3 | 1 | 0 | false |
            | AI 파이프라인 검증 | 5 | 1 | 0 | true |
            | 대량 데이터 적재 | 20 | 5 | 1000 | false |
            | 실 운영 시뮬레이션 | 20 | 3 | 5000 | true |

            **useAiApi 상세**
            - `false` : Gemini API를 호출하지 않습니다. MongoDB의 `summary.status`는 `PENDING`으로 저장되며, `retryPendingSummaries`가 AI 완료 후 자동으로 `COMPLETED`로 갱신합니다. 비용 없이 전체 파이프라인 구조를 검증할 때 사용합니다.
            - `true` : `result_event_status`에 `REQUESTED` 행을 생성합니다. 이후 `ExtractionScheduler`(60초 폴링)가 Gemini API를 호출하여 `retention_analysis`에 AI 한줄요약을 저장합니다. **실제 API 비용이 발생합니다.**

            **원문 생성 규칙**
            - 카테고리(CHN/FEE/TRB/DEV/ADD/ETC)별 시나리오 템플릿에서 무작위 선택
            - CHN 카테고리는 15% 확률로 비CHN 카테고리에도 해지 시나리오 주입
            - 응대품질 8항목(공감응대·사과표현·친절응대·신속응대·정확응대·대기안내·감사인사·마무리인사)이 30%/35%/35% 비율로 무작위 주입
            - CALL/CHATTING 채널별 인사말·맺음말 분리 적용
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
                    name = "batchSize=5, runCount=2, useAiApi=false 응답 예시",
                    value = """
                        {
                          "config": {
                            "batchSize": 5,
                            "runCount": 2,
                            "intervalMs": 500,
                            "useAiApi": false
                          },
                          "result": {
                            "totalInserted": 10,
                            "elapsedMs": 724,
                            "completedAt": "2026-03-10 14:32:11",
                            "runs": [
                              {
                                "runNo": 1,
                                "consultIds": [1005, 1006, 1007, 1008, 1009],
                                "categoryCodes": ["M_CHN_04", "M_FEE_01", "M_TRB_02", "M_CHN_06", "M_ADD_03"],
                                "aiExtractTriggered": false
                              },
                              {
                                "runNo": 2,
                                "consultIds": [1010, 1011, 1012, 1013, 1014],
                                "categoryCodes": ["M_FEE_05", "M_CHN_01", "M_TRB_08", "M_DEV_03", "M_ETC_01"],
                                "aiExtractTriggered": false
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
                    value = """
                        { "error": "batchSize는 1~100 사이여야 합니다. 입력값: 999" }
                        """
                )
            )
        )
    })
    @PostMapping("/run")
    public ResponseEntity<?> run(
            @Parameter(
                description = "회차당 생성 건수. 1회 실행 시 MySQL 5개 테이블에 이 수만큼 레코드가 삽입됩니다.",
                example = "5",
                schema = @Schema(type = "integer", minimum = "1", maximum = "100", defaultValue = "5")
            )
            @RequestParam(defaultValue = "5") int batchSize,

            @Parameter(
                description = "배치 실행 횟수. runCount × batchSize 건이 총 생성됩니다.",
                example = "1",
                schema = @Schema(type = "integer", minimum = "1", maximum = "20", defaultValue = "1")
            )
            @RequestParam(defaultValue = "1") int runCount,

            @Parameter(
                description = "회차 간 대기 시간 (밀리초). 연속 실행 시 DB 부하 조절에 사용합니다. 0이면 즉시 다음 회차를 실행합니다.",
                example = "0",
                schema = @Schema(type = "integer", minimum = "0", maximum = "60000", defaultValue = "0")
            )
            @RequestParam(defaultValue = "0") long intervalMs,

            @Parameter(
                description = """
                    Gemini AI API 호출 여부.
                    - false: AI 호출 없음. summary.status=PENDING으로 저장. 비용 발생 없음. (테스트 권장)
                    - true: Gemini API 실제 호출. ExtractionScheduler가 60초 이내에 감지하여 처리. 비용 발생.
                    """,
                example = "false",
                schema = @Schema(type = "boolean", defaultValue = "false")
            )
            @RequestParam(defaultValue = "false") boolean useAiApi
    ) throws InterruptedException {

        // ── 입력값 검증 ──────────────────────────────────────────────────
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

        log.info("[SyntheticBatchController] 수동 실행 시작 — batchSize={}, runCount={}, intervalMs={}, useAiApi={}",
                batchSize, runCount, intervalMs, useAiApi);

        long startTime = System.currentTimeMillis();
        List<RunDetail> runs = new ArrayList<>(runCount);

        for (int i = 0; i < runCount; i++) {
            int runNo = i + 1;
            log.info("[SyntheticBatchController] 회차 {}/{} 시작", runNo, runCount);

            // Step 1: consultation_results + raw_texts + 연관 테이블 (1 TX)
            SyntheticConsultationFactory.BatchResult step1 = factory.executeStep1(batchSize);

            // Step 2: AI 추출 트리거 (선택)
            if (useAiApi) {
                factory.triggerAiExtraction(step1.consultIds(), step1.categoryCodes());
            }

            // Step 3: 요약 생성 트리거 (항상 실행)
            factory.triggerSummaryGeneration(step1.consultIds());

            runs.add(new RunDetail(runNo, step1.consultIds(), step1.categoryCodes(), useAiApi));
            log.info("[SyntheticBatchController] 회차 {}/{} 완료 — {}건 생성", runNo, runCount, batchSize);

            // 실행 간격 대기 (마지막 회차 제외)
            if (i < runCount - 1 && intervalMs > 0) {
                Thread.sleep(intervalMs);
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        int total = runs.stream().mapToInt(r -> r.consultIds().size()).sum();

        log.info("[SyntheticBatchController] 전체 완료 — 총 {}건, 소요 {}ms", total, elapsed);

        return ResponseEntity.ok(new BatchRunResult(
                new RunConfig(batchSize, runCount, intervalMs, useAiApi),
                new RunSummary(total, elapsed,
                        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                        runs)
        ));
    }

    // ─────────────────────────────────────────────────────────────────────
    //  응답 스키마
    // ─────────────────────────────────────────────────────────────────────

    @Schema(description = "배치 실행 결과 전체")
    record BatchRunResult(

            @Schema(description = "실행 설정값")
            RunConfig config,

            @Schema(description = "실행 결과")
            RunSummary result
    ) {}

    @Schema(description = "실행 설정값")
    record RunConfig(

            @Schema(description = "회차당 생성 건수", example = "5")
            int batchSize,

            @Schema(description = "배치 실행 횟수", example = "2")
            int runCount,

            @Schema(description = "회차 간 대기 시간 (ms)", example = "500")
            long intervalMs,

            @Schema(description = "AI API 호출 여부", example = "false")
            boolean useAiApi
    ) {}

    @Schema(description = "실행 결과 요약")
    record RunSummary(

            @Schema(description = "총 생성된 상담 건수 (runCount × batchSize)", example = "10")
            int totalInserted,

            @Schema(description = "전체 소요 시간 (ms)", example = "724")
            long elapsedMs,

            @Schema(description = "완료 시각 (yyyy-MM-dd HH:mm:ss)", example = "2026-03-10 14:32:11")
            String completedAt,

            @Schema(description = "회차별 상세 결과")
            List<RunDetail> runs
    ) {}

    @Schema(description = "회차별 실행 상세")
    record RunDetail(

            @Schema(description = "회차 번호 (1부터 시작)", example = "1")
            int runNo,

            @Schema(description = "생성된 consult_id 목록", example = "[1005, 1006, 1007, 1008, 1009]")
            List<Long> consultIds,

            @Schema(description = "회차별 카테고리 코드 목록", example = "[\"M_CHN_04\", \"M_FEE_01\"]")
            List<String> categoryCodes,

            @Schema(description = "AI 추출 트리거 여부 (useAiApi 값과 동일)", example = "false")
            boolean aiExtractTriggered
    ) {}

    @Schema(description = "파라미터 오류 응답")
    record ErrorResponse(

            @Schema(description = "오류 메시지", example = "batchSize는 1~100 사이여야 합니다. 입력값: 999")
            String error
    ) {
        static ErrorResponse of(String msg) { return new ErrorResponse(msg); }
    }
}
