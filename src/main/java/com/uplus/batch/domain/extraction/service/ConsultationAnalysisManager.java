package com.uplus.batch.domain.extraction.service;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uplus.batch.domain.extraction.dto.AiExtractionResponse;
import com.uplus.batch.domain.extraction.dto.QualityScoringResponse;
import com.uplus.batch.domain.extraction.entity.*;
import com.uplus.batch.domain.extraction.repository.*;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class ConsultationAnalysisManager {

    private final GeminiExtractor geminiExtractor;
    private final GeminiQualityScorer geminiQualityScorer;
    private final ConsultationRawTextRepository rawTextRepository;
    private final ConsultationExtractionRepository extractionRepository;
    private final ConsultationEvaluationRepository evaluationRepository;
    private final ResultEventStatusRepository resultEventRepository;
    private final ExcellentEventStatusRepository excellentEventRepository;
    private final ManualRepository manualRepository;
    private final AnalysisCodeRepository analysisCodeRepository;
    private final ObjectMapper objectMapper;

    @Qualifier("geminiTaskExecutor")
    private final Executor geminiTaskExecutor;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processIntegratedBundledTasks(List<TaskPair> taskPairs) {
        if (taskPairs == null || taskPairs.isEmpty()) return;

        // [STEP 1] 분석 태스크 상태 선점 및 초기 저장
        taskPairs.forEach(pair -> { pair.summaryTask().start(); pair.scoringTask().start(); });
        resultEventRepository.saveAll(taskPairs.stream().map(TaskPair::summaryTask).toList());
        excellentEventRepository.saveAll(taskPairs.stream().map(TaskPair::scoringTask).toList());

        // [STEP 2] 카테고리별 그룹화 및 분석 코드 구성
        Map<String, List<TaskPair>> groups = taskPairs.stream()
                .collect(Collectors.groupingBy(pair -> getGroupType(pair.summaryTask().getCategoryCode())));
        Map<String, String> validCodes = getAnalysisCodesFromDb();

        groups.forEach((groupType, pairs) -> {
            try {
                log.info("[Batch] {} 그룹 분석 시작 ({}건)", groupType, pairs.size());

                // [STEP 3] 원문 데이터 벌크 조회 및 전처리(절삭)
                List<Long> consultIds = pairs.stream().map(p -> p.summaryTask().getConsultId()).toList();
                Map<Long, ConsultationRawText> rawDataMap = rawTextRepository.findAllByConsultIdIn(consultIds)
                        .stream().collect(Collectors.toMap(ConsultationRawText::getConsultId, r -> r));

                String manualContent = manualRepository.findByCategoryCodeAndIsActiveTrue(pairs.get(0).summaryTask().getCategoryCode())
                        .map(Manual::getContent).orElse("기본 매뉴얼");
                
                List<String> processedRawTexts = pairs.stream()
                        .map(p -> {
                            ConsultationRawText raw = rawDataMap.get(p.summaryTask().getConsultId());
                            if (raw == null) throw new RuntimeException("원문 데이터 부재: " + p.summaryTask().getConsultId());
                            String text = raw.getRawTextJson();
                            return (text != null && text.length() > 10000) ? text.substring(0, 10000) + "..." : text;
                        }).toList();
                
                // [STEP 4] AI 비동기 실행 (요약 번들 및 개별 채점)
                CompletableFuture<List<AiExtractionResponse>> extractionFuture = CompletableFuture.supplyAsync(() ->
                        geminiExtractor.extractBatch(processedRawTexts, groupType, validCodes), geminiTaskExecutor
                );

                List<CompletableFuture<QualityScoringResponse>> scoringFutures = processedRawTexts.stream()
                        .map(text -> CompletableFuture.supplyAsync(() -> geminiQualityScorer.evaluate(text, manualContent), geminiTaskExecutor)
                                .handle((res, ex) -> {
                                    if (ex != null) { 
                                        log.warn("[AI Scoring Error] {}", ex.getMessage());
                                        return null; 
                                    }
                                    return res;
                                })
                        ).toList();

                // [STEP 5] AI 응답 결과 수집 및 검증
                List<AiExtractionResponse> extractionResults;
                try {
                    extractionResults = extractionFuture.orTimeout(180, TimeUnit.SECONDS).join();
                } catch (Exception e) {
                    throw new RuntimeException("AI 요약 응답 시간 초과(3분)");
                }

                if (extractionResults.size() != pairs.size()) throw new RuntimeException("AI 응답 개수 불일치");

                // [STEP 6] 데이터 저장 및 태스크 완료 처리
                for (int i = 0; i < pairs.size(); i++) {
                    TaskPair pair = pairs.get(i);
                    try {
                        QualityScoringResponse scoreRes = scoringFutures.get(i).orTimeout(60, TimeUnit.SECONDS).join();

                        if (scoreRes == null) { 
                            pair.summaryTask().fail("채점 분석 실패");
                            pair.scoringTask().fail("채점 분석 실패");
                            continue; 
                        }

                        saveExtraction(pair.summaryTask().getConsultId(), extractionResults.get(i));
                        saveEvaluation(pair.scoringTask().getConsultId(), scoreRes);
                        pair.summaryTask().complete();
                        pair.scoringTask().complete();

                    } catch (Exception e) {
                        pair.summaryTask().fail("저장/시간초과 오류: " + truncate(e.getMessage()));
                        pair.scoringTask().fail("저장/시간초과 오류: " + truncate(e.getMessage()));
                    }
                }

            } catch (Exception e) {
                log.error("[Group Error] {} 분석 실패: {}", groupType, e.getMessage());
                pairs.forEach(p -> {
                    p.summaryTask().fail("번들링 실패: " + truncate(e.getMessage()));
                    p.scoringTask().fail("번들링 실패: " + truncate(e.getMessage()));
                });
            }
        });

        // [STEP 7] 최종 태스크 상태 DB 동기화
        resultEventRepository.saveAll(taskPairs.stream().map(TaskPair::summaryTask).toList());
        excellentEventRepository.saveAll(taskPairs.stream().map(TaskPair::scoringTask).toList());
    }

    private String getGroupType(String code) {
        if (code.contains("OTB")) return "OUTBOUND";
        if (code.contains("CHN")) return "INBOUND_CHN";
        return "INBOUND_NORMAL";
    }

    private Map<String, String> getAnalysisCodesFromDb() {
        List<String> targets = List.of("complaint_category", "defense_category", "outbound_category");
        return analysisCodeRepository.findAllByClassificationIn(targets).stream()
                .collect(Collectors.groupingBy(AnalysisCode::getClassification,
                        Collectors.mapping(code -> code.getCodeName() + "(" + code.getDescription() + ")", Collectors.joining(", "))));
    }

    private void saveExtraction(Long id, AiExtractionResponse res) throws Exception {
        String callResult = res.outbound_call_result();
        if (callResult != null) {
            String upper = callResult.trim().toUpperCase();
            callResult = (upper.equals("CONVERTED") || upper.equals("REJECTED")) ? upper : null;
        }
        AiExtractionResponse normalized = new AiExtractionResponse(
                res.raw_summary(), res.has_intent(), res.complaint_reason(), res.complaint_category(),
                res.defense_attempted(), res.defense_success(), res.defense_actions(), res.defense_category(),
                callResult, res.outbound_report(), res.outbound_category()
        );
        extractionRepository.save(ConsultationExtraction.builder()
                .consultId(id).res(normalized).actionsJson(objectMapper.writeValueAsString(normalized.defense_actions())).build());
    }

    private void saveEvaluation(Long id, QualityScoringResponse res) {
        evaluationRepository.save(ConsultationEvaluation.builder()
                .consultId(id).score(res.score()).evaluationReason(res.evaluation_reason()).isCandidate(res.is_candidate()).build());
    }

    private String truncate(String msg) {
        return (msg != null && msg.length() > 200) ? msg.substring(0, 200) + "..." : msg;
    }

    public record TaskPair(ResultEventStatus summaryTask, ExcellentEventStatus scoringTask) {}
}