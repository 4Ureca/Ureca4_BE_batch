package com.uplus.batch.domain.extraction.service;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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

    /**
     * [통합 번들 프로세서]
     * 50쌍의 태스크를 카테고리별로 묶어 '번들 요약'과 '개별 채점'을 수행합니다.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processIntegratedBundledTasks(List<TaskPair> taskPairs) {
        if (taskPairs == null || taskPairs.isEmpty()) return;

        // 1. 상태 선점 (모든 태스크 PROCESSING 처리)
        taskPairs.forEach(pair -> {
            pair.summaryTask().start();
            pair.scoringTask().start();
        });
        resultEventRepository.saveAll(taskPairs.stream().map(TaskPair::summaryTask).collect(Collectors.toList()));
        excellentEventRepository.saveAll(taskPairs.stream().map(TaskPair::scoringTask).collect(Collectors.toList()));

        // 2. 성격별 그룹화 (프롬프트 오염 방지)
        Map<String, List<TaskPair>> groups = taskPairs.stream()
                .collect(Collectors.groupingBy(pair -> getGroupType(pair.summaryTask().getCategoryCode())));

        Map<String, String> validCodes = getAnalysisCodesFromDb();

        // 3. 그룹별 실행
        groups.forEach((groupType, pairs) -> {
            try {
                log.info("[Batch] {} 그룹 통합 분석 시작 ({}건)", groupType, pairs.size());

                // 원문 데이터 로드
                List<ConsultationRawText> rawDataList = pairs.stream()
                        .map(p -> rawTextRepository.findByConsultId(p.summaryTask().getConsultId())
                                .orElseThrow(() -> new RuntimeException("원문 없음: " + p.summaryTask().getConsultId())))
                        .collect(Collectors.toList());

                // 공통 매뉴얼 로드 (그룹 내 첫 번째 카테고리 기준)
                String manualContent = manualRepository.findByCategoryCodeAndIsActiveTrue(pairs.get(0).summaryTask().getCategoryCode())
                        .map(Manual::getContent).orElse("기본 매뉴얼");

                // 🚀 [A] 요약: 50개를 묶어서 API 1번 호출 (번들 비동기)
                CompletableFuture<List<AiExtractionResponse>> extractionFuture = CompletableFuture.supplyAsync(() ->
                        geminiExtractor.extractBatch(
                                rawDataList.stream().map(ConsultationRawText::getRawTextJson).collect(Collectors.toList()),
                                groupType,
                                validCodes
                        )
                );

                // 🚀 [B] 채점: 각 건별 병렬 호출 (개별 비동기)
                List<CompletableFuture<QualityScoringResponse>> scoringFutures = rawDataList.stream()
                        .map(raw -> CompletableFuture.supplyAsync(() ->
                                geminiQualityScorer.evaluate(raw.getRawTextJson(), manualContent)
                        )).collect(Collectors.toList());

                // 모든 작업 완료 대기
                CompletableFuture.allOf(extractionFuture).join();
                CompletableFuture.allOf(scoringFutures.toArray(new CompletableFuture[0])).join();

                // 4. 리스트 결과 해체 및 개별 저장 
                List<AiExtractionResponse> extractionResults = extractionFuture.get();
                if (extractionResults.size() != pairs.size()) {
                    throw new RuntimeException("AI 응답 개수 불일치 (보낸 건: " + pairs.size() + ", 받은 건: " + extractionResults.size() + ")");
                }
                for (int i = 0; i < pairs.size(); i++) {
                    TaskPair pair = pairs.get(i);
                    
                    // AI가 반환한 배열에서 내 순서(i)에 맞는 결과 하나를 꺼냄
                    AiExtractionResponse extRes = extractionResults.get(i);
                    QualityScoringResponse scoreRes = scoringFutures.get(i).get();

                    // 개별 Row로 저장
                    saveExtraction(pair.summaryTask().getConsultId(), extRes);
                    saveEvaluation(pair.scoringTask().getConsultId(), scoreRes);

                    pair.summaryTask().complete();
                    pair.scoringTask().complete();
                }

            } catch (Exception e) {
                log.error("[Batch Critical Error] {} 그룹 실패: {}", groupType, e.getMessage());
                pairs.forEach(p -> {
                    p.summaryTask().fail(truncate(e.getMessage()));
                    p.scoringTask().fail(truncate(e.getMessage()));
                });
            }
        });

        // 5. 최종 상태 업데이트
        resultEventRepository.saveAll(taskPairs.stream().map(TaskPair::summaryTask).collect(Collectors.toList()));
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
                .collect(Collectors.groupingBy(
                        AnalysisCode::getClassification,
                        Collectors.mapping(
                            code -> code.getCodeName() + "(" + code.getDescription() + ")", 
                            Collectors.joining(", ")
                        )
                ));
    }

    private void saveExtraction(Long id, AiExtractionResponse res) throws Exception {
        ConsultationExtraction entity = ConsultationExtraction.builder()
                .consultId(id)
                .res(res)
                .actionsJson(objectMapper.writeValueAsString(res.defense_actions()))
                .build();
        extractionRepository.save(entity);
    }

    private void saveEvaluation(Long id, QualityScoringResponse res) {
        evaluationRepository.save(ConsultationEvaluation.builder()
                .consultId(id)
                .score(res.score())
                .evaluationReason(res.evaluation_reason())
                .isCandidate(res.is_candidate())
                .build());
    }

    private String truncate(String msg) {
        return (msg != null && msg.length() > 200) ? msg.substring(0, 200) + "..." : msg;
    }

    public record TaskPair(ResultEventStatus summaryTask, ExcellentEventStatus scoringTask) {}
}