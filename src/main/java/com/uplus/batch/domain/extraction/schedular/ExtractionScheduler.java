package com.uplus.batch.domain.extraction.schedular;

import java.util.List;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.uplus.batch.domain.extraction.dto.AiExtractionResponse;
import com.uplus.batch.domain.extraction.entity.ConsultationExtraction;
import com.uplus.batch.domain.extraction.entity.ConsultationRawText;
import com.uplus.batch.domain.extraction.entity.EventStatus;
import com.uplus.batch.domain.extraction.entity.ResultEventStatus;
import com.uplus.batch.domain.extraction.repository.ConsultationExtractionRepository;
import com.uplus.batch.domain.extraction.repository.ConsultationRawTextRepository;
import com.uplus.batch.domain.extraction.repository.EventStatusRepository;
import com.uplus.batch.domain.extraction.service.ConsultationExtractionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class ExtractionScheduler {

    private final EventStatusRepository eventRepository;
    private final ConsultationRawTextRepository rawTextRepository;
    private final ConsultationExtractionRepository extractionRepository;
    private final ConsultationExtractionManager extractionManager;
    private final ObjectMapper objectMapper;

    /**
     * 1분마다 실행되는 메인 엔진
     * 전체 트랜잭션을 걸지 않아 개별 작업의 실패가 전체에 영향을 주지 않습니다.
     */
    @Scheduled(fixedDelay = 60000)
    public void executeExtractionJob() {
        log.info("[Batch]추출 스케줄러 엔진 가동 - 대기 데이터 확인 중...");

        List<ResultEventStatus> pendingTasks = eventRepository.findTop50ByStatusOrderByCreatedAtAsc(EventStatus.REQUESTED);

        if (pendingTasks.isEmpty()) {
            log.info("[Batch]추출 처리할 대기 데이터(REQUESTED)가 없습니다.");
            return;
        }

        log.info("[Batch] 추출 프로세스 시작 - 대상: {}건", pendingTasks.size());

        for (ResultEventStatus task : pendingTasks) {
            try {
                processIndividualTask(task);
            } catch (Exception e) {
                log.error("[Critical Error] 상담 ID {} 처리 중 예상치 못한 시스템 오류: {}", task.getConsultId(), e.getMessage());
            }
        }
        log.info("[Batch] 추출 프로세스 종료");
    }

    /**
     * 개별 작업에 대해 새로운 트랜잭션을 시작합니다.
     * 한 건이 실패해도 다른 건의 DB 반영에는 영향을 주지 않습니다.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processIndividualTask(ResultEventStatus task) {
        try {
            // 1. [선점] 상태를 PROCESSING으로 변경하여 중복 처리 방지
            task.start();
            eventRepository.saveAndFlush(task);

            // 2. [데이터 로드] 원본 데이터 조회
            ConsultationRawText rawData = rawTextRepository.findByConsultId(task.getConsultId())
                    .orElseThrow(() -> new RuntimeException("원본 대화 데이터를 찾을 수 없습니다."));

            // 3. [AI 추출] 
            AiExtractionResponse result = extractionManager.runExtraction(
                    task.getCategoryCode(), 
                    rawData.getRawTextJson()
            );

            // 4. [검증] AI 응답 결과가 비어있는지 체크 (raw_summary null 방어)
            if (result.raw_summary() == null || result.raw_summary().isBlank()) {
                throw new RuntimeException("AI가 내용을 생성하지 못했습니다.");
            }

            // 5. [저장] 결과 적재
            String actionsJson = objectMapper.writeValueAsString(result.defense_actions());
            ConsultationExtraction extraction = ConsultationExtraction.builder()
                    .consultId(task.getConsultId())
                    .res(result)
                    .actionsJson(actionsJson)
                    .build();

            extractionRepository.save(extraction);

            // 6. [성공 마감]
            task.complete();
            eventRepository.saveAndFlush(task);
            log.info("[Success] 상담 ID {} 추출 완료", task.getConsultId());

        } catch (Exception e) {
        	log.error("[Task Failed] ID {}: {}", task.getConsultId(), e.getMessage());

            //에러 메시지 방어: DB 컬럼 길이를 넘지 않게 200자에서 자릅니다.
            String errorMsg = e.getMessage();
            if (errorMsg != null && errorMsg.length() > 200) {
                errorMsg = errorMsg.substring(0, 200) + "...";
            }
            
            task.fail(errorMsg);
            eventRepository.saveAndFlush(task); 
        }
    }
}