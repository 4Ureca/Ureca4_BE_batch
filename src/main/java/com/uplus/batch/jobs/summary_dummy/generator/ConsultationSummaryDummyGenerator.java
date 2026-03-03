package com.uplus.batch.jobs.summary_dummy.generator;

import com.uplus.batch.domain.summary.entity.ConsultationSummary;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

@Component
@RequiredArgsConstructor
public class ConsultationSummaryDummyGenerator {

  private final ThreadLocalRandom random = ThreadLocalRandom.current();

  // consultId unique 보장용
  private final AtomicLong sequence = new AtomicLong(1);

  private static final List<String> CHANNELS =
      List.of("CALL", "CHATTING");

  private static final List<String> AGE_GROUPS =
      List.of("20대", "30대", "40대", "50대", "60대");

  private static final List<String> CUSTOMER_GRADES =
      List.of("VIP", "일반");

  private static final List<String> RISK_TYPES =
      List.of("FRAUD", "MALICIOUS");

  private static final List<String> DEFENSE_ACTIONS =
      List.of("요금할인", "재약정유도", "단말지원금", "결합상품제안");

  private static final List<String> CATEGORIES =
      List.of("요금문의", "해지문의", "단말문의", "결합문의");

  public ConsultationSummary generate() {

    ConsultationSummary doc = new ConsultationSummary();

    doc.setConsultId(sequence.getAndIncrement());
    doc.setConsultedAt(randomDate());
    doc.setChannel(randomChannel());
    doc.setDurationSec(random.nextInt(60, 1800));

    doc.setAgent(randomAgent());
    doc.setCustomer(randomCustomer());
    doc.setCategory(randomCategory());
    doc.setIam(randomIam());
    doc.setSummary(randomSummary());
    doc.setRiskFlags(randomRiskFlags());
    doc.setCancellation(randomCancellation());

    doc.setCreatedAt(LocalDateTime.now());

    return doc;
  }

  private LocalDateTime randomDate() {
    return LocalDateTime.now()
        .minusDays(random.nextInt(0, 180))
        .minusMinutes(random.nextInt(0, 1440));
  }

  private String randomChannel() {
    return random.nextInt(100) < 70 ? "CALL" : "CHATTING";
  }

  private ConsultationSummary.Agent randomAgent() {
    return ConsultationSummary.Agent.builder()
        .id((long) random.nextInt(1, 50))
        .name("상담사" + random.nextInt(1, 50))
        .build();
  }

  private ConsultationSummary.Customer randomCustomer() {

    boolean vip = random.nextInt(100) < 15;

    return ConsultationSummary.Customer.builder()
        .id((long) random.nextInt(1, 100000))
        .type(random.nextBoolean() ? "개인" : "법인")
        .phone("010-" + random.nextInt(1000, 9999)
            + "-" + random.nextInt(1000, 9999))
        .name("고객" + random.nextInt(1, 100000))
        .ageGroup(AGE_GROUPS.get(random.nextInt(AGE_GROUPS.size())))
        .grade(vip ? "VIP" : "일반")
        .satisfiledScore(
            random.nextInt(100) < 70
                ? random.nextDouble(4.0, 5.0)
                : random.nextDouble(2.0, 4.0)
        )
        .build();
  }

  private ConsultationSummary.Category randomCategory() {

    String category = CATEGORIES.get(random.nextInt(CATEGORIES.size()));

    return ConsultationSummary.Category.builder()
        .code("CAT_" + random.nextInt(1, 10))
        .large(category)
        .medium(category + "_중분류")
        .small(category + "_소분류")
        .build();
  }

  private ConsultationSummary.Iam randomIam() {

    return ConsultationSummary.Iam.builder()
        .issue("고객이 요금에 대해 문의함")
        .action("요금제 안내 및 할인 프로모션 설명")
        .memo(random.nextInt(100) < 30 ? "특이사항 없음" : null)
        .matchKeyword(List.of("요금", "할인"))
        .matchRates(random.nextDouble(0.6, 0.95))
        .build();
  }

  private ConsultationSummary.Summary randomSummary() {

    boolean completed = random.nextInt(100) < 90;

    return ConsultationSummary.Summary.builder()
        .status(completed ? "COMPLETED" : "FAILED")
        .content(completed
            ? "요금 문의 상담 후 할인 안내 완료"
            : null)
        .keywords(List.of("요금", "프로모션"))
        .build();
  }

  private List<String> randomRiskFlags() {

    if (random.nextInt(100) < 2) {
      return List.of(RISK_TYPES.get(random.nextInt(RISK_TYPES.size())));
    }
    return null;
  }

  private ConsultationSummary.Cancellation randomCancellation() {

    boolean intent = random.nextInt(100) < 10;

    return ConsultationSummary.Cancellation.builder()
        .intent(intent)
        .defenseAttempted(intent && random.nextBoolean())
        .defenseSuccess(intent && random.nextInt(100) < 50)
        .defenseActions(intent
            ? List.of(DEFENSE_ACTIONS.get(
            random.nextInt(DEFENSE_ACTIONS.size())))
            : null)
        .complaintReasons(intent
            ? "요금 부담 증가"
            : null)
        .build();
  }
}