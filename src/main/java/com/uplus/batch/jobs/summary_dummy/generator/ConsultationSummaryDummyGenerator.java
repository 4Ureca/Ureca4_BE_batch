package com.uplus.batch.jobs.summary_dummy.generator;

import com.uplus.batch.common.dummy.dto.CacheDummy;
import com.uplus.batch.domain.summary.entity.ConsultationSummary;
import com.uplus.batch.jobs.summary_dummy.dto.ConsultationResultRow;
import java.util.ArrayList;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Component
@RequiredArgsConstructor
public class ConsultationSummaryDummyGenerator {

  private final ThreadLocalRandom random = ThreadLocalRandom.current();
  private final CacheDummy cache;

  public ConsultationSummary generate(ConsultationResultRow row) {

    ConsultationSummary doc = new ConsultationSummary();

    // === 30번 상담결과서 기반 ===
    doc.setConsultId(row.getConsultId());
    doc.setConsultedAt(row.getCreatedAt());
    doc.setChannel(row.getChannel());
    doc.setDurationSec(row.getDurationSec());

    // 상담사
    doc.setAgent(
        ConsultationSummary.Agent.builder()
            .id(row.getEmpId())
            .name(row.getAgentName())
            .build()
    );

    // 고객
    doc.setCustomer(
        ConsultationSummary.Customer.builder()
            .id(row.getCustomerId())
            .name(row.getCustomerName())
            .type(row.getCustomerType())
            .phone(row.getCustomerPhone())
            .grade(row.getGradeCode())
            .ageGroup(calculateAgeGroup(row.getBirthDate()))
            .satisfiledScore(randomSatisfiedScore())
            .build()
    );

    // 카테고리
    doc.setCategory(
        ConsultationSummary.Category.builder()
            .code(row.getCategoryCode())
            .large(row.getCategoryLarge())
            .medium(row.getCategoryMedium())
            .small(row.getCategorySmall())
            .build()
    );

    // IAM (실제 데이터 기반), TODO: 키워드 일치율 미정
    doc.setIam(
        ConsultationSummary.Iam.builder()
            .issue(row.getIamIssue())
            .action(row.getIamAction())
            .memo(row.getIamMemo())
            .matchKeyword(List.of("요금", "할인", "키워드"))
            .matchRates(random.nextDouble(0.3, 0.95))
            .build()
    );
    doc.setRiskFlags(randomRiskFlags());

    // TODO: AI관련 미정
    doc.setSummary(randomSummary());
    doc.setCancellation(randomCancellation());

    doc.setResultProducts(randomResultProducts());
    doc.setCreatedAt(LocalDateTime.now());

    return doc;
  }

  private ConsultationSummary.Summary randomSummary() {

    boolean completed = random.nextInt(100) < 90;

    return ConsultationSummary.Summary.builder()
        .status(completed ? "COMPLETED" : "FAILED")
        .content(completed ? "상담 정상 처리 완료" : null)
        .keywords(List.of("상담", "처리"))
        .build();
  }

  private List<String> randomRiskFlags() {

    if (random.nextInt(100) < 2 &&
        cache.getRiskTypes() != null &&
        !cache.getRiskTypes().isEmpty()) {

      List<String> codes =
          new ArrayList<>(cache.getRiskTypes().keySet());

      String selected =
          codes.get(random.nextInt(codes.size()));

      return List.of(selected);
    }

    return null;
  }

  private ConsultationSummary.Cancellation randomCancellation() {

    boolean intent = random.nextInt(100) < 10;

    List<String> defense = null;

    if (intent &&
        cache.getDefenseActions() != null &&
        !cache.getDefenseActions().isEmpty()) {

      defense = List.of(
          cache.getDefenseActions()
              .get(random.nextInt(cache.getDefenseActions().size()))
      );
    }

    return ConsultationSummary.Cancellation.builder()
        .intent(intent)
        .defenseAttempted(intent && random.nextBoolean())
        .defenseSuccess(intent && random.nextInt(100) < 50)
        .defenseActions(defense)
        .complaintReasons(intent ? "요금 부담 증가" : null)
        .build();
  }

  private String calculateAgeGroup(LocalDate birthDate) {

    if (birthDate == null) return null;

    int age = Period.between(birthDate, LocalDate.now()).getYears();
    int group = (age / 10) * 10;

    return group + "대";
  }

  private Double randomSatisfiedScore() {

    // 60% 확률로 null
    if (random.nextInt(100) < 60) {
      return null;
    }

    // 0.0 ~ 5.0 (소수 1자리 반올림)
    double score = random.nextDouble(0.0, 5.0);
    return Math.round(score * 10) / 10.0;
  }

  private ConsultationSummary.ResultProducts randomResultProducts() {

    String changeType = randomChangeType();

    return switch (changeType) {

      case "NEW" -> buildNewProducts(changeType);

      case "CANCEL" -> buildCancelProducts(changeType);

      case "CHANGE" -> buildConversionProducts(changeType);

      case "RENEW" -> buildRecommitmentProducts(changeType);

      default -> null;
    };
  }

  private String randomChangeType() {

    List<String> types = cache.getContractTypes();

    if (types == null || types.isEmpty()) {
      throw new IllegalStateException("contractTypes not initialized");
    }

    return types.get(random.nextInt(types.size()));
  }

  private ConsultationSummary.ResultProducts buildNewProducts(String type) {

    return ConsultationSummary.ResultProducts.builder()
        .changeType(type)
        .subscribed(List.of(randomAnyProduct()))
        .build();
  }

  private ConsultationSummary.ResultProducts buildCancelProducts(String type) {

    return ConsultationSummary.ResultProducts.builder()
        .changeType(type)
        .canceled(List.of(randomAnyProduct()))
        .build();
  }

  private ConsultationSummary.ResultProducts buildConversionProducts(String type) {

    String oldProduct = randomAnyProduct();
    String newProduct;

    do {
      newProduct = randomAnyProduct();
    } while (newProduct.equals(oldProduct));

    ConsultationSummary.ResultProducts.Conversion conv =
        ConsultationSummary.ResultProducts.Conversion.builder()
            .canceled(oldProduct)
            .subscribed(newProduct)
            .build();

    return ConsultationSummary.ResultProducts.builder()
        .changeType(type)
        .conversion(List.of(conv))
        .build();
  }

  private ConsultationSummary.ResultProducts buildRecommitmentProducts(String type) {

    return ConsultationSummary.ResultProducts.builder()
        .changeType(type)
        .recommitment(List.of(randomAnyProduct()))
        .build();
  }

  private String randomAnyProduct() {

    List<String> pool = new ArrayList<>();

    if (cache.getHomeProductCodes() != null)
      pool.addAll(cache.getHomeProductCodes());

    if (cache.getMobileProductCodes() != null)
      pool.addAll(cache.getMobileProductCodes());

    if (cache.getAdditionalProductCodes() != null)
      pool.addAll(cache.getAdditionalProductCodes());

    if (pool.isEmpty()) {
      throw new IllegalStateException("Product codes not initialized");
    }

    return pool.get(random.nextInt(pool.size()));
  }
}