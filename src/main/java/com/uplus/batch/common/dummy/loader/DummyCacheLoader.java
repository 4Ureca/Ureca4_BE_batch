package com.uplus.batch.common.dummy.loader;

import com.uplus.batch.common.dummy.dto.CacheDummy;
import com.uplus.batch.common.dummy.dto.RiskTypeDummyDto;
import com.uplus.batch.common.dummy.repository.ProductAdditionalRepository;
import com.uplus.batch.common.dummy.repository.ProductHomeRepository;
import com.uplus.batch.common.dummy.repository.ProductMobileRepository;
import com.uplus.batch.common.dummy.repository.RiskTypeRepository;
import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Profile("!test")
@Component
@RequiredArgsConstructor
public class DummyCacheLoader {

  private final CacheDummy cacheDummy;
  private final RiskTypeRepository riskTypeRepository;
  private final ProductHomeRepository homeRepository;
  private final ProductMobileRepository mobileRepository;
  private final ProductAdditionalRepository additionalRepository;

  @PostConstruct
  public void load() {
    List<RiskTypeDummyDto> riskTypeList =
        riskTypeRepository.findActiveRiskTypes();
    List<String> defenseActions = List.of("요금할인", "재약정유도", "단말지원금", "결합상품제안");
    List<String> contractTypes =
        List.of("NEW", "CANCEL", "CHANGE", "RENEW");
    List<String> productTypes =
        List.of("mobile", "home", "additional");

    Map<String, RiskTypeDummyDto> riskTypeMap =
        riskTypeList.stream()
            .collect(Collectors.toMap(
                RiskTypeDummyDto::getCode,
                Function.identity()
            ));
    List<String> homeCodes = homeRepository.findAllCodes();
    List<String> mobileCodes = mobileRepository.findAllCodes();
    List<String> additionalCodes = additionalRepository.findAllCodes();

    cacheDummy.initialize(
        riskTypeMap,
        defenseActions,
        contractTypes,
        homeCodes,
        mobileCodes,
        additionalCodes
    );
  }
}