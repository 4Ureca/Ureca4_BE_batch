package com.uplus.batch.common.dummy.dto;

import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.springframework.stereotype.Component;

@Getter
@Component
public class CacheDummy {
  private Map<String, RiskTypeDummyDto> riskTypes;
  private List<String> defenseActions;
  private List<String> contractTypes;

  private List<String> homeProductCodes;
  private List<String> mobileProductCodes;
  private List<String> additionalProductCodes;

  public void initialize(
      Map<String, RiskTypeDummyDto> riskTypes,
      List<String> defenseActions,
      List<String> contractTypes,
      List<String> homeProductCodes,
      List<String> mobileProductCodes,
      List<String> additionalProductCodes
  ) {
    this.riskTypes = riskTypes;
    this.defenseActions = defenseActions;
    this.contractTypes = contractTypes;
    this.homeProductCodes = homeProductCodes;
    this.mobileProductCodes = mobileProductCodes;
    this.additionalProductCodes = additionalProductCodes;
  }
}