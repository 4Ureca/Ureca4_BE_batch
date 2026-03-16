package com.uplus.batch.config;

import com.uplus.batch.domain.summary.entity.ConsultationSummary;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;

import java.util.List;

/**
 * MongoDB 커스텀 컨버터 설정.
 *
 * <p>기존 MongoDB 문서의 riskFlags 필드가 문자열 배열(["CHURN", "FRAUD"])로
 * 저장된 경우 RiskFlag 객체로 변환하기 위한 컨버터를 등록한다.
 */
@Configuration
public class MongoConfig {

    @Bean
    public MongoCustomConversions mongoCustomConversions() {
        return new MongoCustomConversions(List.of(new StringToRiskFlagConverter()));
    }

    /**
     * "CHURN" 같은 문자열을 RiskFlag{ riskType="CHURN", riskLevel=null }로 변환.
     * 신규 데이터는 Document 형태로 저장되므로 이 컨버터는 구버전 데이터에만 적용된다.
     */
    static class StringToRiskFlagConverter implements Converter<String, ConsultationSummary.RiskFlag> {
        @Override
        public ConsultationSummary.RiskFlag convert(String source) {
            return ConsultationSummary.RiskFlag.builder()
                    .riskType(source)
                    .riskLevel(null)
                    .build();
        }
    }
}
