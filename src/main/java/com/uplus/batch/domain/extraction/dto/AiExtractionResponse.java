package com.uplus.batch.domain.extraction.dto;
import java.util.List;

public record AiExtractionResponse(
    Boolean has_intent,
    String complaint_reason,
    Boolean defense_attempted,
    Boolean defense_success,
    List<String> defense_actions,
    String raw_summary
) {
	public AiExtractionResponse {
        if (defense_actions == null) defense_actions = java.util.Collections.emptyList();
    }
}