package com.uplus.batch.domain.extraction.dto;
import java.util.List;

public record AiExtractionResponse(
    boolean has_intent,
    String complaint_reason,
    boolean defense_attempted,
    boolean defense_success,
    List<String> defense_actions,
    String raw_summary
) {
	public AiExtractionResponse {
        if (defense_actions == null) defense_actions = java.util.Collections.emptyList();
    }
}