export interface RateDto {
    id: string;
    record_id: string;
    conversation_id: string;
    csat: number;
    groundedness: number;
    answer_relevance: number;
    context_relevance: number;
    sentiment: number;
    created_date: Date; 
}