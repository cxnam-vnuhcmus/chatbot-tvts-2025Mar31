import { RecordDto } from "./recordDto";

export interface ConversationDto {
    id: string;
    avg_csat: number;
    first_input: string;
    first_output: string;
    start_time: Date;
    is_rated: boolean;
    records: RecordDto[];
}