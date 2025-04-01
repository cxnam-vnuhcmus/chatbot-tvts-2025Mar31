import { RateDto } from "./rateDto";

export interface RecordDto {
    id: string;
    conversation_id: string;
    record_data: string;
    main_input: string;
    main_output: string;
    start_time: Date;
    is_rated: boolean;
    created_date: Date; 
    rate: RateDto;
}