import axios from "axios"
import { ConversationDto } from "src/models/conversationDto";

const url = import.meta.env.VITE_MONIROTING_EVALUATOR_SERVICE

export const getAllConversations = async (): Promise<ConversationDto[]> => {
    const result = await axios.get(`${url}/conversations`);
    return result.data;
}

export const getConversationById = async (conversationId: string): Promise<ConversationDto> => {
    const result = await axios.get(`${url}/conversations/${conversationId}`);
    return result.data;
}