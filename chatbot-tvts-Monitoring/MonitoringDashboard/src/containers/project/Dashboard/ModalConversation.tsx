import { Divider, Modal, Spin, Table, Tag } from "antd";
import { WechatOutlined } from "@ant-design/icons"
import type { TableProps } from 'antd';
import React, { Dispatch, useEffect, useState } from "react";
import SingleChat from "@components/SingleChat/SingleChat";
import dayjs, { Dayjs } from "dayjs";
import { copyToClipboard, showNotification } from "@shared/utils/commonUtils";
import { getConversationById } from "@services/conversationService";
import { RecordDto } from "@models/recordDto";
import { ConversationDto } from "@models/conversationDto";

interface DataType {
  conversationId: string | undefined
  isOpen: boolean
  setIsOpen: Dispatch<React.SetStateAction<boolean>>
}

const ModalConversation: React.FC<DataType> = ({conversationId, isOpen, setIsOpen}) => {
  const [conversation, setConversation] = useState<ConversationDto>();
  const [records, setRecords] = useState<RecordDto[]>([]);
  const [isLoadingRecords, setIsLoadingRecords] = useState<boolean>(false);

  useEffect(() => {
    initData();
  }, [conversationId]);

  const initData = async () => {
    if (conversationId) {
      getConversations(conversationId);
    }
  }

  const getConversations = async (conversationId: string) => {
    try {
      setIsLoadingRecords(true);
      const result = await getConversationById(conversationId);
      setConversation(result);
      setRecords(result.records);
    }
    catch (e) {
      showNotification("error", "Can not get conversations detail")
    }
    finally {
      setIsLoadingRecords(false);
    }
  }

  const closeModal = () => {
    setIsOpen(false);
  }

  return (
    <Modal
    open={isOpen}
    onCancel={closeModal}
    onClose={closeModal}
    onOk={closeModal}
    width={800}>
      <div className="flex flex-col gap-4 p-8">

        {/* Conversation ID */}
        <div className="flex gap-1">
          <div>Conversation:</div> 
          <Tag className="cursor-pointer" onClick={() => copyToClipboard(conversationId ?? "")}>{conversationId}</Tag>
        </div>

        {/* List record chat */}
        {
          isLoadingRecords 
          ? <Spin className="mt-8" />
          : <>
              {
                records.map((record, index) => {
                  return <div key={index} className="flex flex-col gap-4">
                    <Divider className="m-4" />

                    {/* Record headline */}
                    <div className="flex">

                      {/* Record ID */}
                      <div className="flex gap-1">
                        <div>Record:</div> 
                        <Tag className="cursor-pointer" onClick={() => copyToClipboard(record.id)}>{record.id}</Tag>
                      </div>

                      {/* CSAT Score */}
                      {
                        record.is_rated 
                        ? <Tag color="success"><b>CSAT:</b> {record.rate.csat}</Tag>
                        : <Tag color="error">Not rated</Tag>
                      }
                    </div>

                    {/* Question from user */}
                    <SingleChat isbot={false} text={record.main_input} createdAt={record.start_time} />

                    {/* Answer from chatbot */}
                    <SingleChat isbot={true} text={record.main_output} />

                    {/* Other scores */}
                    {
                      record.is_rated && <div className="flex flex-wrap">
                        <Tag><b>Answer Relevance:</b> {record.rate.answer_relevance}</Tag>
                        <Tag><b>Context Relevance:</b> {record.rate.context_relevance}</Tag>
                        <Tag><b>Groundedness:</b> {record.rate.groundedness}</Tag>
                        <Tag><b>Sentiment:</b> {record.rate.sentiment}</Tag>
                      </div>
                    }
                    
                  </div>
                })
              }
            </>
        }
      </div>
    </Modal>

  );
};

export default ModalConversation;
