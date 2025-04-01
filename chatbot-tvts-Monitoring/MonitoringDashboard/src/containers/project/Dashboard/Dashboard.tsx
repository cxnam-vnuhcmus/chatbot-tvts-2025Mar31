import { Table, Tooltip } from "antd";
import { WechatOutlined } from "@ant-design/icons"
import type { TableProps } from 'antd';
import { useEffect, useState } from "react";
import ModalConversation from "./ModalConversation";
import { ConversationDto } from "@models/conversationDto";
import { getAllConversations } from "@services/conversationService";
import { copyToClipboard, formatNumber, showNotification } from "@shared/utils/commonUtils";
import dayjs from "dayjs";
import { DatetimeFormat } from "@shared/const/DatetimeFormat";
import MyText, { TextType } from "@components/MyText/MyText";

const Dashboard: React.FC = () => {
  const [isOpenModalConversation, setIsOpenModalConversation] = useState<boolean>(false);
  const [selectedConversationId, setSelectedConversationId] = useState<string>();
  const [conversations, setConversations] = useState<ConversationDto[]>();
  const [isLoadingConversations, setIsLoadingConversations] = useState<boolean>(false);

  const columns: TableProps<ConversationDto>['columns'] = [
    { 
      title: "Id", 
      width: "100px", 
      key: "id", 
      dataIndex: "id",
      render: (_, record) => (
        <Tooltip title={record.id}>
          <div className="line-clamp-2 cursor-pointer" onClick={() => copyToClipboard(record.id)}>{record.id}</div>
        </Tooltip> 
      )  
    },
    { 
      title: "Input", 
      width: "20%", 
      key: "first_input", 
      dataIndex: "first_input",
      render: (_, record) => (
        <Tooltip title={record.first_input}>
          <div className="line-clamp-2">{record.first_input}</div>
        </Tooltip> 
      ) 
    },
    { 
      title: "Ouput", 
      width: "40%", 
      key: "first_output", 
      dataIndex: "first_output",
      render: (_, record) => (
        <Tooltip title={record.first_output} overlayClassName={"max-w-[400px]"}>
          <div className="line-clamp-2">{record.first_output}</div>
        </Tooltip> 
      ) 
    },
    { 
      title: "Avg. CSAT", 
      width: "100px",
      key: "avg_csat", 
      dataIndex: "avg_csat", 
      render: (_, record) => formatNumber(record.avg_csat)
    },
    { 
      title: "Start time", 
      width: "160px",
      key: "start_time", 
      dataIndex: "start_time",
      render: (_, record) => dayjs(record.start_time).format(DatetimeFormat.DDMMYYY_HHMMSS)
    },
    {
      title: "", 
      key: "action", 
      dataIndex: "action",
      render: (_, record) => (
        <div className="text-lg cursor-pointer" onClick={() => onConversationClick(record.id)}>
          <WechatOutlined />
        </div>
      )
    },
  ];

  useEffect(() => {
    initData();
  }, []);

  const initData = async () => {
    getConversations();
  }

  const getConversations = async () => {
    try {
      setIsLoadingConversations(true);
      const result = await getAllConversations();
      setConversations(result);
      console.log(result);
    }
    catch (e) {
      showNotification("error", "Can not get conversations")
    }
    finally {
      setIsLoadingConversations(false);
    }
  }

  const onConversationClick = (conversationId: string) => {
    setSelectedConversationId(conversationId);
    openModalConversation();

  }

  const openModalConversation = () => {
    setIsOpenModalConversation(true);
  }

  return (
    <>
    <div className="flex flex-col gap-4">
      <MyText type={TextType.Title}>Conversations</MyText>
      
      <Table 
        loading={isLoadingConversations}
        dataSource={conversations} 
        columns={columns} />;
    </div>

    <ModalConversation
      conversationId={selectedConversationId}
      isOpen={isOpenModalConversation}
      setIsOpen={setIsOpenModalConversation} />
    </>

  );
};

export default Dashboard;
