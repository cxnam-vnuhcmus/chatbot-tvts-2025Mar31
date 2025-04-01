import React from "react";
import { RobotOutlined } from "@ant-design/icons"
import { UserOutlined } from "@ant-design/icons"
import dayjs from "dayjs";
import { DatetimeFormat } from "@shared/const/DatetimeFormat";

interface DataType {
    isbot: boolean
    text: string
    createdAt?: Date
}
  

const SingleChat: React.FC<DataType> = ({isbot, text, createdAt}) => {
    return (
        <div>
            <div className="flex items-start gap-6">
                {/* Image */}
                <div className={`flex items-center justify-center  w-8 h-8 rounded-full ${isbot ? "bg-yellow-200" : "bg-gray-300"}`}>
                    { isbot ? <RobotOutlined /> : <UserOutlined />}
                </div>

                {/* Chat box */}
                <div className={`flex-1 p-4 rounded ${isbot ? "bg-yellow-100" : "bg-gray-200"}`}>
                    <div className="flex flex-col gap-2">
                        <div>{text}</div>
                        <div className="text-xs text-gray-500">{ createdAt ? dayjs(createdAt).format(DatetimeFormat.DDMMYYY_HHMMSS) : ""}</div>
                    </div>
                </div>
            </div>
        </div>
    )
}
export default SingleChat;
