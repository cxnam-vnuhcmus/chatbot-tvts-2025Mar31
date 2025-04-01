import { ReactNode } from "react"

export enum TextType {
    Title = "Title",
    Subtitle = "Subtitle",
    Normal = "Normal",
    Small = "Small",
    ExtraSmall = "ExtraSmall"
}

interface DataType {
    type: TextType
    children: ReactNode
}

const MyText: React.FC<DataType> = ({ type, children }) => {
    switch (type) {
        case TextType.Title:
            return <div className="text-xl font-bold">{children}</div>
        case TextType.Subtitle:
            return <div className="text-lg font-bold">{children}</div>
        case TextType.Normal:
            return <div className="text-base">{children}</div>
        case TextType.Small:
            return <div className="text-sm">{children}</div>
        case TextType.ExtraSmall:
            return <div className="text-xs">{children}</div>
        default:
            return <div>{children}</div>
    }
}
export default MyText;
