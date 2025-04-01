import { notification } from "antd";

export const isNullOrEmpty = (value: string | undefined | null): boolean => {
  if (value === "" || value == undefined || value == null) return true;
  return false;
};

export const formatNumber = (value: number) => {
  return Number(value).toFixed(4);
};

export const showNotification = (
  type: "success" | "info" | "warning" | "error",
  message: string,
  description?: string,
  duration: number = 4.5
) => {
  notification[type]({
    message: message,
    description: description,
    duration: duration,
  });
};

export const copyToClipboard = (text: string) => {
  var textField = document.createElement("textarea");
  textField.innerText = text;
  document.body.appendChild(textField);
  textField.select();
  document.execCommand("copy");
  textField.remove();
  showNotification("success", "Copied");
};
