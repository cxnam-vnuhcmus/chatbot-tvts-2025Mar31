import { Outlet } from "react-router-dom";
import { Breadcrumb, Layout, Menu, theme } from "antd";
import type { MenuProps } from "antd";
import Sider from "antd/es/layout/Sider";
import { useState } from "react";
import { BarChartOutlined, UserOutlined } from '@ant-design/icons';
import VNUHCM_LOGO from "@images/VNUHCM_logo_with_text.png"
import { Content, Header } from "antd/es/layout/layout";

type MenuItem = Required<MenuProps>["items"][number];

const ProjectLayout = () => {
  const [collapsed, setCollapsed] = useState(false);
  const getItem = (
    label: React.ReactNode,
    key: React.Key,
    icon?: React.ReactNode,
    children?: MenuItem[]
  ): MenuItem => {
    return { key, icon, children, label } as MenuItem;
  };

  const items: MenuItem[] = [
    getItem("Dashboard", "1", <BarChartOutlined/>),
    getItem("Option 2", "2", <UserOutlined />, [
      getItem("Child 1", "3"),
      getItem("Child 2", "4"),
    ]),
  ];
  return (
    <>
      <Layout style={{ minHeight: "100vh" }}>
        
        {/* Sider */}
        <Sider 
        collapsible 
        collapsed={collapsed} 
        onCollapse={(value) => setCollapsed(value)}
        className="!bg-white">
          <img src={VNUHCM_LOGO} />
          <Menu defaultSelectedKeys={["1"]} mode="inline" items={items} />
        </Sider>

        <Layout>
          {/* Header */}
          <Header></Header>

          {/* Content */}
          <Content className="p-6">
            <Outlet />
          </Content>

        </Layout>
      </Layout>
    </>
  );
};

export default ProjectLayout;
