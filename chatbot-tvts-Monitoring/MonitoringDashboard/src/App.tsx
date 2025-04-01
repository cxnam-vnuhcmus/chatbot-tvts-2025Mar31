import { useRoutes, RouteObject, Navigate } from "react-router-dom";
import LogIn from "@containers/authentication/LogIn/LogIn";
import Dashboard from "@containers/project/Dashboard/Dashboard";
import ProjectLayout from "@containers/project/ProjectLayout";

const AuthenticationRoutes: RouteObject = {
  children: [
    { path: "/login", element: <LogIn /> }
  ],
};

const ProjectRoutes: RouteObject = {
  element: <ProjectLayout />,
  children: [
    { path: "/", element: <Navigate to={"/dashboard"} /> },
    { path: "/dashboard", element: <Dashboard /> },
  ],
};

const ErrorRoutes: RouteObject = { path: "*", element: <Navigate to={"/dashboard"} /> };

const routesConfig: RouteObject[] = [AuthenticationRoutes, ProjectRoutes, ErrorRoutes];

const App = () => {
  const routes = useRoutes(routesConfig);
  return (
    <>
      {routes}
    </>
  );
};

export default App;
