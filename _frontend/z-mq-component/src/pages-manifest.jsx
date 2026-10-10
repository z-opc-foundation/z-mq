import { AppstoreOutlined, ClusterOutlined, DashboardOutlined, HomeOutlined } from '@ant-design/icons'
import Instance from './pages/Instance'
import Topics from './pages/Topics'


export {default as Instance} from './pages/Instance'
export {default as Topics} from './pages/Topics'
import HomePage from './pages/HomePage'

/** 菜单 + 路由清单（lead 008 §10/§14/§16 批量落地）。App 壳在 suit 侧组装。 */
export const appMeta = { title: 'z-mq 消息队列', short: 'z-mq' }

export const menuItems = [
    { key: '/z-mq/home', label: '首页', icon: <HomeOutlined /> },
    { key: '/z-mq/instance', label: '实例与端口', icon: <DashboardOutlined /> },
    { key: '/z-mq/topics', label: 'Topic 清单', icon: <AppstoreOutlined /> },
]

export const routeTable = [
    { path: '/z-mq/home', Component: HomePage },
    { path: '/z-mq/instance', Component: Instance },
    { path: '/z-mq/topics', Component: Topics },
]

export { default as HomePage } from './pages/HomePage'
export { default as LoginPage } from './pages/LoginPage'
