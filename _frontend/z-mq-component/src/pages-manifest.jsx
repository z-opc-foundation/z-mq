import {
    AppstoreOutlined, ClusterOutlined, DashboardOutlined,
} from '@ant-design/icons'
import Instance from './pages/Instance'
import Topics from './pages/Topics'

export const menuItems = [
    {key: '/instance', icon: <DashboardOutlined/>, label: '实例与端口'},
    {key: '/topics', icon: <AppstoreOutlined/>, label: 'Topic 清单'},
]

const routeTable = [
    {path: 'instance', Component: Instance},
    {path: 'topics', Component: Topics},
]
export {routeTable}
export {default as Instance} from './pages/Instance'
export {default as Topics} from './pages/Topics'
