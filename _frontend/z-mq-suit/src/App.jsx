import {Navigate, Route, Routes} from 'react-router-dom'
import {AppLayout} from '../../../../_shared/z-frontend-common-local/dist/z-frontend-common.es.js'
import {menuItems, routeTable} from '@yuku123/z-mq-component/pages'

export default function App() {
    return (
        <Routes>
            <Route path="/" element={<Navigate to="/instance" replace/>}/>
            <Route path="/" element={
                <AppLayout menuItems={menuItems} appTitle="z-mq 消息队列" appShort="MQ" appIcon={{icon: <img src="/icon.png" alt="MQ" style={{width: "100%", height: "100%", objectFit: "cover", borderRadius: 8}}/>, color: '#475569', label: 'MQ'}}/>
            }>
                {routeTable.map((r) => (
                    <Route key={r.path} path={r.path} element={<r.Component/>}/>
                ))}
            </Route>
        </Routes>
    )
}
