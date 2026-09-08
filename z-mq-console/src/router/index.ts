import { createRouter, createWebHashHistory } from 'vue-router'

const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    {
      path: '/',
      redirect: '/dashboard'
    },
    {
      path: '/dashboard',
      name: 'Dashboard',
      component: () => import('@/views/DashboardView.vue'),
      meta: { title: '仪表盘' }
    },
    {
      path: '/cluster',
      name: 'Cluster',
      component: () => import('@/views/ClusterView.vue'),
      meta: { title: '集群节点' }
    },
    {
      path: '/topics',
      name: 'Topics',
      component: () => import('@/views/TopicsView.vue'),
      meta: { title: 'Topic 管理' }
    },
    {
      path: '/messages',
      name: 'Messages',
      component: () => import('@/views/MessagesView.vue'),
      meta: { title: '消息查询' }
    },
    {
      path: '/consumers',
      name: 'Consumers',
      component: () => import('@/views/ConsumersView.vue'),
      meta: { title: 'Consumer Group' }
    },
    {
      path: '/delay',
      name: 'Delay',
      component: () => import('@/views/DelayView.vue'),
      meta: { title: '延迟消息' }
    },
    {
      path: '/settings',
      name: 'Settings',
      component: () => import('@/views/SettingsView.vue'),
      meta: { title: '系统设置' }
    }
  ]
})

export default router