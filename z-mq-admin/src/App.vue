<template>
  <el-container class="zmq-layout">
    <el-aside :width="sidebarWidth" class="zmq-sidebar">
      <div class="zmq-logo">
        <img src="/zmq-logo.svg" alt="zmq" class="logo-icon" />
        <span v-if="!collapsed" class="logo-text">Z-MQ Console</span>
      </div>
      <el-menu
        :default-active="route.path"
        :collapse="collapsed"
        router
        background-color="#001529"
        text-color="#bfcbd9"
        active-text-color="#409eff"
      >
        <el-menu-item index="/dashboard">
          <el-icon><Odometer /></el-icon>
          <template #title>仪表盘</template>
        </el-menu-item>
        <el-menu-item index="/cluster">
          <el-icon><Connection /></el-icon>
          <template #title>集群</template>
        </el-menu-item>
        <el-menu-item index="/topics">
          <el-icon><Files /></el-icon>
          <template #title>Topic 管理</template>
        </el-menu-item>
        <el-menu-item index="/messages">
          <el-icon><Document /></el-icon>
          <template #title>消息查询</template>
        </el-menu-item>
        <el-menu-item index="/consumers">
          <el-icon><User /></el-icon>
          <template #title>Consumer Group</template>
        </el-menu-item>
        <el-menu-item index="/delay">
          <el-icon><Timer /></el-icon>
          <template #title>延迟消息</template>
        </el-menu-item>
        <el-menu-item index="/settings">
          <el-icon><Setting /></el-icon>
          <template #title>系统设置</template>
        </el-menu-item>
      </el-menu>
    </el-aside>

    <el-container>
      <el-header class="zmq-header">
        <div class="header-left">
          <el-button text @click="toggleSidebar">
            <el-icon><Expand v-if="collapsed" /><Fold v-else /></el-icon>
          </el-button>
          <span class="header-title">{{ pageTitle }}</span>
        </div>
        <div class="header-right">
          <el-tooltip content="集群连接状态">
            <el-tag :type="connectionOk ? 'success' : 'danger'" effect="plain" size="small">
              <el-icon style="vertical-align: middle"><CircleCheck v-if="connectionOk" /><CircleClose v-else /></el-icon>
              {{ connectionOk ? '已连接' : '未连接' }}
            </el-tag>
          </el-tooltip>
          <el-tooltip content="刷新">
            <el-button text @click="refresh">
              <el-icon><Refresh /></el-icon>
            </el-button>
          </el-tooltip>
        </div>
      </el-header>

      <el-main class="zmq-main">
        <router-view v-slot="{ Component }">
          <transition name="fade" mode="out-in">
            <component :is="Component" />
          </transition>
        </router-view>
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useRoute } from 'vue-router'
import { useClusterStore } from '@/stores/cluster'

const route = useRoute()
const clusterStore = useClusterStore()

const collapsed = ref(false)
const sidebarWidth = computed(() => collapsed.value ? '64px' : '220px')

const pageTitle = computed(() => {
  const titles: Record<string, string> = {
    '/dashboard': '仪表盘',
    '/cluster': '集群节点',
    '/topics': 'Topic 管理',
    '/messages': '消息查询',
    '/consumers': 'Consumer Group',
    '/delay': '延迟消息',
    '/settings': '系统设置'
  }
  return titles[route.path] || 'Z-MQ'
})

const connectionOk = computed(() => clusterStore.connected)
let timer: number | null = null

function toggleSidebar() {
  collapsed.value = !collapsed.value
}

async function refresh() {
  await clusterStore.fetchOverview()
}

onMounted(() => {
  clusterStore.fetchOverview()
  // 周期刷新
  timer = window.setInterval(() => clusterStore.fetchOverview(), 10_000)
})

onUnmounted(() => {
  if (timer !== null) window.clearInterval(timer)
})
</script>

<style scoped>
.zmq-layout { height: 100vh; }

.zmq-sidebar {
  background: #001529;
  transition: width 0.2s;
  overflow-x: hidden;
}

.zmq-logo {
  height: var(--zmq-header-height);
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 0 16px;
  color: #fff;
  font-weight: 600;
  font-size: 16px;
  border-bottom: 1px solid #1f2d3d;
}
.logo-icon { width: 28px; height: 28px; }
.logo-text { white-space: nowrap; }

.el-menu { border-right: none; }

.zmq-header {
  background: #fff;
  border-bottom: 1px solid #ebeef5;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 16px;
  height: var(--zmq-header-height);
}
.header-left { display: flex; align-items: center; gap: 12px; }
.header-right { display: flex; align-items: center; gap: 12px; }
.header-title { font-size: 16px; font-weight: 500; color: #303133; }

.zmq-main {
  background: #f5f7fa;
  padding: 0;
  overflow: auto;
}

.fade-enter-active, .fade-leave-active { transition: opacity 0.2s; }
.fade-enter-from, .fade-leave-to { opacity: 0; }
</style>