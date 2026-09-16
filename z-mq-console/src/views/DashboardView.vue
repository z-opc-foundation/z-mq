<template>
  <div class="zmq-page">
    <el-row :gutter="16" class="stat-row">
      <el-col :xs="24" :sm="12" :md="6">
        <div class="zmq-stat-card">
          <div class="icon-wrap"><el-icon><Cpu /></el-icon></div>
          <div>
            <div class="label">Broker 节点</div>
            <div class="value">{{ overview?.brokerCount ?? '—' }}</div>
            <div class="sub">{{ overview?.namesrvCount ?? 0 }} 个 NameServer</div>
          </div>
        </div>
      </el-col>
      <el-col :xs="24" :sm="12" :md="6">
        <div class="zmq-stat-card" style="background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%);">
          <div class="icon-wrap" style="color: #f59e0b;"><el-icon><Files /></el-icon></div>
          <div>
            <div class="label">Topic 数</div>
            <div class="value">{{ overview?.topicCount ?? '—' }}</div>
            <div class="sub">{{ overview?.consumerGroupCount ?? 0 }} 个消费组</div>
          </div>
        </div>
      </el-col>
      <el-col :xs="24" :sm="12" :md="6">
        <div class="zmq-stat-card" style="background: linear-gradient(135deg, #d1fae5 0%, #a7f3d0 100%);">
          <div class="icon-wrap" style="color: #10b981;"><el-icon><Promotion /></el-icon></div>
          <div>
            <div class="label">入队 TPS</div>
            <div class="value">{{ overview?.messagesPerSecond ?? '—' }}</div>
            <div class="sub">{{ overview ? Math.round(overview.messagesPerMinute).toLocaleString() : '—' }} msg/min</div>
          </div>
        </div>
      </el-col>
      <el-col :xs="24" :sm="12" :md="6">
        <div class="zmq-stat-card" style="background: linear-gradient(135deg, #ede9fe 0%, #ddd6fe 100%);">
          <div class="icon-wrap" style="color: #8b5cf6;"><el-icon><Histogram /></el-icon></div>
          <div>
            <div class="label">总消息数</div>
            <div class="value">{{ overview ? formatCount(overview.totalMessages) : '—' }}</div>
            <div class="sub">存储 {{ overview ? formatBytes(overview.storageBytes) : '—' }}</div>
          </div>
        </div>
      </el-col>
    </el-row>

    <el-row :gutter="16" style="margin-top: 16px;">
      <el-col :xs="24" :md="12">
        <div class="zmq-card">
          <div class="card-title">集群拓扑</div>
          <el-table :data="clusterStore.brokers" stripe>
            <el-table-column prop="brokerName" label="Broker" width="120" />
            <el-table-column prop="cluster" label="Cluster" width="120" />
            <el-table-column prop="addr" label="地址" />
            <el-table-column prop="inTps" label="入 TPS" width="100" align="right" />
            <el-table-column prop="outTps" label="出 TPS" width="100" align="right" />
          </el-table>
        </div>
      </el-col>
      <el-col :xs="24" :md="12">
        <div class="zmq-card">
          <div class="card-title">活跃 Topic TOP 5</div>
          <el-table :data="topTopics" stripe>
            <el-table-column prop="topic" label="Topic" />
            <el-table-column prop="readQueueNums" label="读队列" width="90" align="right" />
            <el-table-column prop="writeQueueNums" label="写队列" width="90" align="right" />
            <el-table-column label="顺序" width="70" align="center">
              <template #default="{ row }">
                <el-tag v-if="row.order" size="small" type="warning">是</el-tag>
                <el-tag v-else size="small" type="info">否</el-tag>
              </template>
            </el-table-column>
          </el-table>
        </div>
      </el-col>
    </el-row>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted } from 'vue'
import { useClusterStore } from '@/stores/cluster'

const clusterStore = useClusterStore()
const overview = computed(() => clusterStore.overview)
const topTopics = computed(() => clusterStore.topics.slice(0, 5))

function formatCount(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M'
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K'
  return n.toString()
}

function formatBytes(n: number): string {
  if (n >= 1024 ** 3) return (n / 1024 ** 3).toFixed(2) + ' GB'
  if (n >= 1024 ** 2) return (n / 1024 ** 2).toFixed(2) + ' MB'
  if (n >= 1024) return (n / 1024).toFixed(2) + ' KB'
  return n + ' B'
}

onMounted(() => {
  clusterStore.fetchBrokers()
  clusterStore.fetchTopics()
})
</script>

<style scoped>
.stat-row { margin-bottom: 16px; }
.card-title {
  font-size: 16px;
  font-weight: 600;
  margin-bottom: 16px;
  color: #303133;
}
</style>