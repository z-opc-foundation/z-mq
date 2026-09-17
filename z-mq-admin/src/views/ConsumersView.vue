<template>
  <div class="zmq-page">
    <div class="zmq-card">
      <div class="card-title-row">
        <span class="card-title">Consumer Group</span>
        <el-button @click="clusterStore.fetchConsumerGroups()">
          <el-icon><Refresh /></el-icon>刷新
        </el-button>
      </div>
      <el-table :data="clusterStore.consumerGroups" stripe border>
        <el-table-column prop="group" label="Group 名" min-width="180" />
        <el-table-column label="状态" width="100" align="center">
          <template #default="{ row }">
            <el-tag :type="stateTagType(row.state)" size="small">{{ row.state }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="consumers" label="消费者数" width="100" align="right" />
        <el-table-column label="订阅 Topic" min-width="220">
          <template #default="{ row }">
            <el-tag v-for="t in row.topics" :key="t" size="small" style="margin-right: 4px;">{{ t }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="积压" width="120" align="right">
          <template #default="{ row }">
            <span :style="{ color: row.totalLag > 100 ? '#f56c6c' : '#67c23a', fontWeight: 500 }">
              {{ row.totalLag.toLocaleString() }}
            </span>
          </template>
        </el-table-column>
        <el-table-column prop="tps" label="TPS" width="100" align="right" />
      </el-table>
      <div v-if="clusterStore.consumerGroups.length === 0" class="empty-tip">
        <el-empty description="暂无 Consumer Group" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onMounted } from 'vue'
import { useClusterStore } from '@/stores/cluster'

const clusterStore = useClusterStore()

function stateTagType(state: string): 'success' | 'danger' | 'info' {
  if (state === 'ONLINE') return 'success'
  if (state === 'OFFLINE') return 'danger'
  return 'info'
}

onMounted(() => clusterStore.fetchConsumerGroups())
</script>

<style scoped>
.card-title-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}
.card-title { font-size: 16px; font-weight: 600; color: #303133; }
.empty-tip { padding: 40px 0; text-align: center; }
</style>