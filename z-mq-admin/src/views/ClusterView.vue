<template>
  <div class="zmq-page">
    <div class="zmq-card">
      <div class="card-title">Broker 节点</div>
      <el-table :data="clusterStore.brokers" stripe border>
        <el-table-column prop="brokerName" label="Broker 名" width="160" />
        <el-table-column prop="cluster" label="Cluster" width="160" />
        <el-table-column label="类型" width="80" align="center">
          <template #default="{ row }">
            <el-tag :type="row.brokerId === 0 ? 'success' : 'info'" size="small">
              {{ row.brokerId === 0 ? 'Master' : 'Slave' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="addr" label="地址" width="180" />
        <el-table-column prop="version" label="版本" width="180" />
        <el-table-column prop="queues" label="队列数" width="100" align="right" />
        <el-table-column prop="topics" label="Topic 数" width="100" align="right" />
        <el-table-column prop="producerCount" label="Producer" width="100" align="right" />
        <el-table-column prop="consumerCount" label="Consumer" width="100" align="right" />
        <el-table-column prop="inTps" label="入 TPS" width="100" align="right">
          <template #default="{ row }">
            <span style="color: #10b981; font-weight: 500">{{ row.inTps }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="outTps" label="出 TPS" width="100" align="right">
          <template #default="{ row }">
            <span style="color: #f59e0b; font-weight: 500">{{ row.outTps }}</span>
          </template>
        </el-table-column>
        <el-table-column label="心跳" width="100" align="center">
          <template #default="{ row }">
            <el-tag v-if="Date.now() - row.lastUpdateMs < 60_000" type="success" size="small">正常</el-tag>
            <el-tag v-else type="danger" size="small">离线</el-tag>
          </template>
        </el-table-column>
      </el-table>
      <div v-if="clusterStore.brokers.length === 0" class="empty-tip">
        <el-empty description="暂无 Broker 节点" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onMounted } from 'vue'
import { useClusterStore } from '@/stores/cluster'

const clusterStore = useClusterStore()

onMounted(() => clusterStore.fetchBrokers())
</script>

<style scoped>
.card-title {
  font-size: 16px;
  font-weight: 600;
  margin-bottom: 16px;
  color: #303133;
}
.empty-tip {
  padding: 40px 0;
  text-align: center;
}
</style>