<template>
  <div class="zmq-page">
    <div class="zmq-card">
      <div class="card-title">消息查询</div>
      <el-form :inline="true" :model="queryForm">
        <el-form-item label="Topic">
          <el-select v-model="queryForm.topic" placeholder="选择 Topic" filterable style="width: 220px;">
            <el-option v-for="t in clusterStore.topics" :key="t.topic" :label="t.topic" :value="t.topic" />
          </el-select>
        </el-form-item>
        <el-form-item label="队列 ID">
          <el-input-number v-model="queryForm.queueId" :min="0" :max="63" />
        </el-form-item>
        <el-form-item label="起始 Offset">
          <el-input-number v-model="queryForm.offset" :min="0" />
        </el-form-item>
        <el-form-item label="最多条数">
          <el-input-number v-model="queryForm.maxNum" :min="1" :max="64" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="loading" @click="onQuery">查询</el-button>
        </el-form-item>
      </el-form>
    </div>

    <div class="zmq-card" style="margin-top: 16px;">
      <div class="card-title-row">
        <span class="card-title">消息列表 ({{ messages.length }} 条)</span>
        <div>
          <el-button :disabled="!messages.length" @click="messages = []">清空</el-button>
        </div>
      </div>
      <el-table :data="messages" stripe border>
        <el-table-column prop="msgId" label="消息 ID" width="180" />
        <el-table-column prop="topic" label="Topic" width="160" />
        <el-table-column prop="queueId" label="队列" width="80" align="right" />
        <el-table-column prop="queueOffset" label="Offset" width="100" align="right" />
        <el-table-column prop="tags" label="Tag" width="100" />
        <el-table-column prop="keys" label="Key" width="160" />
        <el-table-column label="Born 时间" width="180">
          <template #default="{ row }">{{ formatTime(row.bornTimestamp) }}</template>
        </el-table-column>
        <el-table-column label="内容" min-width="280">
          <template #default="{ row }">
            <el-tooltip :content="row.body" placement="top" :show-after="500">
              <span class="body-text">{{ row.body }}</span>
            </el-tooltip>
          </template>
        </el-table-column>
      </el-table>
      <div v-if="!loading && messages.length === 0" class="empty-tip">
        <el-empty description="暂无查询结果" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, watch } from 'vue'
import { useRoute } from 'vue-router'
import { useClusterStore } from '@/stores/cluster'
import * as api from '@/api'

const route = useRoute()
const clusterStore = useClusterStore()

const queryForm = ref({
  topic: '',
  queueId: 0,
  offset: 0,
  maxNum: 16
})
const messages = ref<api.MessageItem[]>([])
const loading = ref(false)

function formatTime(ts: number): string {
  if (!ts) return '-'
  const d = new Date(ts)
  return d.toLocaleString('zh-CN', { hour12: false })
}

async function onQuery() {
  if (!queryForm.value.topic) {
    return
  }
  loading.value = true
  try {
    messages.value = await api.queryMessages(queryForm.value)
  } finally {
    loading.value = false
  }
}

// 监听路由 query.topic 自动填入
watch(() => route.query.topic, (t) => {
  if (t && typeof t === 'string') {
    queryForm.value.topic = t
    onQuery()
  }
}, { immediate: false })

onMounted(async () => {
  await clusterStore.fetchTopics()
  if (clusterStore.topics.length > 0 && !queryForm.value.topic) {
    queryForm.value.topic = clusterStore.topics[0].topic
  }
  if (route.query.topic && typeof route.query.topic === 'string') {
    queryForm.value.topic = route.query.topic
  }
  onQuery()
})
</script>

<style scoped>
.card-title-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}
.card-title {
  font-size: 16px;
  font-weight: 600;
  margin-bottom: 16px;
  color: #303133;
}
.body-text {
  display: inline-block;
  max-width: 280px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  vertical-align: middle;
}
.empty-tip { padding: 40px 0; text-align: center; }
</style>