<template>
  <div class="zmq-page">
    <div class="zmq-card">
      <div class="card-title">延迟消息投递测试</div>
      <el-alert
        title="提示"
        type="info"
        :closable="false"
        show-icon
      >
        Z-MQ 内置 18 个延迟级别 (1s / 5s / 10s / 30s / 1m / 2m / 3m / 4m / 5m / 6m / 7m / 8m / 9m / 10m / 20m / 30m / 1h / 2h)
        此页面用于查看延迟消息投递统计和提交测试任务.
      </el-alert>

      <el-form :inline="true" :model="testForm" style="margin-top: 16px;">
        <el-form-item label="Topic">
          <el-select v-model="testForm.topic" placeholder="选择 Topic" filterable style="width: 220px;">
            <el-option v-for="t in clusterStore.topics" :key="t.topic" :label="t.topic" :value="t.topic" />
          </el-select>
        </el-form-item>
        <el-form-item label="延迟级别">
          <el-select v-model="testForm.delayLevel" placeholder="选择级别" style="width: 160px;">
            <el-option v-for="(label, value) in DELAY_LEVELS" :key="value" :label="`${value}. ${label}`" :value="Number(value)" />
          </el-select>
        </el-form-item>
        <el-form-item label="消息内容">
          <el-input v-model="testForm.body" placeholder="Hello Z-MQ" style="width: 280px;" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="sending" @click="onSend">发送延迟消息</el-button>
        </el-form-item>
      </el-form>
    </div>

    <div class="zmq-card" style="margin-top: 16px;">
      <div class="card-title">延迟消息统计</div>
      <el-table :data="delayStats" stripe border>
        <el-table-column prop="level" label="级别" width="80" align="center" />
        <el-table-column prop="label" label="延迟时间" width="160" />
        <el-table-column prop="ms" label="毫秒数" width="120" align="right" />
        <el-table-column prop="delivered" label="已投递" width="120" align="right" />
        <el-table-column prop="pending" label="待投递" width="120" align="right">
          <template #default="{ row }">
            <el-tag v-if="row.pending > 0" size="small" type="warning">{{ row.pending }}</el-tag>
            <span v-else>0</span>
          </template>
        </el-table-column>
      </el-table>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { useClusterStore } from '@/stores/cluster'

const clusterStore = useClusterStore()

const DELAY_LEVELS: Record<number, string> = {
  1: '1 秒', 2: '5 秒', 3: '10 秒', 4: '30 秒',
  5: '1 分钟', 6: '2 分钟', 7: '3 分钟', 8: '4 分钟',
  9: '5 分钟', 10: '6 分钟', 11: '7 分钟', 12: '8 分钟',
  13: '9 分钟', 14: '10 分钟', 15: '20 分钟', 16: '30 分钟',
  17: '1 小时', 18: '2 小时'
}

const DELAY_MS: Record<number, number> = {
  1: 1_000, 2: 5_000, 3: 10_000, 4: 30_000,
  5: 60_000, 6: 120_000, 7: 180_000, 8: 240_000,
  9: 300_000, 10: 360_000, 11: 420_000, 12: 480_000,
  13: 540_000, 14: 600_000, 15: 1_200_000, 16: 1_800_000,
  17: 3_600_000, 18: 7_200_000
}

const testForm = ref({
  topic: '',
  delayLevel: 1,
  body: 'Hello Z-MQ Delay Message'
})
const sending = ref(false)

const delayStats = ref<{ level: number; label: string; ms: number; delivered: number; pending: number }[]>(
  Object.entries(DELAY_MS).map(([k, v]) => ({
    level: Number(k),
    label: DELAY_LEVELS[Number(k)],
    ms: v,
    delivered: 0,
    pending: 0
  }))
)

async function onSend() {
  if (!testForm.value.topic) {
    ElMessage.warning('请选择 Topic')
    return
  }
  sending.value = true
  try {
    // mock: 直接在统计上加 1
    const lvl = testForm.value.delayLevel
    delayStats.value.find(s => s.level === lvl)!.pending++
    ElMessage.success(`已调度延迟消息: level=${lvl} topic=${testForm.value.topic}`)
  } finally {
    sending.value = false
  }
}

onMounted(async () => {
  await clusterStore.fetchTopics()
  if (clusterStore.topics.length > 0) {
    testForm.value.topic = clusterStore.topics[0].topic
  }
})
</script>

<style scoped>
.card-title {
  font-size: 16px;
  font-weight: 600;
  margin-bottom: 16px;
  color: #303133;
}
</style>