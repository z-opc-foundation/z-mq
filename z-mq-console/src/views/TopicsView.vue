<template>
  <div class="zmq-page">
    <div class="zmq-card">
      <div class="card-title-row">
        <span class="card-title">Topic 列表</span>
        <div>
          <el-input v-model="searchText" placeholder="搜索 Topic" clearable style="width: 220px; margin-right: 8px;" />
          <el-button type="primary" @click="openCreateDialog">
            <el-icon><Plus /></el-icon>创建 Topic
          </el-button>
          <el-button @click="refresh"><el-icon><Refresh /></el-icon>刷新</el-button>
        </div>
      </div>

      <el-table :data="filteredTopics" stripe border>
        <el-table-column prop="topic" label="Topic 名" min-width="180" />
        <el-table-column prop="readQueueNums" label="读队列" width="100" align="right" />
        <el-table-column prop="writeQueueNums" label="写队列" width="100" align="right" />
        <el-table-column label="权限" width="100" align="center">
          <template #default="{ row }">
            <el-tag size="small" :type="row.perm === 6 ? 'success' : 'warning'">
              {{ permLabel(row.perm) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="broker" label="Broker" width="160" />
        <el-table-column label="顺序" width="80" align="center">
          <template #default="{ row }">
            <el-tag v-if="row.order" size="small" type="warning">是</el-tag>
            <el-tag v-else size="small" type="info">否</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="180" align="center" fixed="right">
          <template #default="{ row }">
            <el-button type="primary" link size="small" @click="goToMessages(row.topic)">消息</el-button>
            <el-popconfirm :title="`确定删除 ${row.topic}?`" @confirm="onDelete(row.topic)">
              <template #reference>
                <el-button type="danger" link size="small">删除</el-button>
              </template>
            </el-popconfirm>
          </template>
        </el-table-column>
      </el-table>
    </div>

    <el-dialog v-model="createDialogVisible" title="创建 Topic" width="480px">
      <el-form :model="createForm" label-width="100px">
        <el-form-item label="Topic 名">
          <el-input v-model="createForm.topic" placeholder="建议大写 + 下划线" />
        </el-form-item>
        <el-form-item label="读队列数">
          <el-input-number v-model="createForm.readQueueNums" :min="1" :max="64" />
        </el-form-item>
        <el-form-item label="写队列数">
          <el-input-number v-model="createForm.writeQueueNums" :min="1" :max="64" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="createDialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="creating" @click="onCreate">创建</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { useClusterStore } from '@/stores/cluster'
import * as api from '@/api'

const router = useRouter()
const clusterStore = useClusterStore()
const searchText = ref('')

const createDialogVisible = ref(false)
const creating = ref(false)
const createForm = ref({
  topic: '',
  readQueueNums: 4,
  writeQueueNums: 4
})

const filteredTopics = computed(() => {
  const q = searchText.value.trim().toLowerCase()
  if (!q) return clusterStore.topics
  return clusterStore.topics.filter(t => t.topic.toLowerCase().includes(q))
})

function permLabel(perm: number): string {
  if (perm === 6) return '读写'
  if (perm === 4) return '只读'
  if (perm === 2) return '只写'
  return '禁止'
}

async function refresh() {
  await clusterStore.fetchTopics()
}

function openCreateDialog() {
  createForm.value = { topic: '', readQueueNums: 4, writeQueueNums: 4 }
  createDialogVisible.value = true
}

async function onCreate() {
  if (!createForm.value.topic.trim()) {
    ElMessage.warning('请输入 Topic 名')
    return
  }
  creating.value = true
  try {
    await api.createTopic(createForm.value)
    ElMessage.success(`Topic "${createForm.value.topic}" 创建成功`)
    createDialogVisible.value = false
    await refresh()
  } catch (e: any) {
    ElMessage.error('创建失败: ' + (e?.message ?? e))
  } finally {
    creating.value = false
  }
}

async function onDelete(topic: string) {
  await api.deleteTopic(topic)
  ElMessage.success(`Topic "${topic}" 已删除`)
  await refresh()
}

function goToMessages(topic: string) {
  router.push({ path: '/messages', query: { topic } })
}

onMounted(refresh)
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
  color: #303133;
}
</style>