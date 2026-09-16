<template>
  <div class="zmq-page">
    <div class="zmq-card">
      <div class="card-title">系统设置</div>
      <el-form :model="settings" label-width="180px" style="max-width: 720px;">
        <el-form-item label="Broker Admin 地址">
          <el-input v-model="settings.brokerAdminUrl" placeholder="http://localhost:9090" />
          <div class="form-tip">后端 z-mq-broker-admin REST 服务地址, Vite proxy 会把 /api/* 转发到这里</div>
        </el-form-item>
        <el-form-item label="NameServer 地址">
          <el-input v-model="settings.namesrvAddr" placeholder="localhost:9876" />
        </el-form-item>
        <el-form-item label="默认读队列数">
          <el-input-number v-model="settings.defaultReadQueueNums" :min="1" :max="64" />
        </el-form-item>
        <el-form-item label="默认写队列数">
          <el-input-number v-model="settings.defaultWriteQueueNums" :min="1" :max="64" />
        </el-form-item>
        <el-form-item label="刷新间隔 (秒)">
          <el-input-number v-model="settings.refreshIntervalSec" :min="5" :max="300" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="onSave">保存</el-button>
          <el-button @click="onReset">重置</el-button>
        </el-form-item>
      </el-form>
    </div>

    <div class="zmq-card" style="margin-top: 16px;">
      <div class="card-title">关于 Z-MQ Console</div>
      <el-descriptions :column="2" border>
        <el-descriptions-item label="项目">Z-MQ Console</el-descriptions-item>
        <el-descriptions-item label="版本">v1.0.0-SNAPSHOT</el-descriptions-item>
        <el-descriptions-item label="前端">Vue 3 + Vite + Element Plus + Pinia</el-descriptions-item>
        <el-descriptions-item label="后端">z-mq-broker + z-mq-nameserver</el-descriptions-item>
        <el-descriptions-item label="架构">对标 Apache RocketMQ Console</el-descriptions-item>
        <el-descriptions-item label="License">Apache 2.0</el-descriptions-item>
      </el-descriptions>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { ElMessage } from 'element-plus'

const SETTINGS_KEY = 'zmq-console-settings'

const defaults = {
  brokerAdminUrl: 'http://localhost:9090',
  namesrvAddr: 'localhost:9876',
  defaultReadQueueNums: 4,
  defaultWriteQueueNums: 4,
  refreshIntervalSec: 10
}

const settings = ref(loadSettings())

function loadSettings() {
  try {
    const stored = localStorage.getItem(SETTINGS_KEY)
    if (stored) {
      return { ...defaults, ...JSON.parse(stored) }
    }
  } catch {
    // ignore
  }
  return { ...defaults }
}

function onSave() {
  localStorage.setItem(SETTINGS_KEY, JSON.stringify(settings.value))
  ElMessage.success('设置已保存')
}

function onReset() {
  settings.value = { ...defaults }
  localStorage.removeItem(SETTINGS_KEY)
  ElMessage.info('已恢复默认设置')
}
</script>

<style scoped>
.card-title {
  font-size: 16px;
  font-weight: 600;
  margin-bottom: 16px;
  color: #303133;
}
.form-tip {
  font-size: 12px;
  color: #909399;
  margin-top: 4px;
  line-height: 1.4;
}
</style>