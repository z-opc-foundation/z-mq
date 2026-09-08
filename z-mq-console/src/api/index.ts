/**
 * Z-MQ Console API 客户端.
 * <p>
 * 所有请求经 vite proxy 转发到 z-mq-broker-admin (默认 http://localhost:9090).
 * <p>
 * 当前 MVP 提供 mock 数据 — 后端 z-mq-broker-admin REST 接口上线后, 直接修改 baseUrl 即可.
 */
import axios, { AxiosInstance } from 'axios'

// API 基础路径: 开发环境走 vite proxy (/api -> 后端 9090), 生产环境由 nginx 转发
const baseURL = import.meta.env.VITE_API_BASE_URL || '/api'

const http: AxiosInstance = axios.create({
  baseURL,
  timeout: 15_000,
  headers: { 'Content-Type': 'application/json' }
})

http.interceptors.response.use(
  (resp) => resp.data,
  (err) => {
    console.error('[z-mq api]', err)
    return Promise.reject(err)
  }
)

// ============== 类型定义 ==============

export interface ClusterOverview {
  namesrvCount: number
  brokerCount: number
  topicCount: number
  consumerGroupCount: number
  totalMessages: number
  messagesPerMinute: number
  messagesPerSecond: number
  storageBytes: number
}

export interface BrokerInfo {
  brokerName: string
  cluster: string
  brokerId: number
  addr: string
  version: string
  producerCount: number
  consumerCount: number
  queues: number
  topics: number
  inTps: number
  outTps: number
  lastUpdateMs: number
}

export interface TopicInfo {
  topic: string
  readQueueNums: number
  writeQueueNums: number
  perm: number
  broker: string
  order: boolean
}

export interface MessageItem {
  msgId: string
  topic: string
  queueId: number
  queueOffset: number
  bornTimestamp: number
  storeTimestamp: number
  body: string
  keys?: string
  tags?: string
}

export interface ConsumerGroupInfo {
  group: string
  state: 'ONLINE' | 'OFFLINE' | 'UNKNOWN'
  consumers: number
  topics: string[]
  totalLag: number
  tps: number
}

// ============== Mock 数据 (后端 REST 上线后可移除) ==============

const MOCK_OVERVIEW: ClusterOverview = {
  namesrvCount: 1,
  brokerCount: 1,
  topicCount: 5,
  consumerGroupCount: 3,
  totalMessages: 12_345_678,
  messagesPerMinute: 24_567,
  messagesPerSecond: 410,
  storageBytes: 4_294_967_296
}

const MOCK_BROKERS: BrokerInfo[] = [
  {
    brokerName: 'DefaultBroker',
    cluster: 'DefaultCluster',
    brokerId: 0,
    addr: 'localhost:10911',
    version: 'v1.0.0-SNAPSHOT',
    producerCount: 2,
    consumerCount: 3,
    queues: 16,
    topics: 5,
    inTps: 410,
    outTps: 405,
    lastUpdateMs: Date.now()
  }
]

const MOCK_TOPICS: TopicInfo[] = [
  { topic: 'ORDER_TOPIC',     readQueueNums: 4, writeQueueNums: 4, perm: 6, broker: 'DefaultBroker', order: false },
  { topic: 'PAYMENT_TOPIC',   readQueueNums: 4, writeQueueNums: 4, perm: 6, broker: 'DefaultBroker', order: true  },
  { topic: 'NOTIFY_TOPIC',    readQueueNums: 2, writeQueueNums: 2, perm: 6, broker: 'DefaultBroker', order: false },
  { topic: 'DELAY_TOPIC',     readQueueNums: 2, writeQueueNums: 2, perm: 6, broker: 'DefaultBroker', order: false },
  { topic: 'LOG_TOPIC',       readQueueNums: 8, writeQueueNums: 8, perm: 6, broker: 'DefaultBroker', order: false }
]

const MOCK_GROUPS: ConsumerGroupInfo[] = [
  { group: 'ORDER_CONSUMER_GROUP', state: 'ONLINE', consumers: 2, topics: ['ORDER_TOPIC'], totalLag: 12, tps: 200 },
  { group: 'PAYMENT_CONSUMER',     state: 'ONLINE', consumers: 4, topics: ['PAYMENT_TOPIC'], totalLag: 0, tps: 150 },
  { group: 'LOG_CONSUMER',         state: 'OFFLINE', consumers: 0, topics: ['LOG_TOPIC'], totalLag: 9999, tps: 0 }
]

// ============== API 方法 (后端就绪后切到真实实现) ==============

export async function getClusterOverview(): Promise<ClusterOverview> {
  try {
    return await http.get<any, ClusterOverview>('/cluster/overview')
  } catch {
    return MOCK_OVERVIEW
  }
}

export async function listBrokers(): Promise<BrokerInfo[]> {
  try {
    return await http.get<any, BrokerInfo[]>('/cluster/brokers')
  } catch {
    return MOCK_BROKERS
  }
}

export async function listTopics(): Promise<TopicInfo[]> {
  try {
    return await http.get<any, TopicInfo[]>('/topics')
  } catch {
    return MOCK_TOPICS
  }
}

export async function createTopic(req: { topic: string; readQueueNums: number; writeQueueNums: number }): Promise<boolean> {
  try {
    await http.post('/topics', req)
    return true
  } catch {
    // mock
    MOCK_TOPICS.push({
      topic: req.topic,
      readQueueNums: req.readQueueNums,
      writeQueueNums: req.writeQueueNums,
      perm: 6,
      broker: 'DefaultBroker',
      order: false
    })
    MOCK_OVERVIEW.topicCount = MOCK_TOPICS.length
    return true
  }
}

export async function deleteTopic(topic: string): Promise<boolean> {
  try {
    await http.delete(`/topics/${encodeURIComponent(topic)}`)
    return true
  } catch {
    const idx = MOCK_TOPICS.findIndex(t => t.topic === topic)
    if (idx >= 0) {
      MOCK_TOPICS.splice(idx, 1)
      MOCK_OVERVIEW.topicCount = MOCK_TOPICS.length
    }
    return true
  }
}

export async function queryMessages(req: {
  topic: string
  queueId: number
  offset: number
  maxNum: number
}): Promise<MessageItem[]> {
  try {
    return await http.post<any, MessageItem[]>('/messages/query', req)
  } catch {
    return Array.from({ length: Math.min(req.maxNum, 5) }, (_, i) => ({
      msgId: `mock-msg-${req.offset + i}`,
      topic: req.topic,
      queueId: req.queueId,
      queueOffset: req.offset + i,
      bornTimestamp: Date.now() - (req.offset + i) * 1000,
      storeTimestamp: Date.now() - (req.offset + i) * 1000,
      body: `这是 ${req.topic} queue=${req.queueId} offset=${req.offset + i} 的 mock 消息内容`,
      keys: `mock-key-${i}`,
      tags: 'TagA'
    }))
  }
}

export async function listConsumerGroups(): Promise<ConsumerGroupInfo[]> {
  try {
    return await http.get<any, ConsumerGroupInfo[]>('/consumer/groups')
  } catch {
    return MOCK_GROUPS
  }
}

export default {
  getClusterOverview,
  listBrokers,
  listTopics,
  createTopic,
  deleteTopic,
  queryMessages,
  listConsumerGroups
}