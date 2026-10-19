/**
 * Z-MQ Console API 客户端.
 * <p>
 * 开发环境经 vite proxy 转发到 z-mq-broker-admin (默认 http://localhost:9090);
 * 生产环境前端打进 admin jar 的 static/, 与 REST 同源, 直接用绝对路径.
 */
import axios, { AxiosInstance } from 'axios'

const baseURL = import.meta.env.VITE_API_BASE_URL || (import.meta.env.DEV ? '/api' : '')

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

// ============== API 方法 ==============

export async function getClusterOverview(): Promise<ClusterOverview> {
  return http.get<any, ClusterOverview>('/cluster/overview')
}

export async function listBrokers(): Promise<BrokerInfo[]> {
  return http.get<any, BrokerInfo[]>('/cluster/brokers')
}

export async function listTopics(): Promise<TopicInfo[]> {
  return http.get<any, TopicInfo[]>('/topics')
}

export async function createTopic(req: { topic: string; readQueueNums: number; writeQueueNums: number }): Promise<void> {
  await http.post('/topics', req)
}

export async function deleteTopic(topic: string): Promise<void> {
  await http.delete(`/topics/${encodeURIComponent(topic)}`)
}

export async function queryMessages(req: {
  topic: string
  queueId: number
  offset: number
  count: number
}): Promise<MessageItem[]> {
  return http.post<any, MessageItem[]>('/messages/query', req)
}

export async function listConsumerGroups(): Promise<ConsumerGroupInfo[]> {
  return http.get<any, ConsumerGroupInfo[]>('/consumer/groups')
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