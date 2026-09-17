import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import * as api from '@/api'
import type { ClusterOverview, BrokerInfo, TopicInfo, ConsumerGroupInfo } from '@/api'

/**
 * 集群全局状态 — Dashboard / 集群 / Topic 页面共享.
 */
export const useClusterStore = defineStore('cluster', () => {
  const connected = ref(false)
  const overview = ref<ClusterOverview | null>(null)
  const brokers = ref<BrokerInfo[]>([])
  const topics = ref<TopicInfo[]>([])
  const consumerGroups = ref<ConsumerGroupInfo[]>([])
  const lastUpdate = ref<number>(0)

  const totalTopics = computed(() => overview.value?.topicCount ?? topics.value.length)
  const totalBrokers = computed(() => overview.value?.brokerCount ?? brokers.value.length)

  async function fetchOverview() {
    try {
      overview.value = await api.getClusterOverview()
      lastUpdate.value = Date.now()
      connected.value = true
    } catch {
      connected.value = false
    }
  }

  async function fetchBrokers() {
    brokers.value = await api.listBrokers()
  }

  async function fetchTopics() {
    topics.value = await api.listTopics()
  }

  async function fetchConsumerGroups() {
    consumerGroups.value = await api.listConsumerGroups()
  }

  return {
    connected,
    overview,
    brokers,
    topics,
    consumerGroups,
    lastUpdate,
    totalTopics,
    totalBrokers,
    fetchOverview,
    fetchBrokers,
    fetchTopics,
    fetchConsumerGroups
  }
})