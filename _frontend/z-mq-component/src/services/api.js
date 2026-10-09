/**
 * z-mq API client：走 /mq/** 面（MqProxyController 同 JVM 应答）。
 */
import {createRequest} from '@yuku123/z-frontend-common'

const request = createRequest({baseURL: '', tokenKey: 'zmq_token'})

export default request

export function configureMq(config) {
    if (config && config.apiBase !== undefined) {
        request.defaults.baseURL = config.apiBase
    }
}

export const mqApi = {
    instance: () => request.get('/mq/__instance'),
    cluster: () => request.get('/mq/cluster'),
    topics: () => request.get('/mq/topics'),
    route: (topic) => request.get('/mq/route', {params: {topic}}),
    brokerTopics: (brokerName) => request.get('/mq/broker/topics', {params: brokerName ? {brokerName} : {}}),
    brokerTopicConfig: (brokerName) => request.get('/mq/broker/topic-config', {params: brokerName ? {brokerName} : {}}),
    brokerConsumerOffset: (brokerName) => request.get('/mq/broker/consumer-offset', {params: brokerName ? {brokerName} : {}}),
    brokerSubscription: (brokerName) => request.get('/mq/broker/subscription', {params: brokerName ? {brokerName} : {}}),
}
