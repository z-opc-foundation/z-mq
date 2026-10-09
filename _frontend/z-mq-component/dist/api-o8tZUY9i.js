import { createRequest as s } from "@yuku123/z-frontend-common";
const t = s({ baseURL: "", tokenKey: "zmq_token" });
function o(e) {
  e && e.apiBase !== void 0 && (t.defaults.baseURL = e.apiBase);
}
const i = {
  instance: () => t.get("/mq/__instance"),
  cluster: () => t.get("/mq/cluster"),
  topics: () => t.get("/mq/topics"),
  route: (e) => t.get("/mq/route", { params: { topic: e } }),
  brokerTopics: (e) => t.get("/mq/broker/topics", { params: e ? { brokerName: e } : {} }),
  brokerTopicConfig: (e) => t.get("/mq/broker/topic-config", { params: e ? { brokerName: e } : {} }),
  brokerConsumerOffset: (e) => t.get("/mq/broker/consumer-offset", { params: e ? { brokerName: e } : {} }),
  brokerSubscription: (e) => t.get("/mq/broker/subscription", { params: e ? { brokerName: e } : {} })
};
export {
  o as c,
  i as m
};
