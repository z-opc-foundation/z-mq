import Ie, { useState as v, useRef as Ae, useEffect as te, useCallback as Y, useMemo as Re } from "react";
import { Routes as Be, Route as H, Navigate as $e } from "react-router-dom";
import { Alert as y, Card as I, Descriptions as Q, Tag as h, Space as z, Button as q, Table as he, Row as be, Col as A, Statistic as B, Select as Me } from "antd";
import { ReloadOutlined as ne } from "@ant-design/icons";
import $ from "@yuku123/z-frontend-common";
import { PageHeader as le, EmptyState as oe, ListTable as fe } from "@yuku123/z-frontend-common/ui";
const U = {
  /** 自省：由 MqProxyController 自己应答，不经过 z-mq，所以内嵌全挂时它也必须能通 */
  instance: () => $.get("/mq/__instance"),
  cluster: () => $.get("/mq/cluster"),
  topics: () => $.get("/mq/topics"),
  route: (e) => $.get("/mq/route", { params: { topic: e } }),
  brokerTopics: (e) => $.get("/mq/broker/topics", { params: e ? { brokerName: e } : {} }),
  brokerTopicConfig: (e) => $.get("/mq/broker/topic-config", { params: e ? { brokerName: e } : {} }),
  brokerConsumerOffset: (e) => $.get("/mq/broker/consumer-offset", { params: e ? { brokerName: e } : {} }),
  brokerDelayOffset: (e) => $.get("/mq/broker/delay-offset", { params: e ? { brokerName: e } : {} }),
  brokerSubscription: (e) => $.get("/mq/broker/subscription", { params: e ? { brokerName: e } : {} }),
  brokerDataVersion: (e) => $.get("/mq/broker/data-version", { params: e ? { brokerName: e } : {} })
}, xe = {
  0: "SUCCESS",
  1: "SYSTEM_ERROR",
  2: "SYSTEM_BUSY",
  3: "REQUEST_CODE_NOT_SUPPORTED",
  4: "TRANSACTION_FAILED"
};
function lr(e) {
  return !!e && e.responseCode === 0;
}
function L(e) {
  var s;
  const t = (s = e == null ? void 0 : e.response) == null ? void 0 : s.data;
  if (t && typeof t == "object") {
    const i = t.message || t.error;
    if (i) return `${i}${t.hint ? ` — ${t.hint}` : ""}`;
    if (t.remark) return `z-mq 返回 responseCode=${t.responseCode} (${xe[t.responseCode] || "?"}) ${t.remark}`;
  }
  return typeof t == "string" && t.trim() ? t.slice(0, 300) : (e == null ? void 0 : e.message) || String(e);
}
function K(e) {
  return !e || e.responseCode === 0 ? null : `上游返回 responseCode=${e.responseCode}（${xe[e.responseCode] || "未知码"}）${e.remark ? `：${e.remark}` : ""}` + (e.responseCode === 3 ? ' —— 这个请求码在 z-mq 当前版本没注册，是"上游没实现"，不是"集群里没有"' : "");
}
function ir(e) {
  return !!(e && (e.attributionUnverified || e.attributionVerified === !1));
}
function cr(e) {
  var s;
  const t = (s = e == null ? void 0 : e.response) == null ? void 0 : s.status;
  return t === 503 || t === 502 ? !0 : /没有任何监听者|不是本 JVM|Address already in use|not bound|Connect/.test(String(L(e)));
}
const qe = {
  "broker/subscription": "BrokerOutAPI.handleGetAllSubscriptionGroup 的源码注释写着「MVP: 订阅组配置暂未单独存储, 返回空 wrapper + 默认版本号」，而且它实际 new 出来的是一个空的 ConsumerOffsetSerializeWrapper（body 里连 subscription 字段都没有）⇒ 这张表恒空，不代表集群里没有订阅组",
  "broker/delay-offset": "源码注释「MVP: 延迟消息 offset 与 Topic 自身 offset 共享 (无 SCHEDULE_TOPIC_XXXX 隔离队列)。返回与 GET_ALL_CONSUMER_OFFSET 一致即可」⇒ 这一格的 offsetTable 和 321 那份是同一份数据，只有 dataVersion 不同",
  "broker/consumer-offset": 'collectOffsetTable 的注释「简化: 遍历可能的 queueId (MVP 用 0-15)」且取的是各 group 的 max ⇒ 显示的是"该 topic 前 16 个队列里出现过的最大 offset"，不是某个消费组的真实位点'
};
function ze(e) {
  if (!e || e.responseCode === 0) return null;
  const t = String(e.remark || "");
  return /not exist/i.test(t) ? t : null;
}
function Se(e) {
  if (!e || typeof e != "object") return "—";
  const t = e.counter ?? e.counterValue, s = e.timestamp ? new Date(e.timestamp).toLocaleString("zh-CN", { hour12: !1 }) : "-";
  return `counter=${t ?? "?"} · timestamp=${s}`;
}
const dr = /^[A-Za-z0-9_\-.:;%#]{1,120}$/;
var ae = { exports: {} }, ee = {};
/**
 * @license React
 * react-jsx-runtime.production.js
 *
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
var we;
function Le() {
  if (we) return ee;
  we = 1;
  var e = Symbol.for("react.transitional.element"), t = Symbol.for("react.fragment");
  function s(i, o, u) {
    var c = null;
    if (u !== void 0 && (c = "" + u), o.key !== void 0 && (c = "" + o.key), "key" in o) {
      u = {};
      for (var m in o)
        m !== "key" && (u[m] = o[m]);
    } else u = o;
    return o = u.ref, {
      $$typeof: e,
      type: i,
      key: c,
      ref: o !== void 0 ? o : null,
      props: u
    };
  }
  return ee.Fragment = t, ee.jsx = s, ee.jsxs = s, ee;
}
var re = {};
/**
 * @license React
 * react-jsx-runtime.development.js
 *
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
var Ce;
function De() {
  return Ce || (Ce = 1, process.env.NODE_ENV !== "production" && (function() {
    function e(n) {
      if (n == null) return null;
      if (typeof n == "function")
        return n.$$typeof === Z ? null : n.displayName || n.name || null;
      if (typeof n == "string") return n;
      switch (n) {
        case a:
          return "Fragment";
        case x:
          return "Profiler";
        case l:
          return "StrictMode";
        case F:
          return "Suspense";
        case J:
          return "SuspenseList";
        case j:
          return "Activity";
        case R:
          return "ViewTransition";
      }
      if (typeof n == "object")
        switch (typeof n.tag == "number" && console.error(
          "Received an unexpected object in getComponentNameFromType(). This is likely a bug in React. Please file an issue."
        ), n.$$typeof) {
          case D:
            return "Portal";
          case w:
            return n.displayName || "Context";
          case k:
            return (n._context.displayName || "Context") + ".Consumer";
          case P:
            var d = n.render;
            return n = n.displayName, n || (n = d.displayName || d.name || "", n = n !== "" ? "ForwardRef(" + n + ")" : "ForwardRef"), n;
          case se:
            return d = n.displayName || null, d !== null ? d : e(n.type) || "Memo";
          case G:
            d = n._payload, n = n._init;
            try {
              return e(n(d));
            } catch {
            }
        }
      return null;
    }
    function t(n) {
      return "" + n;
    }
    function s(n) {
      try {
        t(n);
        var d = !1;
      } catch {
        d = !0;
      }
      if (d) {
        d = console;
        var g = d.error, T = typeof Symbol == "function" && Symbol.toStringTag && n[Symbol.toStringTag] || n.constructor.name || "Object";
        return g.call(
          d,
          "The provided key is an unsupported type %s. This value must be coerced to a string before using it here.",
          T
        ), t(n);
      }
    }
    function i(n) {
      if (n === a) return "<>";
      if (typeof n == "object" && n !== null && n.$$typeof === G)
        return "<...>";
      try {
        var d = e(n);
        return d ? "<" + d + ">" : "<...>";
      } catch {
        return "<...>";
      }
    }
    function o() {
      var n = ue.A;
      return n === null ? null : n.getOwner();
    }
    function u() {
      return Error("react-stack-top-frame");
    }
    function c(n) {
      if (je.call(n, "key")) {
        var d = Object.getOwnPropertyDescriptor(n, "key").get;
        if (d && d.isReactWarning) return !1;
      }
      return n.key !== void 0;
    }
    function m(n, d) {
      function g() {
        ye || (ye = !0, console.error(
          "%s: `key` is not a prop. Trying to access it will result in `undefined` being returned. If you need to access the same value within the child component, you should pass it as a different prop. (https://react.dev/link/special-props)",
          d
        ));
      }
      g.isReactWarning = !0, Object.defineProperty(n, "key", {
        get: g,
        configurable: !0
      });
    }
    function b() {
      var n = e(this.type);
      return ge[n] || (ge[n] = !0, console.error(
        "Accessing element.ref was removed in React 19. ref is now a regular prop. It will be removed from the JSX Element type in a future release."
      )), n = this.props.ref, n !== void 0 ? n : null;
    }
    function S(n, d, g, T, V, M) {
      var E = g.ref;
      return n = {
        $$typeof: f,
        type: n,
        key: d,
        props: g,
        _owner: T
      }, (E !== void 0 ? E : null) !== null ? Object.defineProperty(n, "ref", {
        enumerable: !1,
        get: b
      }) : Object.defineProperty(n, "ref", { enumerable: !1, value: null }), n._store = {}, Object.defineProperty(n._store, "validated", {
        configurable: !1,
        enumerable: !1,
        writable: !0,
        value: 0
      }), Object.defineProperty(n, "_debugInfo", {
        configurable: !1,
        enumerable: !1,
        writable: !0,
        value: null
      }), Object.defineProperty(n, "_debugStack", {
        configurable: !1,
        enumerable: !1,
        writable: !0,
        value: V
      }), Object.defineProperty(n, "_debugTask", {
        configurable: !1,
        enumerable: !1,
        writable: !0,
        value: M
      }), Object.freeze && (Object.freeze(n.props), Object.freeze(n)), n;
    }
    function C(n, d, g, T, V, M) {
      var E = d.children;
      if (E !== void 0)
        if (T)
          if (Ne(E)) {
            for (T = 0; T < E.length; T++)
              O(E[T]);
            Object.freeze && Object.freeze(E);
          } else
            console.error(
              "React.jsx: Static children should always be an array. You are likely explicitly calling React.jsxs or React.jsxDEV. Use the Babel transform instead."
            );
        else O(E);
      if (je.call(d, "key")) {
        E = e(n);
        var W = Object.keys(d).filter(function(Pe) {
          return Pe !== "key";
        });
        T = 0 < W.length ? "{key: someKey, " + W.join(": ..., ") + ": ...}" : "{key: someKey}", Ee[E + T] || (W = 0 < W.length ? "{" + W.join(": ..., ") + ": ...}" : "{}", console.error(
          `A props object containing a "key" prop is being spread into JSX:
  let props = %s;
  <%s {...props} />
React keys must be passed directly to JSX without using spread:
  let props = %s;
  <%s key={someKey} {...props} />`,
          T,
          E,
          W,
          E
        ), Ee[E + T] = !0);
      }
      if (E = null, g !== void 0 && (s(g), E = "" + g), c(d) && (s(d.key), E = "" + d.key), "key" in d) {
        g = {};
        for (var pe in d)
          pe !== "key" && (g[pe] = d[pe]);
      } else g = d;
      return E && m(
        g,
        typeof n == "function" ? n.displayName || n.name || "Unknown" : n
      ), S(
        n,
        E,
        g,
        o(),
        V,
        M
      );
    }
    function O(n) {
      _(n) ? n._store && (n._store.validated = 1) : typeof n == "object" && n !== null && n.$$typeof === G && (n._payload.status === "fulfilled" ? _(n._payload.value) && n._payload.value._store && (n._payload.value._store.validated = 1) : n._store && (n._store.validated = 1));
    }
    function _(n) {
      return typeof n == "object" && n !== null && n.$$typeof === f;
    }
    var p = Ie, f = Symbol.for("react.transitional.element"), D = Symbol.for("react.portal"), a = Symbol.for("react.fragment"), l = Symbol.for("react.strict_mode"), x = Symbol.for("react.profiler"), k = Symbol.for("react.consumer"), w = Symbol.for("react.context"), P = Symbol.for("react.forward_ref"), F = Symbol.for("react.suspense"), J = Symbol.for("react.suspense_list"), se = Symbol.for("react.memo"), G = Symbol.for("react.lazy"), j = Symbol.for("react.activity"), R = Symbol.for("react.view_transition"), Z = Symbol.for("react.client.reference"), ue = p.__CLIENT_INTERNALS_DO_NOT_USE_OR_WARN_USERS_THEY_CANNOT_UPGRADE, je = Object.prototype.hasOwnProperty, Ne = Array.isArray, me = console.createTask ? console.createTask : function() {
      return null;
    };
    p = {
      react_stack_bottom_frame: function(n) {
        return n();
      }
    };
    var ye, ge = {}, ve = p.react_stack_bottom_frame.bind(
      p,
      u
    )(), Te = me(i(u)), Ee = {};
    re.Fragment = a, re.jsx = function(n, d, g) {
      var T = 1e4 > ue.recentlyCreatedOwnerStacks++;
      if (T) {
        var V = Error.stackTraceLimit;
        Error.stackTraceLimit = 10;
        var M = Error("react-stack-top-frame");
        Error.stackTraceLimit = V;
      } else M = ve;
      return C(
        n,
        d,
        g,
        !1,
        M,
        T ? me(i(n)) : Te
      );
    }, re.jsxs = function(n, d, g) {
      var T = 1e4 > ue.recentlyCreatedOwnerStacks++;
      if (T) {
        var V = Error.stackTraceLimit;
        Error.stackTraceLimit = 10;
        var M = Error("react-stack-top-frame");
        Error.stackTraceLimit = V;
      } else M = ve;
      return C(
        n,
        d,
        g,
        !0,
        M,
        T ? me(i(n)) : Te
      );
    };
  })()), re;
}
var _e;
function Ve() {
  return _e || (_e = 1, process.env.NODE_ENV === "production" ? ae.exports = Le() : ae.exports = De()), ae.exports;
}
var r = Ve();
function ie() {
  const [e, t] = v(null), [s, i] = v(!1), [o, u] = v(""), c = Ae(!0);
  te(() => () => {
    c.current = !1;
  }, []);
  const m = Y(async () => {
    try {
      const b = await U.instance();
      if (!c.current) return;
      t(b), i(!1), u("");
    } catch (b) {
      if (!c.current) return;
      t(null), i(!0), u(L(b));
    }
  }, []);
  return { instance: e, instanceError: s, instanceErrorText: o, poll: m };
}
function ce({ data: e }) {
  if (!e) return null;
  const { namesrvForeignListenerSuspected: t, brokerForeignListenerSuspected: s, namesrvPort: i, brokerPort: o } = e;
  if (!t && !s) return null;
  const u = [
    t ? `NameServer :${i}（属主 PID ${e.namesrvPortOwnerPid}，我的子进程 PID ${e.namesrvChildPid}）` : null,
    s ? `Broker :${o}（属主 PID ${e.brokerPortOwnerPid}，我的子进程 PID ${e.brokerChildPid}）` : null
  ].filter(Boolean).join("；");
  return /* @__PURE__ */ r.jsx(
    y,
    {
      type: "error",
      showIcon: !0,
      style: { marginBottom: 16 },
      message: "有端口在应答，但应答者不是本次构建 spawn 的子进程 —— 下面所有表格都不算孵化结果",
      description: `外来嫌疑：${u}。` + (e.bindError ? ` 本 JVM 的子进程 bind 失败原文：${e.bindError}` : " 本 JVM 的子进程没有 bind 成功的记录。") + // 端口取实例自省回来的实际值：写死 9876/10911 会在换端口后把人指到别的进程上
      ` 处理方式只有两种：让出 ${i}/${o}，或给 zmq.namesrv-port / zmq.broker-port 换端口。 代理层不会替你去占端口，也不会去 kill 别人的进程。`
    }
  );
}
function X({ res: e }) {
  return !e || !(e.attributionUnverified || e.attributionVerified === !1) ? null : /* @__PURE__ */ r.jsx(
    y,
    {
      type: "warning",
      showIcon: !0,
      style: { marginBottom: 12 },
      message: `本响应来自 ${e.target || "?"}，但归属未通过（应答者不是本 JVM 的子进程）`,
      description: e.attributionNote
    }
  );
}
function de({ data: e }) {
  if (!e) return null;
  const t = [
    e.namesrvChildAlive && !e.namesrvPortOwnedByMyChild ? `NameServer pid=${e.namesrvChildPid}` : null,
    e.brokerChildAlive && !e.brokerPortOwnedByMyChild ? `Broker pid=${e.brokerChildPid}` : null
  ].filter(Boolean);
  return t.length ? /* @__PURE__ */ r.jsx(
    y,
    {
      type: "error",
      showIcon: !0,
      style: { marginBottom: 16 },
      message: `子进程 isAlive()=true 但端口不归它：${t.join("、")} —— 「进程在」不等于「服务在」`,
      description: 'z-mq 的 Main 会 Thread.currentThread().join()，所以 ZMqEmbeddedServerConfig 是 spawn 子进程而不是线程内嵌。bind 失败时 main 线程已死、Netty 非守护线程吊着 JVM 不退，于是 isAlive() 一路报"好"。这就是 __instance 把 *ChildAlive 和 *PortOwnedByMyChild 拆成两格的原因。'
    }
  ) : null;
}
function ke({ data: e }) {
  return !e || e.attributionAvailable !== !1 ? null : /* @__PURE__ */ r.jsx(
    y,
    {
      type: "warning",
      showIcon: !0,
      style: { marginBottom: 16 },
      message: "归属无法自证（lsof 探针不可用或已关闭）",
      description: e.attributionMethod + "。此时页面不猜：请人工执行 lsof -nP -iTCP:" + e.namesrvPort + " -sTCP:LISTEN 与本页 jvm/子进程 pid 对照。"
    }
  );
}
function Ue(e) {
  if (!e) return "-";
  try {
    return new Date(e).toLocaleString("zh-CN", { hour12: !1 });
  } catch {
    return String(e);
  }
}
function Fe(e) {
  if (e == null) return "-";
  const t = Math.max(0, Math.floor((Date.now() - e) / 1e3));
  return t < 60 ? `${t} 秒前` : t < 3600 ? `${Math.floor(t / 60)} 分前` : t < 86400 ? `${Math.floor(t / 3600)} 时前` : `${Math.floor(t / 86400)} 天前`;
}
function N(e, t = "是", s = "否", i = "error") {
  return e == null ? /* @__PURE__ */ r.jsx(h, { children: "未知" }) : e ? /* @__PURE__ */ r.jsx(h, { color: "success", children: t }) : /* @__PURE__ */ r.jsx(h, { color: i, children: s });
}
const Ye = [
  { title: "逻辑端点", dataIndex: "ep", key: "ep", width: 240, render: (e) => /* @__PURE__ */ r.jsx("code", { children: e }) },
  { title: "请求码", dataIndex: "code", key: "code", width: 100, render: (e) => /* @__PURE__ */ r.jsx(h, { children: e }) },
  { title: "常量名", dataIndex: "name", key: "name", render: (e) => /* @__PURE__ */ r.jsx("code", { children: e }) }
];
function Je() {
  var _;
  const { instance: e, instanceError: t, instanceErrorText: s, poll: i } = ie(), [o, u] = v(null), [c, m] = v(null), [b, S] = v(!1), C = Y(async (p) => {
    S(!0);
    try {
      const f = await U[p]();
      u({ which: p, res: f }), m(null);
    } catch (f) {
      u(null), m(f);
    } finally {
      S(!1), i();
    }
  }, [i]), O = e ? Object.entries(e.readOnlyRequestCodes || {}).map(([p, f]) => ({
    ep: p,
    code: f,
    name: (e.requestCodeNames || {})[String(f)] || "-",
    key: p
  })) : [];
  return /* @__PURE__ */ r.jsxs("div", { children: [
    /* @__PURE__ */ r.jsx(
      le,
      {
        title: "实例与端口",
        subtitle: "自省接口 GET /api/mq/__instance（由 MqProxyController 自己应答，不经过 z-mq）+ 只读请求码实发探针"
      }
    ),
    /* @__PURE__ */ r.jsx(I, { extra: /* @__PURE__ */ r.jsxs(z, { children: [
      /* @__PURE__ */ r.jsx(q, { icon: /* @__PURE__ */ r.jsx(ne, {}), loading: b, onClick: () => C("cluster"), children: "实发一发 码4 GET_BROKER_CLUSTER_INFO" }),
      /* @__PURE__ */ r.jsx(q, { icon: /* @__PURE__ */ r.jsx(ne, {}), onClick: i, children: "刷新自省" })
    ] }), children: t ? /* @__PURE__ */ r.jsx(
      y,
      {
        type: "error",
        showIcon: !0,
        message: `自省接口调用失败：${s}`,
        description: "/api/mq/__instance 不碰 NameServer 也不碰 Broker，由 z-opc 自己的 controller 直接应答；它都不通说明是 z-opc 侧的路由/认证问题（302 到登录页 = 没带会话 Cookie；404 = MainWebConfig.spaPaths 或 /api/mq 映射没接上）。"
      }
    ) : /* @__PURE__ */ r.jsxs(r.Fragment, { children: [
      /* @__PURE__ */ r.jsx(ke, { data: e }),
      /* @__PURE__ */ r.jsx(ce, { data: e }),
      /* @__PURE__ */ r.jsx(de, { data: e }),
      e && !e.embeddedConfigLoaded && /* @__PURE__ */ r.jsx(
        y,
        {
          type: "warning",
          showIcon: !0,
          style: { marginBottom: 16 },
          message: "ZMqEmbeddedServerConfig 未装载",
          description: "zmq.enabled 不为 true 时这个 @Configuration 整个不生效 ⇒ 不会有任何子进程被 spawn，页面所有数据必然拿不到。"
        }
      ),
      /* @__PURE__ */ r.jsx(
        Q,
        {
          bordered: !0,
          size: "small",
          column: 2,
          items: [
            {
              key: "jvm",
              label: "本 JVM (pid@host)",
              children: /* @__PURE__ */ r.jsx("code", { children: (e == null ? void 0 : e.jvm) ?? "-" })
            },
            {
              key: "surface",
              label: "z-mq 的管理面形状",
              children: /* @__PURE__ */ r.jsx(h, { color: "volcano", children: (e == null ? void 0 : e.surface) ?? "-" })
            },
            {
              key: "loaded",
              label: "内嵌配置已装载",
              children: e ? e.embeddedConfigLoaded ? /* @__PURE__ */ r.jsx(h, { color: "success", children: "是" }) : /* @__PURE__ */ r.jsx(h, { color: "error", children: "否（zmq.enabled≠true）" }) : "-"
            },
            {
              key: "addr",
              label: "NameServer 地址 / Broker 端口",
              children: /* @__PURE__ */ r.jsx("code", { children: e ? `${e.namesrvAddr} / ${e.brokerPort}` : "-" })
            },
            {
              key: "nsChild",
              label: "NameServer 子进程已 spawn",
              children: N(e == null ? void 0 : e.namesrvChildSpawned, "是", "否（未 spawn 或 boot jar 未找到）")
            },
            {
              key: "nsPid",
              label: "NameServer 子进程 pid / 存活",
              children: e ? /* @__PURE__ */ r.jsxs("span", { children: [
                /* @__PURE__ */ r.jsx("code", { children: e.namesrvChildPid ?? "n/a" }),
                " ",
                N(e.namesrvChildAlive, "alive", "dead")
              ] }) : "-"
            },
            {
              key: "nsOwner",
              label: "NameServer 端口属主 pid（lsof）",
              children: /* @__PURE__ */ r.jsx("code", { children: (e == null ? void 0 : e.namesrvPortOwnerPid) ?? "取不到" })
            },
            {
              key: "nsOwned",
              label: "NameServer 端口归我的子进程",
              children: N(e == null ? void 0 : e.namesrvPortOwnedByMyChild, "是（这才是孵化成功）", "否")
            },
            {
              key: "brChild",
              label: "Broker 子进程已 spawn",
              children: N(e == null ? void 0 : e.brokerChildSpawned, "是", "否")
            },
            {
              key: "brPid",
              label: "Broker 子进程 pid / 存活",
              children: e ? /* @__PURE__ */ r.jsxs("span", { children: [
                /* @__PURE__ */ r.jsx("code", { children: e.brokerChildPid ?? "n/a" }),
                " ",
                N(e.brokerChildAlive, "alive", "dead")
              ] }) : "-"
            },
            {
              key: "brOwner",
              label: "Broker 端口属主 pid（lsof）",
              children: /* @__PURE__ */ r.jsx("code", { children: (e == null ? void 0 : e.brokerPortOwnerPid) ?? "取不到" })
            },
            {
              key: "brOwned",
              label: "Broker 端口归我的子进程",
              children: N(e == null ? void 0 : e.brokerPortOwnedByMyChild, "是", "否")
            },
            {
              key: "reach",
              label: "两个端口此刻 TCP 可连",
              children: e ? /* @__PURE__ */ r.jsxs(z, { children: [
                N(e.namesrvPortReachable, `:${e.namesrvPort} 通`, `:${e.namesrvPort} 不通`, "warning"),
                N(e.brokerPortReachable, `:${e.brokerPort} 通`, `:${e.brokerPort} 不通`, "warning")
              ] }) : "-"
            },
            {
              key: "store",
              label: "store 目录（存在 / 是否为空）",
              children: e ? /* @__PURE__ */ r.jsxs("span", { children: [
                /* @__PURE__ */ r.jsx("code", { children: e.storePath }),
                " ",
                N(e.storePathExists, "目录在", "目录不存在", "warning"),
                " ",
                e.storePathExists && N(!e.storePathEmpty, "有内容", "空 = 这个实例没落过任何消息", "warning")
              ] }) : "-"
            }
          ]
        }
      ),
      (e == null ? void 0 : e.surfaceNote) && /* @__PURE__ */ r.jsx("div", { style: { marginTop: 12, color: "#64748b", fontSize: 12 }, children: e.surfaceNote })
    ] }) }),
    /* @__PURE__ */ r.jsx(
      I,
      {
        title: "子进程 bind 失败原文（spawn 器落的日志）",
        style: { marginTop: 16 },
        extra: /* @__PURE__ */ r.jsx(h, { color: "blue", children: "ZMqEmbeddedServerConfig 把子进程 stdout/stderr 追加到这里" }),
        children: e ? /* @__PURE__ */ r.jsx(
          Q,
          {
            bordered: !0,
            size: "small",
            column: 1,
            items: ["namesrvSpawnLog", "brokerSpawnLog"].map((p) => {
              const f = e[p] || {};
              return {
                key: p,
                label: p === "namesrvSpawnLog" ? "NameServer" : "Broker",
                children: /* @__PURE__ */ r.jsxs("div", { style: { fontSize: 12 }, children: [
                  /* @__PURE__ */ r.jsxs("div", { children: [
                    /* @__PURE__ */ r.jsx("code", { children: f.path }),
                    " ",
                    N(f.exists, "存在", "不存在", "warning"),
                    " ",
                    f.exists ? /* @__PURE__ */ r.jsxs("span", { style: { color: "#94a3b8" }, children: [
                      "（",
                      Fe(f.lastModified),
                      "写入）"
                    ] }) : null
                  ] }),
                  f.bindErrorLines ? /* @__PURE__ */ r.jsxs("div", { style: { color: "#dc2626" }, children: [
                    "命中 ",
                    f.bindErrorLines,
                    " 行 bind 失败，最后一条：",
                    /* @__PURE__ */ r.jsx("code", { children: f.lastBindError })
                  ] }) : /* @__PURE__ */ r.jsx("div", { style: { color: "#94a3b8" }, children: "日志里没有 bind 失败记录" })
                ] })
              };
            })
          }
        ) : /* @__PURE__ */ r.jsx(y, { type: "info", showIcon: !0, message: "自省接口没通，读不到日志字段" })
      }
    ),
    /* @__PURE__ */ r.jsx(I, { title: "实发一发只读请求码（证明协议适配器本身成立，而不只是在报自省）", style: { marginTop: 16 }, children: /* @__PURE__ */ r.jsxs(z, { direction: "vertical", style: { width: "100%" }, children: [
      /* @__PURE__ */ r.jsxs(z, { wrap: !0, children: [
        /* @__PURE__ */ r.jsx(q, { size: "small", loading: b, onClick: () => C("cluster"), children: "码 4 · 集群注册表" }),
        /* @__PURE__ */ r.jsx(q, { size: "small", loading: b, onClick: () => C("topics"), children: "码 8 · NameServer topic 列表" }),
        /* @__PURE__ */ r.jsx(q, { size: "small", loading: b, onClick: () => C("brokerTopics"), children: "Broker 码 8 · 本 broker topic" }),
        /* @__PURE__ */ r.jsx(q, { size: "small", loading: b, onClick: () => C("brokerDataVersion"), children: "Broker 码 324 · DataVersion" })
      ] }),
      c && /* @__PURE__ */ r.jsx(
        y,
        {
          type: "error",
          showIcon: !0,
          message: `探针失败：${L(c)}`
        }
      ),
      o && /* @__PURE__ */ r.jsxs(r.Fragment, { children: [
        /* @__PURE__ */ r.jsx(X, { res: o.res }),
        /* @__PURE__ */ r.jsx(
          Q,
          {
            bordered: !0,
            size: "small",
            column: 3,
            items: [
              { key: "t", label: "打的哪一档", children: /* @__PURE__ */ r.jsx(h, { children: o.res.tier }) },
              { key: "target", label: "目标地址", children: /* @__PURE__ */ r.jsx("code", { children: o.res.target }) },
              { key: "req", label: "请求码", children: /* @__PURE__ */ r.jsxs("code", { children: [
                o.res.requestCode,
                " ",
                o.res.requestName
              ] }) },
              {
                key: "responseCode",
                label: "z-mq 业务码",
                children: /* @__PURE__ */ r.jsxs(h, { color: o.res.responseCode === 0 ? "success" : "error", children: [
                  o.res.responseCode,
                  " · ",
                  xe[o.res.responseCode] || "未知码"
                ] })
              },
              { key: "remark", label: "remark", children: o.res.remark || /* @__PURE__ */ r.jsx("span", { style: { color: "#94a3b8" }, children: "—" }) },
              { key: "elapsed", label: "耗时", children: `${o.res.elapsedMs ?? "-"} ms` },
              { key: "attr", label: "归属已验证", children: N(o.res.attributionVerified) },
              { key: "size", label: "响应体字节数", children: JSON.stringify(o.res.data ?? null).length },
              {
                key: "ts",
                label: "本页数据读取时刻",
                children: Ue(Date.now())
              }
            ]
          }
        ),
        /* @__PURE__ */ r.jsx("pre", { style: {
          background: "#0f172a",
          color: "#e2e8f0",
          padding: 12,
          borderRadius: 6,
          maxHeight: 260,
          overflow: "auto",
          fontSize: 12,
          margin: 0
        }, children: JSON.stringify(o.res.data ?? null, null, 2) })
      ] }),
      !o && !c && /* @__PURE__ */ r.jsx(
        y,
        {
          type: "info",
          showIcon: !0,
          message: "还没发探针",
          description: "自省接口证明的是「z-opc 这一层活着、且知道自己没起」；发一发只读码证明的是「协议适配这条形状走得通」。两个都要。"
        }
      )
    ] }) }),
    /* @__PURE__ */ r.jsxs(I, { title: "本代理可达的只读请求码（穷举，无通用转发口）", style: { marginTop: 16 }, children: [
      /* @__PURE__ */ r.jsx(
        he,
        {
          size: "small",
          rowKey: "ep",
          dataSource: O,
          columns: Ye,
          pagination: !1
        }
      ),
      ((_ = e == null ? void 0 : e.unavailableOnBroker120) == null ? void 0 : _.length) > 0 && /* @__PURE__ */ r.jsx(
        y,
        {
          type: "warning",
          showIcon: !0,
          style: { marginTop: 12 },
          message: "以下能力在 z-mq 1.2.0 上根本没有请求码，页面不做也不假做",
          description: /* @__PURE__ */ r.jsxs("div", { children: [
            /* @__PURE__ */ r.jsx("div", { style: { fontWeight: 600, marginTop: 4 }, children: "Broker 侧未注册：" }),
            /* @__PURE__ */ r.jsx("ul", { style: { margin: "4px 0 0 18px" }, children: e.unavailableOnBroker120.map((p) => /* @__PURE__ */ r.jsx("li", { children: p }, p)) }),
            /* @__PURE__ */ r.jsx("div", { style: { fontWeight: 600, marginTop: 4 }, children: "NameServer 侧未实现：" }),
            /* @__PURE__ */ r.jsx("ul", { style: { margin: "4px 0 0 18px" }, children: (e.unavailableOnNamesrv120 || []).map((p) => /* @__PURE__ */ r.jsx("li", { children: p }, p)) })
          ] })
        }
      )
    ] })
  ] });
}
const Ge = [
  {
    title: "Broker 名",
    dataIndex: "brokerName",
    key: "brokerName",
    width: 180,
    render: (e) => /* @__PURE__ */ r.jsx("code", { children: e })
  },
  { title: "Cluster", dataIndex: "cluster", key: "cluster", width: 160, render: (e) => e || /* @__PURE__ */ r.jsx(h, { children: "未上报" }) },
  {
    title: "Master 地址 (brokerId=0)",
    dataIndex: "masterAddr",
    key: "masterAddr",
    width: 220,
    render: (e) => /* @__PURE__ */ r.jsx("code", { children: e || "-" })
  },
  {
    title: "全部副本 (id → addr)",
    dataIndex: "replicas",
    key: "replicas",
    render: (e) => e.length ? /* @__PURE__ */ r.jsx(z, { direction: "vertical", size: 0, children: e.map((t) => /* @__PURE__ */ r.jsxs("span", { children: [
      /* @__PURE__ */ r.jsxs(h, { color: t.id === "0" ? "blue" : "default", children: [
        "id=",
        t.id
      ] }),
      /* @__PURE__ */ r.jsx("code", { children: t.addr })
    ] }, t.id)) }) : /* @__PURE__ */ r.jsx("span", { style: { color: "#94a3b8" }, children: "—" })
  },
  {
    title: "端口是否等于本 JVM 配置的 broker 端口",
    key: "portMatch",
    width: 240,
    render: (e, t) => t.port ? t.portMatched === null ? /* @__PURE__ */ r.jsx(h, { color: "warning", children: "配置端口未知" }) : t.portMatched ? /* @__PURE__ */ r.jsxs(h, { color: "success", children: [
      "一致（",
      t.port,
      "）"
    ] }) : /* @__PURE__ */ r.jsxs(h, { color: "error", children: [
      "不一致：注册的是 ",
      t.port,
      "，本 JVM 配的是 ",
      t.expected
    ] }) : /* @__PURE__ */ r.jsx(h, { children: "无地址" })
  }
];
function We() {
  const { instance: e, poll: t } = ie(), [s, i] = v(null), [o, u] = v(null), [c, m] = v(!1), b = Y(async () => {
    m(!0);
    try {
      const a = await U.cluster();
      i(a), u(null);
    } catch (a) {
      i(null), u(a);
    } finally {
      m(!1);
    }
  }, []);
  te(() => {
    b(), t();
  }, [b, t]);
  const S = (s == null ? void 0 : s.data) || {}, C = S.brokerAddrTable && typeof S.brokerAddrTable == "object" ? S.brokerAddrTable : {}, O = S.clusterAddrTable && typeof S.clusterAddrTable == "object" ? S.clusterAddrTable : {}, _ = (e == null ? void 0 : e.brokerPort) ?? null, p = Object.keys(C).map((a) => {
    var F;
    const l = C[a] || {}, x = l.brokerAddrs && typeof l.brokerAddrs == "object" ? l.brokerAddrs : {}, k = Object.keys(x).map((J) => ({ id: J, addr: String(x[J]) })), w = x[0] ? String(x[0]) : ((F = k[0]) == null ? void 0 : F.addr) || null, P = w ? Number(w.split(":").pop()) : null;
    return {
      key: a,
      brokerName: l.brokerName || a,
      cluster: l.cluster,
      masterAddr: w,
      replicas: k,
      port: P,
      expected: _,
      portMatched: _ == null ? null : P === _
    };
  }), f = Object.keys(O).map((a) => ({
    key: a,
    cluster: a,
    members: (Array.isArray(O[a]) ? O[a] : []).map(String)
  })), D = s && !o ? K(s) : null;
  return /* @__PURE__ */ r.jsxs("div", { children: [
    /* @__PURE__ */ r.jsx(
      le,
      {
        title: "集群与 Broker",
        subtitle: "NameServer 码 4 GET_BROKER_CLUSTER_INFO → brokerAddrTable / clusterAddrTable"
      }
    ),
    /* @__PURE__ */ r.jsxs(I, { extra: /* @__PURE__ */ r.jsx(z, { children: /* @__PURE__ */ r.jsx(q, { icon: /* @__PURE__ */ r.jsx(ne, {}), loading: c, onClick: () => {
      b(), t();
    }, children: "刷新" }) }), children: [
      /* @__PURE__ */ r.jsx(ce, { data: e }),
      /* @__PURE__ */ r.jsx(de, { data: e }),
      o && /* @__PURE__ */ r.jsx(
        y,
        {
          type: "error",
          showIcon: !0,
          message: `读 NameServer 失败：${L(o)}`,
          description: `这一档是 nameserver 的只读码，不需要 broker 注册就能应答；它不通说明 :${(e == null ? void 0 : e.namesrvPort) ?? "?"} 上根本没有能答话的进程（或者答话的那个跟本 JVM 不是一家的，见顶部红条）。端口取自 /api/mq/__instance，写死端口会在换端口后把人指到别的进程上。`
        }
      ),
      D && /* @__PURE__ */ r.jsx(y, { type: "warning", showIcon: !0, style: { marginBottom: 12 }, message: D }),
      s && !o && /* @__PURE__ */ r.jsxs(r.Fragment, { children: [
        /* @__PURE__ */ r.jsx(X, { res: s }),
        /* @__PURE__ */ r.jsxs(be, { gutter: 16, style: { marginBottom: 12 }, children: [
          /* @__PURE__ */ r.jsx(A, { span: 6, children: /* @__PURE__ */ r.jsx(B, { title: "已注册 Broker 数", value: p.length }) }),
          /* @__PURE__ */ r.jsx(A, { span: 6, children: /* @__PURE__ */ r.jsx(B, { title: "Cluster 数", value: f.length }) }),
          /* @__PURE__ */ r.jsx(A, { span: 6, children: /* @__PURE__ */ r.jsx(B, { title: "请求码", value: `${s.requestCode || "-"} ${s.requestName || ""}` }) }),
          /* @__PURE__ */ r.jsx(A, { span: 6, children: /* @__PURE__ */ r.jsx(B, { title: "往返耗时", value: s.elapsedMs ?? "-", suffix: "ms" }) })
        ] }),
        p.length === 0 ? /* @__PURE__ */ r.jsx(
          oe,
          {
            title: "brokerAddrTable 是空的",
            description: "没有任何 broker 向这台 NameServer 发过 REGISTER_BROKER（码 1）。两种成因要分开：① broker 子进程没起 / 没 bind 上（看顶部红条与「实例与端口」页的 bind 原文）；② broker 起了但配的 zmq.namesrv-addr 指向的是另一台 NameServer。本页不给「注册 broker」按钮 —— 那是写面，另立卡。"
          }
        ) : /* @__PURE__ */ r.jsx(
          he,
          {
            size: "small",
            rowKey: "key",
            loading: c,
            dataSource: p,
            columns: Ge,
            pagination: !1
          }
        )
      ] }),
      !s && !o && /* @__PURE__ */ r.jsx(oe, { title: "还没读到数据", description: "刷新以发送一次只读请求码 4。" })
    ] }),
    f.length > 0 && /* @__PURE__ */ r.jsx(I, { title: "Cluster → Broker 归属", style: { marginTop: 16 }, children: /* @__PURE__ */ r.jsx(
      Q,
      {
        bordered: !0,
        size: "small",
        column: 1,
        items: f.map((a) => ({
          key: a.key,
          label: a.cluster,
          children: /* @__PURE__ */ r.jsx(z, { wrap: !0, children: a.members.map((l) => /* @__PURE__ */ r.jsx(h, { children: l }, l)) })
        }))
      }
    ) }),
    /* @__PURE__ */ r.jsx(I, { title: "上游语义提示", style: { marginTop: 16 }, children: /* @__PURE__ */ r.jsxs("div", { style: { color: "#64748b", fontSize: 12, lineHeight: 1.9 }, children: [
      /* @__PURE__ */ r.jsxs("div", { children: [
        "· ",
        /* @__PURE__ */ r.jsx("code", { children: "brokerAddrTable" }),
        " 记的是「谁发过 REGISTER_BROKER 心跳」，不是「谁在跑」。进程活着但心跳线程没起 ⇒ 这里为空。"
      ] }),
      /* @__PURE__ */ r.jsxs("div", { children: [
        "· 上表最后一列是本页自带的第二重归属校验：注册的 broker 端口应当等于",
        /* @__PURE__ */ r.jsx("code", { children: "zmq.broker-port" }),
        "。不等就说明",
        /* @__PURE__ */ r.jsx("b", { children: "这台 NameServer 记的是另一套集群" }),
        "， 后面 broker 页拿到的地址会被代理层的环回白名单挡掉（403），那是正确行为不是 bug。"
      ] }),
      /* @__PURE__ */ r.jsxs("div", { children: [
        "· 代理层只会拨环回地址（",
        /* @__PURE__ */ r.jsx("code", { children: "127.0.0.1" }),
        " / ",
        /* @__PURE__ */ r.jsx("code", { children: "localhost" }),
        " / ",
        /* @__PURE__ */ r.jsx("code", { children: "::1" }),
        "）， 非环回的注册地址一律拒绝，这既是安全边界也是防误连别机的护栏。"
      ] })
    ] }) })
  ] });
}
const Qe = [
  {
    title: "Topic",
    dataIndex: "topic",
    key: "topic",
    render: (e) => /* @__PURE__ */ r.jsx("code", { children: e })
  },
  {
    title: "NameServer (码 8)",
    dataIndex: "onNamesrv",
    key: "onNamesrv",
    width: 160,
    render: (e) => e ? /* @__PURE__ */ r.jsx(h, { color: "success", children: "已注册" }) : /* @__PURE__ */ r.jsx(h, { color: "error", children: "没有" })
  },
  {
    title: "Broker (码 8)",
    dataIndex: "onBroker",
    key: "onBroker",
    width: 140,
    render: (e, t) => e ? /* @__PURE__ */ r.jsx(h, { color: "success", children: "有配置" }) : t.brokerUnavailable ? /* @__PURE__ */ r.jsx(h, { children: "未取到" }) : /* @__PURE__ */ r.jsx(h, { color: "error", children: "没有" })
  },
  {
    title: "一致性",
    key: "consistency",
    width: 200,
    render: (e, t) => t.onNamesrv && t.onBroker ? /* @__PURE__ */ r.jsx(h, { color: "success", children: "两侧一致" }) : /* @__PURE__ */ r.jsx(h, { color: "warning", children: t.onNamesrv ? "只在 NameServer（疑似残留注册）" : "只在 Broker（未向 NameServer 注册）" })
  }
];
function Ke() {
  var se, G;
  const { instance: e, poll: t } = ie(), [s, i] = v(null), [o, u] = v(null), [c, m] = v(null), [b, S] = v(null), [C, O] = v(!1), [_, p] = v({}), f = Y(async () => {
    O(!0), m(null), S(null), await Promise.all([
      U.topics().then(i).catch((j) => {
        i(null), m(j);
      }),
      U.brokerTopics().then(u).catch((j) => {
        u(null), S(j);
      })
    ]), O(!1), t();
  }, [t]);
  te(() => {
    f();
  }, [f]);
  const D = Y(async (j) => {
    if (!_[j]) {
      p((R) => ({ ...R, [j]: { loading: !0 } }));
      try {
        const R = await U.route(j);
        p((Z) => ({ ...Z, [j]: R }));
      } catch (R) {
        p((Z) => ({ ...Z, [j]: { loadError: L(R) } }));
      }
    }
  }, [_]), a = s && s.responseCode === 0 && Array.isArray((se = s.data) == null ? void 0 : se.topics) ? s.data.topics.map(String) : null, l = o && o.responseCode === 0 && Array.isArray((G = o.data) == null ? void 0 : G.topics) ? o.data.topics.map(String) : null, x = Re(() => {
    const j = /* @__PURE__ */ new Set([...a || [], ...l || []]);
    return Array.from(j).sort().map((R) => ({
      key: R,
      topic: R,
      onNamesrv: a ? a.includes(R) : !1,
      onBroker: l ? l.includes(R) : !1,
      brokerUnavailable: !l
    }));
  }, [a, l]), k = x.filter((j) => j.onNamesrv && !j.onBroker).length, w = x.filter((j) => !j.onNamesrv && j.onBroker).length, P = s && !c ? K(s) : null, F = o && !b ? K(o) : null, J = !a && !l;
  return /* @__PURE__ */ r.jsxs("div", { children: [
    /* @__PURE__ */ r.jsx(
      le,
      {
        title: "Topic 列表",
        subtitle: "NameServer 码 8 GET_ALL_TOPIC_LIST ∥ Broker 码 8 GET_ALL_TOPIC_LIST → 差集 + 按需码 3 路由详情"
      }
    ),
    /* @__PURE__ */ r.jsxs(I, { extra: /* @__PURE__ */ r.jsx(z, { children: /* @__PURE__ */ r.jsx(q, { icon: /* @__PURE__ */ r.jsx(ne, {}), loading: C, onClick: f, children: "刷新" }) }), children: [
      /* @__PURE__ */ r.jsx(ce, { data: e }),
      /* @__PURE__ */ r.jsx(de, { data: e }),
      /* @__PURE__ */ r.jsx(ke, { data: e }),
      c && /* @__PURE__ */ r.jsx(
        y,
        {
          type: "error",
          showIcon: !0,
          style: { marginBottom: 12 },
          message: `读 NameServer 的 topic 列表失败：${L(c)}`
        }
      ),
      b && /* @__PURE__ */ r.jsx(
        y,
        {
          type: "error",
          showIcon: !0,
          style: { marginBottom: 12 },
          message: `读 Broker 的 topicConfigTable 失败：${L(b)}`,
          description: "Broker 侧的地址全靠 NameServer 的码 4 发现，且只允许环回。这一档失败通常说明集群里根本没有注册过 broker（见「集群与 Broker」页），或者代理层按环回白名单拒了外部地址 —— 后者是正确行为。"
        }
      ),
      P && /* @__PURE__ */ r.jsx(y, { type: "warning", showIcon: !0, style: { marginBottom: 12 }, message: `NameServer：${P}` }),
      F && /* @__PURE__ */ r.jsx(y, { type: "warning", showIcon: !0, style: { marginBottom: 12 }, message: `Broker：${F}` }),
      s && !c && /* @__PURE__ */ r.jsx(X, { res: s }),
      o && !b && /* @__PURE__ */ r.jsx(X, { res: o }),
      /* @__PURE__ */ r.jsxs(be, { gutter: 16, style: { marginBottom: 12 }, children: [
        /* @__PURE__ */ r.jsx(A, { span: 6, children: /* @__PURE__ */ r.jsx(B, { title: "NameServer 侧 topic 数", value: a ? a.length : "—" }) }),
        /* @__PURE__ */ r.jsx(A, { span: 6, children: /* @__PURE__ */ r.jsx(B, { title: "Broker 侧 topic 数", value: l ? l.length : "—" }) }),
        /* @__PURE__ */ r.jsx(A, { span: 6, children: /* @__PURE__ */ r.jsx(
          B,
          {
            title: "只在 NameServer",
            value: l ? k : "—",
            valueStyle: k && l ? { color: "#b45309" } : void 0
          }
        ) }),
        /* @__PURE__ */ r.jsx(A, { span: 6, children: /* @__PURE__ */ r.jsx(
          B,
          {
            title: "只在 Broker",
            value: l ? w : "—",
            valueStyle: w && l ? { color: "#b45309" } : void 0
          }
        ) })
      ] }),
      J ? /* @__PURE__ */ r.jsx(
        oe,
        {
          title: "两侧都没取到 topic 列表",
          description: "这不是「集群里没有 topic」的证据：上面任一档报错、或上游回了非 0 业务码（HTTP 仍是 200），都到这里显示为取不到。先读上面的红/黄条定性，再决定要不要怀疑数据。"
        }
      ) : (
        /* 列表页走基座 ListTable（lead 008 §2 规则 2/3/4）：分页 / 过滤 / EmptyState 三件套。
           外层 Card 已经给了边框与内边距（里面还有横幅/告警/统计条），
           所以基座 Card 设成无边框，避免两层卡片套在一起。
           展开行（码 3 路由详情）仍由 antd Table 的 expandable 处理。 */
        /* @__PURE__ */ r.jsx(
          fe,
          {
            rows: x,
            columns: Qe,
            rowKey: "key",
            loading: C,
            searchKeys: "topic",
            searchPlaceholder: "按 topic 名过滤",
            cardProps: { variant: "borderless", styles: { body: { padding: 0 } } },
            expandable: {
              onExpand: (j, R) => {
                j && D(R.topic);
              },
              expandedRowRender: (j) => /* @__PURE__ */ r.jsx(Xe, { state: _[j.topic] })
            }
          }
        )
      )
    ] }),
    /* @__PURE__ */ r.jsx(I, { title: "这一页的两个语义坑", style: { marginTop: 16 }, children: /* @__PURE__ */ r.jsxs("div", { style: { color: "#64748b", fontSize: 12, lineHeight: 1.9 }, children: [
      /* @__PURE__ */ r.jsxs("div", { children: [
        "· 码 8 只回",
        /* @__PURE__ */ r.jsx("b", { children: "名字" }),
        "，队列数 / perm / 集群归属要展开走码 3；展开是懒加载，所以打开本页不会打出 N 个请求。"
      ] }),
      /* @__PURE__ */ r.jsxs("div", { children: [
        "· 码 3 查不存在的 topic 时上游回的是 ",
        /* @__PURE__ */ r.jsx("code", { children: "responseCode=1 SYSTEM_ERROR" }),
        " + remark 「topic X not exist」—— ",
        /* @__PURE__ */ r.jsx("b", { children: "同一个码 1 既表示系统错误也表示 topic 不存在" }),
        '， 只有 remark 能分开，所以下面把 remark 原文贴出来而不是翻译成"出错了"。'
      ] })
    ] }) })
  ] });
}
function Xe({ state: e }) {
  var u, c;
  if (!e) return /* @__PURE__ */ r.jsx("span", { style: { color: "#94a3b8", fontSize: 12 }, children: "展开时向 NameServer 发送码 3 GET_ROUTEINFO_BY_TOPIC。" });
  if (e.loading) return /* @__PURE__ */ r.jsx("span", { style: { color: "#94a3b8", fontSize: 12 }, children: "查询中…" });
  if (e.loadError) return /* @__PURE__ */ r.jsx(y, { type: "error", showIcon: !0, message: `路由查询失败：${e.loadError}` });
  const t = ze(e), s = K(e), i = Array.isArray((u = e.data) == null ? void 0 : u.queueDatas) ? e.data.queueDatas : [], o = Array.isArray((c = e.data) == null ? void 0 : c.brokerDatas) ? e.data.brokerDatas : [];
  return /* @__PURE__ */ r.jsxs("div", { children: [
    t && /* @__PURE__ */ r.jsx(
      y,
      {
        type: "warning",
        showIcon: !0,
        style: { marginBottom: 8 },
        message: `NameServer 报这个 topic 没有路由（原文：${t}）`,
        description: "上游用的是 SYSTEM_ERROR(1) + remark，不是「查到但是空」，所以这一格不能渲染成空表。"
      }
    ),
    !t && s && /* @__PURE__ */ r.jsx(y, { type: "error", showIcon: !0, style: { marginBottom: 8 }, message: s }),
    e.data && /* @__PURE__ */ r.jsx(X, { res: e }),
    e.data && /* @__PURE__ */ r.jsxs(r.Fragment, { children: [
      /* @__PURE__ */ r.jsx(
        Q,
        {
          bordered: !0,
          size: "small",
          column: 3,
          style: { marginBottom: 8 },
          items: [
            { key: "target", label: "目标", children: /* @__PURE__ */ r.jsx("code", { children: e.target || "-" }) },
            { key: "order", label: "order", children: String(e.data.order ?? "-") },
            { key: "elapsed", label: "往返", children: `${e.elapsedMs ?? "-"} ms` }
          ]
        }
      ),
      /* @__PURE__ */ r.jsx(
        he,
        {
          size: "small",
          rowKey: (m) => `${m.brokerName}-${m.queueId ?? ""}`,
          dataSource: i,
          pagination: !1,
          columns: [
            { title: "Broker", dataIndex: "brokerName", key: "brokerName", render: (m) => /* @__PURE__ */ r.jsx("code", { children: m }) },
            { title: "readQueueNums", dataIndex: "readQueueNums", key: "readQueueNums" },
            { title: "writeQueueNums", dataIndex: "writeQueueNums", key: "writeQueueNums" },
            { title: "perm", dataIndex: "perm", key: "perm" },
            { title: "topicSysFlag", dataIndex: "topicSysFlag", key: "topicSysFlag" }
          ]
        }
      ),
      o.length > 0 && /* @__PURE__ */ r.jsxs("div", { style: { marginTop: 8, color: "#64748b", fontSize: 12 }, children: [
        "brokerDatas：",
        o.map((m) => `${m.brokerName}→${JSON.stringify(m.brokerAddrs || {})}`).join("，")
      ] })
    ] })
  ] });
}
const Oe = [
  { key: "topicConfig", path: "broker/topic-config", label: "Topic 配置（码 320）", api: "brokerTopicConfig" },
  { key: "consumerOffset", path: "broker/consumer-offset", label: "消费位点（码 321）", api: "brokerConsumerOffset" },
  { key: "delayOffset", path: "broker/delay-offset", label: "延迟位点（码 322）", api: "brokerDelayOffset" },
  { key: "subscription", path: "broker/subscription", label: "订阅组（码 323）", api: "brokerSubscription" },
  { key: "dataVersion", path: "broker/data-version", label: "DataVersion（码 324）", api: "brokerDataVersion" }
], Ze = [
  { title: "Topic", dataIndex: "topic", key: "topic", render: (e) => /* @__PURE__ */ r.jsx("code", { children: e }) },
  { title: "readQueueNums", dataIndex: "readQueueNums", key: "readQueueNums", width: 130 },
  { title: "writeQueueNums", dataIndex: "writeQueueNums", key: "writeQueueNums", width: 140 },
  { title: "perm", dataIndex: "perm", key: "perm", width: 90 },
  {
    title: "order",
    dataIndex: "order",
    key: "order",
    width: 90,
    render: (e) => e ? /* @__PURE__ */ r.jsx(h, { color: "blue", children: "true" }) : /* @__PURE__ */ r.jsx(h, { children: "false" })
  },
  { title: "unit", dataIndex: "unit", key: "unit", width: 110, render: (e) => e ?? "—" }
];
function He() {
  const { instance: e, poll: t } = ie(), [s, i] = v([]), [o, u] = v(null), [c, m] = v(null), [b, S] = v({}), [C, O] = v(!1), _ = Y(async () => {
    var a;
    try {
      const l = await U.cluster(), x = ((a = l == null ? void 0 : l.data) == null ? void 0 : a.brokerAddrTable) || {}, k = Object.keys(x);
      return i(k), u((w) => w && k.includes(w) ? w : k[0] || null), m(l && l.responseCode !== 0 ? new Error(K(l) || "码 4 返回非 0") : null), k;
    } catch (l) {
      return i([]), u(null), m(l), [];
    }
  }, []), p = Y(async (a) => {
    O(!0), await Promise.all(Oe.map(async (l) => {
      S((x) => ({ ...x, [l.key]: { loading: !0 } }));
      try {
        const x = await U[l.api](a || void 0);
        S((k) => ({ ...k, [l.key]: x }));
      } catch (x) {
        S((k) => ({ ...k, [l.key]: { loadError: L(x), raw: x } }));
      }
    })), O(!1), t();
  }, [t]);
  te(() => {
    _();
  }, [_]), te(() => {
    p(o || void 0);
  }, [o, p]);
  const f = (a) => {
    var x;
    const l = (x = a == null ? void 0 : a.data) == null ? void 0 : x.offsetTable;
    return !l || typeof l != "object" ? null : Object.keys(l).flatMap((k) => {
      const w = l[k];
      return !w || typeof w != "object" ? [] : Object.keys(w).map((P) => ({
        key: `${k}-${P}`,
        topic: k,
        queueId: P,
        maxOffset: w[P]
      }));
    });
  }, D = Re(() => {
    var l, x;
    const a = (x = (l = b.topicConfig) == null ? void 0 : l.data) == null ? void 0 : x.topicConfigTable;
    return !a || typeof a != "object" ? null : Object.keys(a).map((k) => ({ key: k, topic: k, ...a[k] || {} }));
  }, [b.topicConfig]);
  return /* @__PURE__ */ r.jsxs("div", { children: [
    /* @__PURE__ */ r.jsx(
      le,
      {
        title: "Broker 元数据",
        subtitle: "BrokerOutAPI 码 320/321/322/323/324 · 地址由 NameServer 码 4 发现且只允许环回"
      }
    ),
    /* @__PURE__ */ r.jsxs(I, { extra: /* @__PURE__ */ r.jsxs(z, { children: [
      /* @__PURE__ */ r.jsx("span", { style: { color: "#64748b", fontSize: 12 }, children: "Broker" }),
      /* @__PURE__ */ r.jsx(
        Me,
        {
          size: "small",
          style: { width: 240 },
          value: o,
          onChange: u,
          placeholder: s.length ? void 0 : "码 4 没有发现任何 broker",
          options: s.map((a) => ({ value: a, label: a }))
        }
      ),
      /* @__PURE__ */ r.jsx(q, { icon: /* @__PURE__ */ r.jsx(ne, {}), loading: C, onClick: () => p(o || void 0), children: "刷新五档" })
    ] }), children: [
      /* @__PURE__ */ r.jsx(ce, { data: e }),
      /* @__PURE__ */ r.jsx(de, { data: e }),
      /* @__PURE__ */ r.jsx(ke, { data: e }),
      c && /* @__PURE__ */ r.jsx(
        y,
        {
          type: "error",
          showIcon: !0,
          style: { marginBottom: 12 },
          message: `无法从 NameServer 发现 broker：${L(c)}`,
          description: "下面五档的地址全靠码 4，发现不到就只能整页 503 —— 那是「集群里没有注册过的 broker」，不是这五档本身坏了。"
        }
      ),
      /* @__PURE__ */ r.jsxs(be, { gutter: 16, children: [
        /* @__PURE__ */ r.jsx(A, { span: 8, children: /* @__PURE__ */ r.jsx(B, { title: "码 4 发现的 broker 数", value: s.length }) }),
        /* @__PURE__ */ r.jsx(A, { span: 8, children: /* @__PURE__ */ r.jsx(B, { title: "当前取数对象", value: o || "—" }) }),
        /* @__PURE__ */ r.jsx(A, { span: 8, children: /* @__PURE__ */ r.jsx(
          B,
          {
            title: "store 路径存在",
            value: (e == null ? void 0 : e.storePathExists) == null ? "未知" : e.storePathExists ? "是" : "否"
          }
        ) })
      ] })
    ] }),
    Oe.map((a) => /* @__PURE__ */ r.jsx(
      er,
      {
        section: a,
        state: b[a.key],
        rows: a.key === "topicConfig" ? D : a.key === "consumerOffset" || a.key === "delayOffset" ? f(b[a.key]) : null
      },
      a.key
    ))
  ] });
}
function er({ section: e, state: t, rows: s }) {
  const i = t == null ? void 0 : t.loadError, o = t && !i && t.responseCode !== void 0 ? K(t) : null, u = qe[e.path], c = t == null ? void 0 : t.data;
  return /* @__PURE__ */ r.jsxs(
    I,
    {
      title: e.label,
      style: { marginTop: 16 },
      size: "small",
      extra: t != null && t.target ? /* @__PURE__ */ r.jsx(h, { children: String(t.target) }) : null,
      children: [
        !t || t.loading ? /* @__PURE__ */ r.jsx("span", { style: { color: "#94a3b8", fontSize: 12 }, children: "取数中…" }) : null,
        i && /* @__PURE__ */ r.jsx(y, { type: "error", showIcon: !0, message: `调用失败：${i}` }),
        o && /* @__PURE__ */ r.jsx(y, { type: "warning", showIcon: !0, style: { marginBottom: 12 }, message: o }),
        t && !i && /* @__PURE__ */ r.jsx(X, { res: t }),
        u && /* @__PURE__ */ r.jsx(
          y,
          {
            type: "info",
            showIcon: !0,
            style: { marginBottom: 12 },
            message: "上游语义（不是本页的推断）",
            description: u
          }
        ),
        t && !i && c && e.key === "topicConfig" && (s && s.length ? /* @__PURE__ */ r.jsx(
          fe,
          {
            rows: s,
            columns: Ze,
            rowKey: "key",
            searchKeys: "topic",
            searchPlaceholder: "按 Topic 名过滤",
            cardProps: { variant: "borderless", styles: { body: { padding: 0 } } }
          }
        ) : /* @__PURE__ */ r.jsx(oe, { title: "topicConfigTable 是空的", description: "本 broker 没有注册任何 topic 配置。" })),
        t && !i && c && (e.key === "consumerOffset" || e.key === "delayOffset") && (s && s.length ? /* @__PURE__ */ r.jsx(
          fe,
          {
            rows: s,
            columns: [
              { title: "Topic", dataIndex: "topic", key: "topic", render: (m) => /* @__PURE__ */ r.jsx("code", { children: m }) },
              { title: "queueId", dataIndex: "queueId", key: "queueId", width: 110 },
              { title: "maxOffset", dataIndex: "maxOffset", key: "maxOffset", width: 140 }
            ],
            rowKey: "key",
            searchKeys: "topic,queueId",
            searchPlaceholder: "按 Topic / queueId 过滤",
            cardProps: { variant: "borderless", styles: { body: { padding: 0 } } }
          }
        ) : /* @__PURE__ */ r.jsx(
          oe,
          {
            title: "offsetTable 是空的",
            description: "注意上面那条「上游语义」：位点表为空可能是真没有消息，也可能只是这条链路上还没产生过写入。"
          }
        )),
        t && !i && c && e.key === "subscription" && /* @__PURE__ */ r.jsxs("div", { style: { color: "#64748b", fontSize: 12 }, children: [
          "body 原文（注意它连 ",
          /* @__PURE__ */ r.jsx("code", { children: "subscription" }),
          " 字段都没有，是一个空的",
          /* @__PURE__ */ r.jsx("code", { children: "ConsumerOffsetSerializeWrapper" }),
          "）：",
          /* @__PURE__ */ r.jsx("pre", { style: { whiteSpace: "pre-wrap", marginTop: 8, fontSize: 12 }, children: JSON.stringify(c, null, 2) })
        ] }),
        t && !i && c && e.key === "dataVersion" && /* @__PURE__ */ r.jsx(
          Q,
          {
            bordered: !0,
            size: "small",
            column: 1,
            items: Object.keys(c).map((m) => ({
              key: m,
              label: m,
              children: Se(c[m])
            }))
          }
        ),
        t && !i && (c == null ? void 0 : c.dataVersion) && e.key !== "dataVersion" && /* @__PURE__ */ r.jsxs("div", { style: { marginTop: 10, color: "#64748b", fontSize: 12 }, children: [
          "dataVersion：",
          Se(c.dataVersion)
        ] })
      ]
    }
  );
}
function ur() {
  return /* @__PURE__ */ r.jsxs(Be, { children: [
    /* @__PURE__ */ r.jsx(H, { index: !0, element: /* @__PURE__ */ r.jsx($e, { to: "instance", replace: !0 }) }),
    /* @__PURE__ */ r.jsx(H, { path: "instance", element: /* @__PURE__ */ r.jsx(Je, {}) }),
    /* @__PURE__ */ r.jsx(H, { path: "cluster", element: /* @__PURE__ */ r.jsx(We, {}) }),
    /* @__PURE__ */ r.jsx(H, { path: "topics", element: /* @__PURE__ */ r.jsx(Ke, {}) }),
    /* @__PURE__ */ r.jsx(H, { path: "broker", element: /* @__PURE__ */ r.jsx(He, {}) })
  ] });
}
export {
  qe as K,
  ur as M,
  dr as T,
  xe as Z,
  cr as a,
  ir as b,
  L as c,
  Se as d,
  K as e,
  lr as i,
  r as j,
  U as m,
  ze as r
};
