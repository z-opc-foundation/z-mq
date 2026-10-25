import { Routes as Ae, Route as ee, Navigate as qe } from "react-router-dom";
import Be, { useState as j, useRef as Me, useEffect as ne, useCallback as W, useMemo as Oe } from "react";
import { Alert as y, Card as q, Descriptions as H, Tag as x, Space as A, Button as D, Table as be, Input as xe, Row as he, Col as B, Statistic as M, Select as $e } from "antd";
import { ReloadOutlined as oe } from "@ant-design/icons";
import $ from "@yuku123/z-frontend-common";
import { PageHeader as ie, EmptyState as se, ListTable as fe, ComponentDoc as ze } from "@yuku123/z-frontend-common/ui";
const F = {
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
}, ke = {
  0: "SUCCESS",
  1: "SYSTEM_ERROR",
  2: "SYSTEM_BUSY",
  3: "REQUEST_CODE_NOT_SUPPORTED",
  4: "TRANSACTION_FAILED"
};
function dr(e) {
  return !!e && e.responseCode === 0;
}
function U(e) {
  var a;
  const t = (a = e == null ? void 0 : e.response) == null ? void 0 : a.data;
  if (t && typeof t == "object") {
    const c = t.message || t.error;
    if (c) return `${c}${t.hint ? ` — ${t.hint}` : ""}`;
    if (t.remark) return `z-mq 返回 responseCode=${t.responseCode} (${ke[t.responseCode] || "?"}) ${t.remark}`;
  }
  return typeof t == "string" && t.trim() ? t.slice(0, 300) : (e == null ? void 0 : e.message) || String(e);
}
function K(e) {
  return !e || e.responseCode === 0 ? null : `上游返回 responseCode=${e.responseCode}（${ke[e.responseCode] || "未知码"}）${e.remark ? `：${e.remark}` : ""}` + (e.responseCode === 3 ? ' —— 这个请求码在 z-mq 当前版本没注册，是"上游没实现"，不是"集群里没有"' : "");
}
function ur(e) {
  return !!(e && (e.attributionUnverified || e.attributionVerified === !1));
}
function mr(e) {
  var a;
  const t = (a = e == null ? void 0 : e.response) == null ? void 0 : a.status;
  return t === 503 || t === 502 ? !0 : /没有任何监听者|不是本 JVM|Address already in use|not bound|Connect/.test(String(U(e)));
}
const Le = {
  "broker/subscription": "BrokerOutAPI.handleGetAllSubscriptionGroup 的源码注释写着「MVP: 订阅组配置暂未单独存储, 返回空 wrapper + 默认版本号」，而且它实际 new 出来的是一个空的 ConsumerOffsetSerializeWrapper（body 里连 subscription 字段都没有）⇒ 这张表恒空，不代表集群里没有订阅组",
  "broker/delay-offset": "源码注释「MVP: 延迟消息 offset 与 Topic 自身 offset 共享 (无 SCHEDULE_TOPIC_XXXX 隔离队列)。返回与 GET_ALL_CONSUMER_OFFSET 一致即可」⇒ 这一格的 offsetTable 和 321 那份是同一份数据，只有 dataVersion 不同",
  "broker/consumer-offset": 'collectOffsetTable 的注释「简化: 遍历可能的 queueId (MVP 用 0-15)」且取的是各 group 的 max ⇒ 显示的是"该 topic 前 16 个队列里出现过的最大 offset"，不是某个消费组的真实位点'
};
function De(e) {
  if (!e || e.responseCode === 0) return null;
  const t = String(e.remark || "");
  return /not exist/i.test(t) ? t : null;
}
function Ce(e) {
  if (!e || typeof e != "object") return "—";
  const t = e.counter ?? e.counterValue, a = e.timestamp ? new Date(e.timestamp).toLocaleString("zh-CN", { hour12: !1 }) : "-";
  return `counter=${t ?? "?"} · timestamp=${a}`;
}
const pr = /^[A-Za-z0-9_\-.:;%#]{1,120}$/;
var ae = { exports: {} }, re = {};
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
function Ue() {
  if (we) return re;
  we = 1;
  var e = Symbol.for("react.transitional.element"), t = Symbol.for("react.fragment");
  function a(c, s, m) {
    var d = null;
    if (m !== void 0 && (d = "" + m), s.key !== void 0 && (d = "" + s.key), "key" in s) {
      m = {};
      for (var b in s)
        b !== "key" && (m[b] = s[b]);
    } else m = s;
    return s = m.ref, {
      $$typeof: e,
      type: c,
      key: d,
      ref: s !== void 0 ? s : null,
      props: m
    };
  }
  return re.Fragment = t, re.jsx = a, re.jsxs = a, re;
}
var te = {};
/**
 * @license React
 * react-jsx-runtime.development.js
 *
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
var _e;
function Ve() {
  return _e || (_e = 1, process.env.NODE_ENV !== "production" && (function() {
    function e(n) {
      if (n == null) return null;
      if (typeof n == "function")
        return n.$$typeof === Z ? null : n.displayName || n.name || null;
      if (typeof n == "string") return n;
      switch (n) {
        case o:
          return "Fragment";
        case k:
          return "Profiler";
        case i:
          return "StrictMode";
        case Y:
          return "Suspense";
        case G:
          return "SuspenseList";
        case h:
          return "Activity";
        case P:
          return "ViewTransition";
      }
      if (typeof n == "object")
        switch (typeof n.tag == "number" && console.error(
          "Received an unexpected object in getComponentNameFromType(). This is likely a bug in React. Please file an issue."
        ), n.$$typeof) {
          case I:
            return "Portal";
          case g:
            return n.displayName || "Context";
          case l:
            return (n._context.displayName || "Context") + ".Consumer";
          case O:
            var u = n.render;
            return n = n.displayName, n || (n = u.displayName || u.name || "", n = n !== "" ? "ForwardRef(" + n + ")" : "ForwardRef"), n;
          case Q:
            return u = n.displayName || null, u !== null ? u : e(n.type) || "Memo";
          case z:
            u = n._payload, n = n._init;
            try {
              return e(n(u));
            } catch {
            }
        }
      return null;
    }
    function t(n) {
      return "" + n;
    }
    function a(n) {
      try {
        t(n);
        var u = !1;
      } catch {
        u = !0;
      }
      if (u) {
        u = console;
        var T = u.error, S = typeof Symbol == "function" && Symbol.toStringTag && n[Symbol.toStringTag] || n.constructor.name || "Object";
        return T.call(
          u,
          "The provided key is an unsupported type %s. This value must be coerced to a string before using it here.",
          S
        ), t(n);
      }
    }
    function c(n) {
      if (n === o) return "<>";
      if (typeof n == "object" && n !== null && n.$$typeof === z)
        return "<...>";
      try {
        var u = e(n);
        return u ? "<" + u + ">" : "<...>";
      } catch {
        return "<...>";
      }
    }
    function s() {
      var n = ue.A;
      return n === null ? null : n.getOwner();
    }
    function m() {
      return Error("react-stack-top-frame");
    }
    function d(n) {
      if (ye.call(n, "key")) {
        var u = Object.getOwnPropertyDescriptor(n, "key").get;
        if (u && u.isReactWarning) return !1;
      }
      return n.key !== void 0;
    }
    function b(n, u) {
      function T() {
        ge || (ge = !0, console.error(
          "%s: `key` is not a prop. Trying to access it will result in `undefined` being returned. If you need to access the same value within the child component, you should pass it as a different prop. (https://react.dev/link/special-props)",
          u
        ));
      }
      T.isReactWarning = !0, Object.defineProperty(n, "key", {
        get: T,
        configurable: !0
      });
    }
    function f() {
      var n = e(this.type);
      return Te[n] || (Te[n] = !0, console.error(
        "Accessing element.ref was removed in React 19. ref is now a regular prop. It will be removed from the JSX Element type in a future release."
      )), n = this.props.ref, n !== void 0 ? n : null;
    }
    function E(n, u, T, S, V, L) {
      var C = T.ref;
      return n = {
        $$typeof: R,
        type: n,
        key: u,
        props: T,
        _owner: S
      }, (C !== void 0 ? C : null) !== null ? Object.defineProperty(n, "ref", {
        enumerable: !1,
        get: f
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
        value: L
      }), Object.freeze && (Object.freeze(n.props), Object.freeze(n)), n;
    }
    function p(n, u, T, S, V, L) {
      var C = u.children;
      if (C !== void 0)
        if (S)
          if (Ne(C)) {
            for (S = 0; S < C.length; S++)
              _(C[S]);
            Object.freeze && Object.freeze(C);
          } else
            console.error(
              "React.jsx: Static children should always be an array. You are likely explicitly calling React.jsxs or React.jsxDEV. Use the Babel transform instead."
            );
        else _(C);
      if (ye.call(u, "key")) {
        C = e(n);
        var J = Object.keys(u).filter(function(Ie) {
          return Ie !== "key";
        });
        S = 0 < J.length ? "{key: someKey, " + J.join(": ..., ") + ": ...}" : "{key: someKey}", Se[C + S] || (J = 0 < J.length ? "{" + J.join(": ..., ") + ": ...}" : "{}", console.error(
          `A props object containing a "key" prop is being spread into JSX:
  let props = %s;
  <%s {...props} />
React keys must be passed directly to JSX without using spread:
  let props = %s;
  <%s key={someKey} {...props} />`,
          S,
          C,
          J,
          C
        ), Se[C + S] = !0);
      }
      if (C = null, T !== void 0 && (a(T), C = "" + T), d(u) && (a(u.key), C = "" + u.key), "key" in u) {
        T = {};
        for (var pe in u)
          pe !== "key" && (T[pe] = u[pe]);
      } else T = u;
      return C && b(
        T,
        typeof n == "function" ? n.displayName || n.name || "Unknown" : n
      ), E(
        n,
        C,
        T,
        s(),
        V,
        L
      );
    }
    function _(n) {
      w(n) ? n._store && (n._store.validated = 1) : typeof n == "object" && n !== null && n.$$typeof === z && (n._payload.status === "fulfilled" ? w(n._payload.value) && n._payload.value._store && (n._payload.value._store.validated = 1) : n._store && (n._store.validated = 1));
    }
    function w(n) {
      return typeof n == "object" && n !== null && n.$$typeof === R;
    }
    var v = Be, R = Symbol.for("react.transitional.element"), I = Symbol.for("react.portal"), o = Symbol.for("react.fragment"), i = Symbol.for("react.strict_mode"), k = Symbol.for("react.profiler"), l = Symbol.for("react.consumer"), g = Symbol.for("react.context"), O = Symbol.for("react.forward_ref"), Y = Symbol.for("react.suspense"), G = Symbol.for("react.suspense_list"), Q = Symbol.for("react.memo"), z = Symbol.for("react.lazy"), h = Symbol.for("react.activity"), P = Symbol.for("react.view_transition"), Z = Symbol.for("react.client.reference"), ue = v.__CLIENT_INTERNALS_DO_NOT_USE_OR_WARN_USERS_THEY_CANNOT_UPGRADE, ye = Object.prototype.hasOwnProperty, Ne = Array.isArray, me = console.createTask ? console.createTask : function() {
      return null;
    };
    v = {
      react_stack_bottom_frame: function(n) {
        return n();
      }
    };
    var ge, Te = {}, Ee = v.react_stack_bottom_frame.bind(
      v,
      m
    )(), ve = me(c(m)), Se = {};
    te.Fragment = o, te.jsx = function(n, u, T) {
      var S = 1e4 > ue.recentlyCreatedOwnerStacks++;
      if (S) {
        var V = Error.stackTraceLimit;
        Error.stackTraceLimit = 10;
        var L = Error("react-stack-top-frame");
        Error.stackTraceLimit = V;
      } else L = Ee;
      return p(
        n,
        u,
        T,
        !1,
        L,
        S ? me(c(n)) : ve
      );
    }, te.jsxs = function(n, u, T) {
      var S = 1e4 > ue.recentlyCreatedOwnerStacks++;
      if (S) {
        var V = Error.stackTraceLimit;
        Error.stackTraceLimit = 10;
        var L = Error("react-stack-top-frame");
        Error.stackTraceLimit = V;
      } else L = Ee;
      return p(
        n,
        u,
        T,
        !0,
        L,
        S ? me(c(n)) : ve
      );
    };
  })()), te;
}
var Re;
function Fe() {
  return Re || (Re = 1, process.env.NODE_ENV === "production" ? ae.exports = Ue() : ae.exports = Ve()), ae.exports;
}
var r = Fe();
function le() {
  const [e, t] = j(null), [a, c] = j(!1), [s, m] = j(""), d = Me(!0);
  ne(() => () => {
    d.current = !1;
  }, []);
  const b = W(async () => {
    try {
      const f = await F.instance();
      if (!d.current) return;
      t(f), c(!1), m("");
    } catch (f) {
      if (!d.current) return;
      t(null), c(!0), m(U(f));
    }
  }, []);
  return { instance: e, instanceError: a, instanceErrorText: s, poll: b };
}
function ce({ data: e }) {
  if (!e) return null;
  const { namesrvForeignListenerSuspected: t, brokerForeignListenerSuspected: a, namesrvPort: c, brokerPort: s } = e;
  if (!t && !a) return null;
  const m = [
    t ? `NameServer :${c}（属主 PID ${e.namesrvPortOwnerPid}，我的子进程 PID ${e.namesrvChildPid}）` : null,
    a ? `Broker :${s}（属主 PID ${e.brokerPortOwnerPid}，我的子进程 PID ${e.brokerChildPid}）` : null
  ].filter(Boolean).join("；");
  return /* @__PURE__ */ r.jsx(
    y,
    {
      type: "error",
      showIcon: !0,
      style: { marginBottom: 16 },
      message: "有端口在应答，但应答者不是本次构建 spawn 的子进程 —— 下面所有表格都不算孵化结果",
      description: `外来嫌疑：${m}。` + (e.bindError ? ` 本 JVM 的子进程 bind 失败原文：${e.bindError}` : " 本 JVM 的子进程没有 bind 成功的记录。") + // 端口取实例自省回来的实际值：写死 9876/10911 会在换端口后把人指到别的进程上
      ` 处理方式只有两种：让出 ${c}/${s}，或给 zmq.namesrv-port / zmq.broker-port 换端口。 代理层不会替你去占端口，也不会去 kill 别人的进程。`
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
function je({ data: e }) {
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
function Ye(e) {
  if (!e) return "-";
  try {
    return new Date(e).toLocaleString("zh-CN", { hour12: !1 });
  } catch {
    return String(e);
  }
}
function Ge(e) {
  if (e == null) return "-";
  const t = Math.max(0, Math.floor((Date.now() - e) / 1e3));
  return t < 60 ? `${t} 秒前` : t < 3600 ? `${Math.floor(t / 60)} 分前` : t < 86400 ? `${Math.floor(t / 3600)} 时前` : `${Math.floor(t / 86400)} 天前`;
}
function N(e, t = "是", a = "否", c = "error") {
  return e == null ? /* @__PURE__ */ r.jsx(x, { children: "未知" }) : e ? /* @__PURE__ */ r.jsx(x, { color: "success", children: t }) : /* @__PURE__ */ r.jsx(x, { color: c, children: a });
}
const Qe = [
  { title: "逻辑端点", dataIndex: "ep", key: "ep", width: 240, render: (e) => /* @__PURE__ */ r.jsx("code", { children: e }) },
  { title: "请求码", dataIndex: "code", key: "code", width: 100, render: (e) => /* @__PURE__ */ r.jsx(x, { children: e }) },
  { title: "常量名", dataIndex: "name", key: "name", render: (e) => /* @__PURE__ */ r.jsx("code", { children: e }) }
];
function We() {
  var I;
  const { instance: e, instanceError: t, instanceErrorText: a, poll: c } = le(), [s, m] = j(null), [d, b] = j(null), [f, E] = j(!1), p = W(async (o) => {
    E(!0);
    try {
      const i = await F[o]();
      m({ which: o, res: i }), b(null);
    } catch (i) {
      m(null), b(i);
    } finally {
      E(!1), c();
    }
  }, [c]), _ = e ? Object.entries(e.readOnlyRequestCodes || {}).map(([o, i]) => ({
    ep: o,
    code: i,
    name: (e.requestCodeNames || {})[String(i)] || "-",
    key: o
  })) : [], [w, v] = j(""), R = w.trim() ? _.filter((o) => `${o.ep || ""} ${o.code || ""} ${o.name || ""}`.toLowerCase().includes(w.trim().toLowerCase())) : _;
  return /* @__PURE__ */ r.jsxs("div", { children: [
    /* @__PURE__ */ r.jsx(
      ie,
      {
        title: "实例与端口",
        subtitle: "自省接口 GET /api/mq/__instance（由 MqProxyController 自己应答，不经过 z-mq）+ 只读请求码实发探针"
      }
    ),
    /* @__PURE__ */ r.jsx(q, { extra: /* @__PURE__ */ r.jsxs(A, { children: [
      /* @__PURE__ */ r.jsx(D, { icon: /* @__PURE__ */ r.jsx(oe, {}), loading: f, onClick: () => p("cluster"), children: "实发一发 码4 GET_BROKER_CLUSTER_INFO" }),
      /* @__PURE__ */ r.jsx(D, { icon: /* @__PURE__ */ r.jsx(oe, {}), onClick: c, children: "刷新自省" })
    ] }), children: t ? /* @__PURE__ */ r.jsx(
      y,
      {
        type: "error",
        showIcon: !0,
        message: `自省接口调用失败：${a}`,
        description: "/api/mq/__instance 不碰 NameServer 也不碰 Broker，由 z-opc 自己的 controller 直接应答；它都不通说明是 z-opc 侧的路由/认证问题（302 到登录页 = 没带会话 Cookie；404 = MainWebConfig.spaPaths 或 /api/mq 映射没接上）。"
      }
    ) : /* @__PURE__ */ r.jsxs(r.Fragment, { children: [
      /* @__PURE__ */ r.jsx(je, { data: e }),
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
        H,
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
              children: /* @__PURE__ */ r.jsx(x, { color: "volcano", children: (e == null ? void 0 : e.surface) ?? "-" })
            },
            {
              key: "loaded",
              label: "内嵌配置已装载",
              children: e ? e.embeddedConfigLoaded ? /* @__PURE__ */ r.jsx(x, { color: "success", children: "是" }) : /* @__PURE__ */ r.jsx(x, { color: "error", children: "否（zmq.enabled≠true）" }) : "-"
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
              children: e ? /* @__PURE__ */ r.jsxs(A, { children: [
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
      q,
      {
        title: "子进程 bind 失败原文（spawn 器落的日志）",
        style: { marginTop: 16 },
        extra: /* @__PURE__ */ r.jsx(x, { color: "blue", children: "ZMqEmbeddedServerConfig 把子进程 stdout/stderr 追加到这里" }),
        children: e ? /* @__PURE__ */ r.jsx(
          H,
          {
            bordered: !0,
            size: "small",
            column: 1,
            items: ["namesrvSpawnLog", "brokerSpawnLog"].map((o) => {
              const i = e[o] || {};
              return {
                key: o,
                label: o === "namesrvSpawnLog" ? "NameServer" : "Broker",
                children: /* @__PURE__ */ r.jsxs("div", { style: { fontSize: 12 }, children: [
                  /* @__PURE__ */ r.jsxs("div", { children: [
                    /* @__PURE__ */ r.jsx("code", { children: i.path }),
                    " ",
                    N(i.exists, "存在", "不存在", "warning"),
                    " ",
                    i.exists ? /* @__PURE__ */ r.jsxs("span", { style: { color: "#94a3b8" }, children: [
                      "（",
                      Ge(i.lastModified),
                      "写入）"
                    ] }) : null
                  ] }),
                  i.bindErrorLines ? /* @__PURE__ */ r.jsxs("div", { style: { color: "#dc2626" }, children: [
                    "命中 ",
                    i.bindErrorLines,
                    " 行 bind 失败，最后一条：",
                    /* @__PURE__ */ r.jsx("code", { children: i.lastBindError })
                  ] }) : /* @__PURE__ */ r.jsx("div", { style: { color: "#94a3b8" }, children: "日志里没有 bind 失败记录" })
                ] })
              };
            })
          }
        ) : /* @__PURE__ */ r.jsx(y, { type: "info", showIcon: !0, message: "自省接口没通，读不到日志字段" })
      }
    ),
    /* @__PURE__ */ r.jsx(q, { title: "实发一发只读请求码（证明协议适配器本身成立，而不只是在报自省）", style: { marginTop: 16 }, children: /* @__PURE__ */ r.jsxs(A, { direction: "vertical", style: { width: "100%" }, children: [
      /* @__PURE__ */ r.jsxs(A, { wrap: !0, children: [
        /* @__PURE__ */ r.jsx(D, { size: "small", loading: f, onClick: () => p("cluster"), children: "码 4 · 集群注册表" }),
        /* @__PURE__ */ r.jsx(D, { size: "small", loading: f, onClick: () => p("topics"), children: "码 8 · NameServer topic 列表" }),
        /* @__PURE__ */ r.jsx(D, { size: "small", loading: f, onClick: () => p("brokerTopics"), children: "Broker 码 8 · 本 broker topic" }),
        /* @__PURE__ */ r.jsx(D, { size: "small", loading: f, onClick: () => p("brokerDataVersion"), children: "Broker 码 324 · DataVersion" })
      ] }),
      d && /* @__PURE__ */ r.jsx(
        y,
        {
          type: "error",
          showIcon: !0,
          message: `探针失败：${U(d)}`
        }
      ),
      s && /* @__PURE__ */ r.jsxs(r.Fragment, { children: [
        /* @__PURE__ */ r.jsx(X, { res: s.res }),
        /* @__PURE__ */ r.jsx(
          H,
          {
            bordered: !0,
            size: "small",
            column: 3,
            items: [
              { key: "t", label: "打的哪一档", children: /* @__PURE__ */ r.jsx(x, { children: s.res.tier }) },
              { key: "target", label: "目标地址", children: /* @__PURE__ */ r.jsx("code", { children: s.res.target }) },
              { key: "req", label: "请求码", children: /* @__PURE__ */ r.jsxs("code", { children: [
                s.res.requestCode,
                " ",
                s.res.requestName
              ] }) },
              {
                key: "responseCode",
                label: "z-mq 业务码",
                children: /* @__PURE__ */ r.jsxs(x, { color: s.res.responseCode === 0 ? "success" : "error", children: [
                  s.res.responseCode,
                  " · ",
                  ke[s.res.responseCode] || "未知码"
                ] })
              },
              { key: "remark", label: "remark", children: s.res.remark || /* @__PURE__ */ r.jsx("span", { style: { color: "#94a3b8" }, children: "—" }) },
              { key: "elapsed", label: "耗时", children: `${s.res.elapsedMs ?? "-"} ms` },
              { key: "attr", label: "归属已验证", children: N(s.res.attributionVerified) },
              { key: "size", label: "响应体字节数", children: JSON.stringify(s.res.data ?? null).length },
              {
                key: "ts",
                label: "本页数据读取时刻",
                children: Ye(Date.now())
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
        }, children: JSON.stringify(s.res.data ?? null, null, 2) })
      ] }),
      !s && !d && /* @__PURE__ */ r.jsx(
        y,
        {
          type: "info",
          showIcon: !0,
          message: "还没发探针",
          description: "自省接口证明的是「z-opc 这一层活着、且知道自己没起」；发一发只读码证明的是「协议适配这条形状走得通」。两个都要。"
        }
      )
    ] }) }),
    /* @__PURE__ */ r.jsxs(
      q,
      {
        title: "本代理可达的只读请求码（穷举，无通用转发口）",
        style: { marginTop: 16 },
        extra: /* @__PURE__ */ r.jsx(A, { children: /* @__PURE__ */ r.jsx(
          xe.Search,
          {
            allowClear: !0,
            placeholder: "按逻辑端点 / 请求码 / 常量名过滤",
            style: { width: 240 },
            onChange: (o) => v(o.target.value)
          }
        ) }),
        children: [
          /* @__PURE__ */ r.jsx(
            be,
            {
              size: "small",
              rowKey: "ep",
              dataSource: R,
              columns: Qe,
              pagination: !1
            }
          ),
          ((I = e == null ? void 0 : e.unavailableOnBroker120) == null ? void 0 : I.length) > 0 && /* @__PURE__ */ r.jsx(
            y,
            {
              type: "warning",
              showIcon: !0,
              style: { marginTop: 12 },
              message: "以下能力在 z-mq 1.2.0 上根本没有请求码，页面不做也不假做",
              description: /* @__PURE__ */ r.jsxs("div", { children: [
                /* @__PURE__ */ r.jsx("div", { style: { fontWeight: 600, marginTop: 4 }, children: "Broker 侧未注册：" }),
                /* @__PURE__ */ r.jsx("ul", { style: { margin: "4px 0 0 18px" }, children: e.unavailableOnBroker120.map((o) => /* @__PURE__ */ r.jsx("li", { children: o }, o)) }),
                /* @__PURE__ */ r.jsx("div", { style: { fontWeight: 600, marginTop: 4 }, children: "NameServer 侧未实现：" }),
                /* @__PURE__ */ r.jsx("ul", { style: { margin: "4px 0 0 18px" }, children: (e.unavailableOnNamesrv120 || []).map((o) => /* @__PURE__ */ r.jsx("li", { children: o }, o)) })
              ] })
            }
          )
        ]
      }
    )
  ] });
}
const Je = [
  {
    title: "Broker 名",
    dataIndex: "brokerName",
    key: "brokerName",
    width: 180,
    render: (e) => /* @__PURE__ */ r.jsx("code", { children: e })
  },
  { title: "Cluster", dataIndex: "cluster", key: "cluster", width: 160, render: (e) => e || /* @__PURE__ */ r.jsx(x, { children: "未上报" }) },
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
    render: (e) => e.length ? /* @__PURE__ */ r.jsx(A, { direction: "vertical", size: 0, children: e.map((t) => /* @__PURE__ */ r.jsxs("span", { children: [
      /* @__PURE__ */ r.jsxs(x, { color: t.id === "0" ? "blue" : "default", children: [
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
    render: (e, t) => t.port ? t.portMatched === null ? /* @__PURE__ */ r.jsx(x, { color: "warning", children: "配置端口未知" }) : t.portMatched ? /* @__PURE__ */ r.jsxs(x, { color: "success", children: [
      "一致（",
      t.port,
      "）"
    ] }) : /* @__PURE__ */ r.jsxs(x, { color: "error", children: [
      "不一致：注册的是 ",
      t.port,
      "，本 JVM 配的是 ",
      t.expected
    ] }) : /* @__PURE__ */ r.jsx(x, { children: "无地址" })
  }
];
function He() {
  const { instance: e, poll: t } = le(), [a, c] = j(null), [s, m] = j(null), [d, b] = j(!1), f = W(async () => {
    b(!0);
    try {
      const l = await F.cluster();
      c(l), m(null);
    } catch (l) {
      c(null), m(l);
    } finally {
      b(!1);
    }
  }, []);
  ne(() => {
    f(), t();
  }, [f, t]);
  const E = (a == null ? void 0 : a.data) || {}, p = E.brokerAddrTable && typeof E.brokerAddrTable == "object" ? E.brokerAddrTable : {}, _ = E.clusterAddrTable && typeof E.clusterAddrTable == "object" ? E.clusterAddrTable : {}, w = (e == null ? void 0 : e.brokerPort) ?? null, v = Object.keys(p).map((l) => {
    var z;
    const g = p[l] || {}, O = g.brokerAddrs && typeof g.brokerAddrs == "object" ? g.brokerAddrs : {}, Y = Object.keys(O).map((h) => ({ id: h, addr: String(O[h]) })), G = O[0] ? String(O[0]) : ((z = Y[0]) == null ? void 0 : z.addr) || null, Q = G ? Number(G.split(":").pop()) : null;
    return {
      key: l,
      brokerName: g.brokerName || l,
      cluster: g.cluster,
      masterAddr: G,
      replicas: Y,
      port: Q,
      expected: w,
      portMatched: w == null ? null : Q === w
    };
  }), R = Object.keys(_).map((l) => ({
    key: l,
    cluster: l,
    members: (Array.isArray(_[l]) ? _[l] : []).map(String)
  })), [I, o] = j(""), i = I.trim() ? v.filter((l) => `${l.brokerName || ""} ${l.cluster || ""} ${l.masterAddr || ""}`.toLowerCase().includes(I.trim().toLowerCase())) : v, k = a && !s ? K(a) : null;
  return /* @__PURE__ */ r.jsxs("div", { children: [
    /* @__PURE__ */ r.jsx(
      ie,
      {
        title: "集群与 Broker",
        subtitle: "NameServer 码 4 GET_BROKER_CLUSTER_INFO → brokerAddrTable / clusterAddrTable"
      }
    ),
    /* @__PURE__ */ r.jsxs(q, { extra: /* @__PURE__ */ r.jsxs(A, { children: [
      /* @__PURE__ */ r.jsx(
        xe.Search,
        {
          allowClear: !0,
          placeholder: "按 Broker 名 / Cluster / Master 地址过滤",
          style: { width: 240 },
          onChange: (l) => o(l.target.value)
        }
      ),
      /* @__PURE__ */ r.jsx(D, { icon: /* @__PURE__ */ r.jsx(oe, {}), loading: d, onClick: () => {
        f(), t();
      }, children: "刷新" })
    ] }), children: [
      /* @__PURE__ */ r.jsx(ce, { data: e }),
      /* @__PURE__ */ r.jsx(de, { data: e }),
      s && /* @__PURE__ */ r.jsx(
        y,
        {
          type: "error",
          showIcon: !0,
          message: `读 NameServer 失败：${U(s)}`,
          description: `这一档是 nameserver 的只读码，不需要 broker 注册就能应答；它不通说明 :${(e == null ? void 0 : e.namesrvPort) ?? "?"} 上根本没有能答话的进程（或者答话的那个跟本 JVM 不是一家的，见顶部红条）。端口取自 /api/mq/__instance，写死端口会在换端口后把人指到别的进程上。`
        }
      ),
      k && /* @__PURE__ */ r.jsx(y, { type: "warning", showIcon: !0, style: { marginBottom: 12 }, message: k }),
      a && !s && /* @__PURE__ */ r.jsxs(r.Fragment, { children: [
        /* @__PURE__ */ r.jsx(X, { res: a }),
        /* @__PURE__ */ r.jsxs(he, { gutter: 16, style: { marginBottom: 12 }, children: [
          /* @__PURE__ */ r.jsx(B, { span: 6, children: /* @__PURE__ */ r.jsx(M, { title: "已注册 Broker 数", value: v.length }) }),
          /* @__PURE__ */ r.jsx(B, { span: 6, children: /* @__PURE__ */ r.jsx(M, { title: "Cluster 数", value: R.length }) }),
          /* @__PURE__ */ r.jsx(B, { span: 6, children: /* @__PURE__ */ r.jsx(M, { title: "请求码", value: `${a.requestCode || "-"} ${a.requestName || ""}` }) }),
          /* @__PURE__ */ r.jsx(B, { span: 6, children: /* @__PURE__ */ r.jsx(M, { title: "往返耗时", value: a.elapsedMs ?? "-", suffix: "ms" }) })
        ] }),
        v.length === 0 ? /* @__PURE__ */ r.jsx(
          se,
          {
            title: "brokerAddrTable 是空的",
            description: "没有任何 broker 向这台 NameServer 发过 REGISTER_BROKER（码 1）。两种成因要分开：① broker 子进程没起 / 没 bind 上（看顶部红条与「实例与端口」页的 bind 原文）；② broker 起了但配的 zmq.namesrv-addr 指向的是另一台 NameServer。本页不给「注册 broker」按钮 —— 那是写面，另立卡。"
          }
        ) : /* @__PURE__ */ r.jsx(
          be,
          {
            size: "small",
            rowKey: "key",
            loading: d,
            dataSource: i,
            columns: Je,
            pagination: !1
          }
        )
      ] }),
      !a && !s && /* @__PURE__ */ r.jsx(se, { title: "还没读到数据", description: "刷新以发送一次只读请求码 4。" })
    ] }),
    R.length > 0 && /* @__PURE__ */ r.jsx(q, { title: "Cluster → Broker 归属", style: { marginTop: 16 }, children: /* @__PURE__ */ r.jsx(
      H,
      {
        bordered: !0,
        size: "small",
        column: 1,
        items: R.map((l) => ({
          key: l.key,
          label: l.cluster,
          children: /* @__PURE__ */ r.jsx(A, { wrap: !0, children: l.members.map((g) => /* @__PURE__ */ r.jsx(x, { children: g }, g)) })
        }))
      }
    ) }),
    /* @__PURE__ */ r.jsx(q, { title: "上游语义提示", style: { marginTop: 16 }, children: /* @__PURE__ */ r.jsxs("div", { style: { color: "#64748b", fontSize: 12, lineHeight: 1.9 }, children: [
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
const Ke = [
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
    render: (e) => e ? /* @__PURE__ */ r.jsx(x, { color: "success", children: "已注册" }) : /* @__PURE__ */ r.jsx(x, { color: "error", children: "没有" })
  },
  {
    title: "Broker (码 8)",
    dataIndex: "onBroker",
    key: "onBroker",
    width: 140,
    render: (e, t) => e ? /* @__PURE__ */ r.jsx(x, { color: "success", children: "有配置" }) : t.brokerUnavailable ? /* @__PURE__ */ r.jsx(x, { children: "未取到" }) : /* @__PURE__ */ r.jsx(x, { color: "error", children: "没有" })
  },
  {
    title: "一致性",
    key: "consistency",
    width: 200,
    render: (e, t) => t.onNamesrv && t.onBroker ? /* @__PURE__ */ r.jsx(x, { color: "success", children: "两侧一致" }) : /* @__PURE__ */ r.jsx(x, { color: "warning", children: t.onNamesrv ? "只在 NameServer（疑似残留注册）" : "只在 Broker（未向 NameServer 注册）" })
  }
];
function Xe() {
  var Q, z;
  const { instance: e, poll: t } = le(), [a, c] = j(null), [s, m] = j(null), [d, b] = j(null), [f, E] = j(null), [p, _] = j(!1), [w, v] = j({}), R = W(async () => {
    _(!0), b(null), E(null), await Promise.all([
      F.topics().then(c).catch((h) => {
        c(null), b(h);
      }),
      F.brokerTopics().then(m).catch((h) => {
        m(null), E(h);
      })
    ]), _(!1), t();
  }, [t]);
  ne(() => {
    R();
  }, [R]);
  const I = W(async (h) => {
    if (!w[h]) {
      v((P) => ({ ...P, [h]: { loading: !0 } }));
      try {
        const P = await F.route(h);
        v((Z) => ({ ...Z, [h]: P }));
      } catch (P) {
        v((Z) => ({ ...Z, [h]: { loadError: U(P) } }));
      }
    }
  }, [w]), o = a && a.responseCode === 0 && Array.isArray((Q = a.data) == null ? void 0 : Q.topics) ? a.data.topics.map(String) : null, i = s && s.responseCode === 0 && Array.isArray((z = s.data) == null ? void 0 : z.topics) ? s.data.topics.map(String) : null, k = Oe(() => {
    const h = /* @__PURE__ */ new Set([...o || [], ...i || []]);
    return Array.from(h).sort().map((P) => ({
      key: P,
      topic: P,
      onNamesrv: o ? o.includes(P) : !1,
      onBroker: i ? i.includes(P) : !1,
      brokerUnavailable: !i
    }));
  }, [o, i]), l = k.filter((h) => h.onNamesrv && !h.onBroker).length, g = k.filter((h) => !h.onNamesrv && h.onBroker).length, O = a && !d ? K(a) : null, Y = s && !f ? K(s) : null, G = !o && !i;
  return /* @__PURE__ */ r.jsxs("div", { children: [
    /* @__PURE__ */ r.jsx(ie, { title: "Topic 列表" }),
    /* @__PURE__ */ r.jsxs(q, { extra: /* @__PURE__ */ r.jsx(A, { children: /* @__PURE__ */ r.jsx(D, { icon: /* @__PURE__ */ r.jsx(oe, {}), loading: p, onClick: R, children: "刷新" }) }), children: [
      /* @__PURE__ */ r.jsx(ce, { data: e }),
      /* @__PURE__ */ r.jsx(de, { data: e }),
      /* @__PURE__ */ r.jsx(je, { data: e }),
      d && /* @__PURE__ */ r.jsx(
        y,
        {
          type: "error",
          showIcon: !0,
          style: { marginBottom: 12 },
          message: `读 NameServer 的 topic 列表失败：${U(d)}`
        }
      ),
      f && /* @__PURE__ */ r.jsx(
        y,
        {
          type: "error",
          showIcon: !0,
          style: { marginBottom: 12 },
          message: `读 Broker 的 topicConfigTable 失败：${U(f)}`,
          description: "Broker 侧的地址全靠 NameServer 的码 4 发现，且只允许环回。这一档失败通常说明集群里根本没有注册过 broker（见「集群与 Broker」页），或者代理层按环回白名单拒了外部地址 —— 后者是正确行为。"
        }
      ),
      O && /* @__PURE__ */ r.jsx(y, { type: "warning", showIcon: !0, style: { marginBottom: 12 }, message: `NameServer：${O}` }),
      Y && /* @__PURE__ */ r.jsx(y, { type: "warning", showIcon: !0, style: { marginBottom: 12 }, message: `Broker：${Y}` }),
      a && !d && /* @__PURE__ */ r.jsx(X, { res: a }),
      s && !f && /* @__PURE__ */ r.jsx(X, { res: s }),
      /* @__PURE__ */ r.jsxs(he, { gutter: 16, style: { marginBottom: 12 }, children: [
        /* @__PURE__ */ r.jsx(B, { span: 6, children: /* @__PURE__ */ r.jsx(M, { title: "NameServer 侧 topic 数", value: o ? o.length : "—" }) }),
        /* @__PURE__ */ r.jsx(B, { span: 6, children: /* @__PURE__ */ r.jsx(M, { title: "Broker 侧 topic 数", value: i ? i.length : "—" }) }),
        /* @__PURE__ */ r.jsx(B, { span: 6, children: /* @__PURE__ */ r.jsx(
          M,
          {
            title: "只在 NameServer",
            value: i ? l : "—",
            valueStyle: l && i ? { color: "#b45309" } : void 0
          }
        ) }),
        /* @__PURE__ */ r.jsx(B, { span: 6, children: /* @__PURE__ */ r.jsx(
          M,
          {
            title: "只在 Broker",
            value: i ? g : "—",
            valueStyle: g && i ? { color: "#b45309" } : void 0
          }
        ) })
      ] }),
      G ? /* @__PURE__ */ r.jsx(
        se,
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
            rows: k,
            columns: Ke,
            rowKey: "key",
            loading: p,
            searchKeys: "topic",
            searchPlaceholder: "按 topic 名过滤",
            cardProps: { variant: "borderless", styles: { body: { padding: 0 } } },
            expandable: {
              onExpand: (h, P) => {
                h && I(P.topic);
              },
              expandedRowRender: (h) => /* @__PURE__ */ r.jsx(Ze, { state: w[h.topic] })
            }
          }
        )
      )
    ] }),
    /* @__PURE__ */ r.jsx(q, { title: "这一页的两个语义坑", style: { marginTop: 16 }, children: /* @__PURE__ */ r.jsxs("div", { style: { color: "#64748b", fontSize: 12, lineHeight: 1.9 }, children: [
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
function Ze({ state: e }) {
  var f, E;
  const [t, a] = j("");
  if (!e) return /* @__PURE__ */ r.jsx("span", { style: { color: "#94a3b8", fontSize: 12 }, children: "展开时向 NameServer 发送码 3 GET_ROUTEINFO_BY_TOPIC。" });
  if (e.loading) return /* @__PURE__ */ r.jsx("span", { style: { color: "#94a3b8", fontSize: 12 }, children: "查询中…" });
  if (e.loadError) return /* @__PURE__ */ r.jsx(y, { type: "error", showIcon: !0, message: `路由查询失败：${e.loadError}` });
  const c = De(e), s = K(e), m = Array.isArray((f = e.data) == null ? void 0 : f.queueDatas) ? e.data.queueDatas : [], d = Array.isArray((E = e.data) == null ? void 0 : E.brokerDatas) ? e.data.brokerDatas : [], b = t.trim() ? m.filter((p) => `${p.brokerName || ""} ${p.perm ?? ""} ${p.topicSysFlag ?? ""}`.toLowerCase().includes(t.trim().toLowerCase())) : m;
  return /* @__PURE__ */ r.jsxs("div", { children: [
    c && /* @__PURE__ */ r.jsx(
      y,
      {
        type: "warning",
        showIcon: !0,
        style: { marginBottom: 8 },
        message: `NameServer 报这个 topic 没有路由（原文：${c}）`,
        description: "上游用的是 SYSTEM_ERROR(1) + remark，不是「查到但是空」，所以这一格不能渲染成空表。"
      }
    ),
    !c && s && /* @__PURE__ */ r.jsx(y, { type: "error", showIcon: !0, style: { marginBottom: 8 }, message: s }),
    e.data && /* @__PURE__ */ r.jsx(X, { res: e }),
    e.data && /* @__PURE__ */ r.jsxs(r.Fragment, { children: [
      /* @__PURE__ */ r.jsx(
        H,
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
      /* @__PURE__ */ r.jsx(A, { style: { marginBottom: 8 }, children: /* @__PURE__ */ r.jsx(
        xe.Search,
        {
          allowClear: !0,
          placeholder: "按 Broker / perm / topicSysFlag 过滤",
          style: { width: 220 },
          onChange: (p) => a(p.target.value)
        }
      ) }),
      /* @__PURE__ */ r.jsx(
        be,
        {
          size: "small",
          rowKey: (p) => `${p.brokerName}-${p.queueId ?? ""}`,
          dataSource: b,
          pagination: !1,
          columns: [
            { title: "Broker", dataIndex: "brokerName", key: "brokerName", render: (p) => /* @__PURE__ */ r.jsx("code", { children: p }) },
            { title: "readQueueNums", dataIndex: "readQueueNums", key: "readQueueNums" },
            { title: "writeQueueNums", dataIndex: "writeQueueNums", key: "writeQueueNums" },
            { title: "perm", dataIndex: "perm", key: "perm" },
            { title: "topicSysFlag", dataIndex: "topicSysFlag", key: "topicSysFlag" }
          ]
        }
      ),
      d.length > 0 && /* @__PURE__ */ r.jsxs("div", { style: { marginTop: 8, color: "#64748b", fontSize: 12 }, children: [
        "brokerDatas：",
        d.map((p) => `${p.brokerName}→${JSON.stringify(p.brokerAddrs || {})}`).join("，")
      ] })
    ] })
  ] });
}
const Pe = [
  { key: "topicConfig", path: "broker/topic-config", label: "Topic 配置（码 320）", api: "brokerTopicConfig" },
  { key: "consumerOffset", path: "broker/consumer-offset", label: "消费位点（码 321）", api: "brokerConsumerOffset" },
  { key: "delayOffset", path: "broker/delay-offset", label: "延迟位点（码 322）", api: "brokerDelayOffset" },
  { key: "subscription", path: "broker/subscription", label: "订阅组（码 323）", api: "brokerSubscription" },
  { key: "dataVersion", path: "broker/data-version", label: "DataVersion（码 324）", api: "brokerDataVersion" }
], er = [
  { title: "Topic", dataIndex: "topic", key: "topic", render: (e) => /* @__PURE__ */ r.jsx("code", { children: e }) },
  { title: "readQueueNums", dataIndex: "readQueueNums", key: "readQueueNums", width: 130 },
  { title: "writeQueueNums", dataIndex: "writeQueueNums", key: "writeQueueNums", width: 140 },
  { title: "perm", dataIndex: "perm", key: "perm", width: 90 },
  {
    title: "order",
    dataIndex: "order",
    key: "order",
    width: 90,
    render: (e) => e ? /* @__PURE__ */ r.jsx(x, { color: "blue", children: "true" }) : /* @__PURE__ */ r.jsx(x, { children: "false" })
  },
  { title: "unit", dataIndex: "unit", key: "unit", width: 110, render: (e) => e ?? "—" }
];
function rr() {
  const { instance: e, poll: t } = le(), [a, c] = j([]), [s, m] = j(null), [d, b] = j(null), [f, E] = j({}), [p, _] = j(!1), w = W(async () => {
    var o;
    try {
      const i = await F.cluster(), k = ((o = i == null ? void 0 : i.data) == null ? void 0 : o.brokerAddrTable) || {}, l = Object.keys(k);
      return c(l), m((g) => g && l.includes(g) ? g : l[0] || null), b(i && i.responseCode !== 0 ? new Error(K(i) || "码 4 返回非 0") : null), l;
    } catch (i) {
      return c([]), m(null), b(i), [];
    }
  }, []), v = W(async (o) => {
    _(!0), await Promise.all(Pe.map(async (i) => {
      E((k) => ({ ...k, [i.key]: { loading: !0 } }));
      try {
        const k = await F[i.api](o || void 0);
        E((l) => ({ ...l, [i.key]: k }));
      } catch (k) {
        E((l) => ({ ...l, [i.key]: { loadError: U(k), raw: k } }));
      }
    })), _(!1), t();
  }, [t]);
  ne(() => {
    w();
  }, [w]), ne(() => {
    v(s || void 0);
  }, [s, v]);
  const R = (o) => {
    var k;
    const i = (k = o == null ? void 0 : o.data) == null ? void 0 : k.offsetTable;
    return !i || typeof i != "object" ? null : Object.keys(i).flatMap((l) => {
      const g = i[l];
      return !g || typeof g != "object" ? [] : Object.keys(g).map((O) => ({
        key: `${l}-${O}`,
        topic: l,
        queueId: O,
        maxOffset: g[O]
      }));
    });
  }, I = Oe(() => {
    var i, k;
    const o = (k = (i = f.topicConfig) == null ? void 0 : i.data) == null ? void 0 : k.topicConfigTable;
    return !o || typeof o != "object" ? null : Object.keys(o).map((l) => ({ key: l, topic: l, ...o[l] || {} }));
  }, [f.topicConfig]);
  return /* @__PURE__ */ r.jsxs("div", { children: [
    /* @__PURE__ */ r.jsx(
      ie,
      {
        title: "Broker 元数据",
        subtitle: "BrokerOutAPI 码 320/321/322/323/324 · 地址由 NameServer 码 4 发现且只允许环回"
      }
    ),
    /* @__PURE__ */ r.jsxs(q, { extra: /* @__PURE__ */ r.jsxs(A, { children: [
      /* @__PURE__ */ r.jsx("span", { style: { color: "#64748b", fontSize: 12 }, children: "Broker" }),
      /* @__PURE__ */ r.jsx(
        $e,
        {
          size: "small",
          style: { width: 240 },
          value: s,
          onChange: m,
          placeholder: a.length ? void 0 : "码 4 没有发现任何 broker",
          options: a.map((o) => ({ value: o, label: o }))
        }
      ),
      /* @__PURE__ */ r.jsx(D, { icon: /* @__PURE__ */ r.jsx(oe, {}), loading: p, onClick: () => v(s || void 0), children: "刷新五档" })
    ] }), children: [
      /* @__PURE__ */ r.jsx(ce, { data: e }),
      /* @__PURE__ */ r.jsx(de, { data: e }),
      /* @__PURE__ */ r.jsx(je, { data: e }),
      d && /* @__PURE__ */ r.jsx(
        y,
        {
          type: "error",
          showIcon: !0,
          style: { marginBottom: 12 },
          message: `无法从 NameServer 发现 broker：${U(d)}`,
          description: "下面五档的地址全靠码 4，发现不到就只能整页 503 —— 那是「集群里没有注册过的 broker」，不是这五档本身坏了。"
        }
      ),
      /* @__PURE__ */ r.jsxs(he, { gutter: 16, children: [
        /* @__PURE__ */ r.jsx(B, { span: 8, children: /* @__PURE__ */ r.jsx(M, { title: "码 4 发现的 broker 数", value: a.length }) }),
        /* @__PURE__ */ r.jsx(B, { span: 8, children: /* @__PURE__ */ r.jsx(M, { title: "当前取数对象", value: s || "—" }) }),
        /* @__PURE__ */ r.jsx(B, { span: 8, children: /* @__PURE__ */ r.jsx(
          M,
          {
            title: "store 路径存在",
            value: (e == null ? void 0 : e.storePathExists) == null ? "未知" : e.storePathExists ? "是" : "否"
          }
        ) })
      ] })
    ] }),
    Pe.map((o) => /* @__PURE__ */ r.jsx(
      tr,
      {
        section: o,
        state: f[o.key],
        rows: o.key === "topicConfig" ? I : o.key === "consumerOffset" || o.key === "delayOffset" ? R(f[o.key]) : null
      },
      o.key
    ))
  ] });
}
function tr({ section: e, state: t, rows: a }) {
  const c = t == null ? void 0 : t.loadError, s = t && !c && t.responseCode !== void 0 ? K(t) : null, m = Le[e.path], d = t == null ? void 0 : t.data;
  return /* @__PURE__ */ r.jsxs(
    q,
    {
      title: e.label,
      style: { marginTop: 16 },
      size: "small",
      extra: t != null && t.target ? /* @__PURE__ */ r.jsx(x, { children: String(t.target) }) : null,
      children: [
        !t || t.loading ? /* @__PURE__ */ r.jsx("span", { style: { color: "#94a3b8", fontSize: 12 }, children: "取数中…" }) : null,
        c && /* @__PURE__ */ r.jsx(y, { type: "error", showIcon: !0, message: `调用失败：${c}` }),
        s && /* @__PURE__ */ r.jsx(y, { type: "warning", showIcon: !0, style: { marginBottom: 12 }, message: s }),
        t && !c && /* @__PURE__ */ r.jsx(X, { res: t }),
        m && /* @__PURE__ */ r.jsx(
          y,
          {
            type: "info",
            showIcon: !0,
            style: { marginBottom: 12 },
            message: "上游语义（不是本页的推断）",
            description: m
          }
        ),
        t && !c && d && e.key === "topicConfig" && (a && a.length ? /* @__PURE__ */ r.jsx(
          fe,
          {
            rows: a,
            columns: er,
            rowKey: "key",
            searchKeys: "topic",
            searchPlaceholder: "按 Topic 名过滤",
            cardProps: { variant: "borderless", styles: { body: { padding: 0 } } }
          }
        ) : /* @__PURE__ */ r.jsx(se, { title: "topicConfigTable 是空的", description: "本 broker 没有注册任何 topic 配置。" })),
        t && !c && d && (e.key === "consumerOffset" || e.key === "delayOffset") && (a && a.length ? /* @__PURE__ */ r.jsx(
          fe,
          {
            rows: a,
            columns: [
              { title: "Topic", dataIndex: "topic", key: "topic", render: (b) => /* @__PURE__ */ r.jsx("code", { children: b }) },
              { title: "queueId", dataIndex: "queueId", key: "queueId", width: 110 },
              { title: "maxOffset", dataIndex: "maxOffset", key: "maxOffset", width: 140 }
            ],
            rowKey: "key",
            searchKeys: "topic,queueId",
            searchPlaceholder: "按 Topic / queueId 过滤",
            cardProps: { variant: "borderless", styles: { body: { padding: 0 } } }
          }
        ) : /* @__PURE__ */ r.jsx(
          se,
          {
            title: "offsetTable 是空的",
            description: "注意上面那条「上游语义」：位点表为空可能是真没有消息，也可能只是这条链路上还没产生过写入。"
          }
        )),
        t && !c && d && e.key === "subscription" && /* @__PURE__ */ r.jsxs("div", { style: { color: "#64748b", fontSize: 12 }, children: [
          "body 原文（注意它连 ",
          /* @__PURE__ */ r.jsx("code", { children: "subscription" }),
          " 字段都没有，是一个空的",
          /* @__PURE__ */ r.jsx("code", { children: "ConsumerOffsetSerializeWrapper" }),
          "）：",
          /* @__PURE__ */ r.jsx("pre", { style: { whiteSpace: "pre-wrap", marginTop: 8, fontSize: 12 }, children: JSON.stringify(d, null, 2) })
        ] }),
        t && !c && d && e.key === "dataVersion" && /* @__PURE__ */ r.jsx(
          H,
          {
            bordered: !0,
            size: "small",
            column: 1,
            items: Object.keys(d).map((b) => ({
              key: b,
              label: b,
              children: Ce(d[b])
            }))
          }
        ),
        t && !c && (d == null ? void 0 : d.dataVersion) && e.key !== "dataVersion" && /* @__PURE__ */ r.jsxs("div", { style: { marginTop: 10, color: "#64748b", fontSize: 12 }, children: [
          "dataVersion：",
          Ce(d.dataVersion)
        ] })
      ]
    }
  );
}
function fr() {
  return /* @__PURE__ */ r.jsxs(Ae, { children: [
    /* @__PURE__ */ r.jsx(ee, { index: !0, element: /* @__PURE__ */ r.jsx(qe, { to: "instance", replace: !0 }) }),
    /* @__PURE__ */ r.jsx(ee, { path: "instance", element: /* @__PURE__ */ r.jsx(We, {}) }),
    /* @__PURE__ */ r.jsx(ee, { path: "cluster", element: /* @__PURE__ */ r.jsx(He, {}) }),
    /* @__PURE__ */ r.jsx(ee, { path: "topics", element: /* @__PURE__ */ r.jsx(Xe, {}) }),
    /* @__PURE__ */ r.jsx(ee, { path: "broker", element: /* @__PURE__ */ r.jsx(rr, {}) })
  ] });
}
const nr = "# z-mq-component\n\nz-mq（自研 RocketMQ 风格消息中间件）跨仓唯一前端面：把「实例与端口 / 集群与 Broker / Topic 列表 /\nBroker 元数据」这一整套消息中间件管理页面收敛成一个可复用的 React 组件包。\n\n- **npm 包名**：`@yuku123/z-mq-component`\n- **宿主接入**：`@yuku123/z-mq-component/pages`（manifest + 页面 + API），主壳可只取 `ComponentDocPage`\n- **后端**：z-opc 侧 `MqProxyController` 的 `/api/mq/**`（**协议适配器**，不是字节透传代理）\n- **技术栈**：React 19 + antd 6 + react-router-dom 7 + vite（library mode，双入口 `.` / `./pages`）\n\n---\n\n## 一、能力概览\n\n路由前缀 `/z-mq`，菜单与路由在 `src/pages-manifest.jsx` 里声明。\n\n| 页面 | 路由（相对 `/z-mq/*`） | 源文件 | 能力 |\n|------|--------------------------|--------|------|\n| 登录 | `login` | `pages/LoginPage.jsx` | 登录页（suit 侧单独挂这条路由） |\n| 首页 | `home` | `pages/HomePage.jsx` | 入口卡片页 |\n| 实例与端口 | `instance` | `pages/Instance.jsx` | nameserver / broker 存活、bind 状态等（简版） |\n| Topic 清单 | `topics` | `pages/Topics.jsx` | Topic 列表（简版） |\n| 实例与端口（详版） | `*/instance` | `pages/InstanceStatus.jsx` | 自省接口 + 只读请求码实发探针 + 归属判定 |\n| 集群与 Broker | `*/cluster` | `pages/ClusterPage.jsx` | NameServer 码 4 的 brokerAddrTable / clusterAddrTable |\n| Topic 列表（详版） | `*/topics` | `pages/TopicListPage.jsx` | NameServer 码 8 与 Broker 码 8 的**差集** + 按需码 3 路由详情 |\n| Broker 元数据 | `*/broker` | `pages/BrokerMetadataPage.jsx` | BrokerOutAPI 码 320/321/322/323/324 五档 |\n| 组件文档 | `docs` | `pages/ComponentDoc.jsx` | 本文件渲染页（左侧 240px TOC + 右侧正文） |\n\n`src/pages/MqApp.jsx` 是子路由挂载点（`instance` / `cluster` / `topics` / `broker`），\n被 `routes` 里的 `/z-mq/:rest*` 承接，同时从包根具名导出（OpsWorkbench 以 `/mq/*` 挂它）。\n\n`src/pages/AttributionBanner.jsx` 是四张详版页共用的**归属提示条**：z-mq 是子进程型内嵌，\n页面必须先证明「应答的端口上是不是我自己 spawn 的那个进程」，再谈数据。\n\n## 二、导出面\n\n```js\n// 主入口（. 桶）\nimport {mqApi, mqErrorText, isMqSuccess, ZMQ_CODE, MqApp, ComponentDocPage} from '@yuku123/z-mq-component'\n\n// 宿主组装面（./pages 桶）\nimport {menuItems, routes, HomePage, LoginPage, Instance, Topics} from '@yuku123/z-mq-component/pages'\n```\n\n`./pages` 桶是**显式具名转发**（`export {menuItems, routes, Instance, Topics, HomePage, LoginPage} from './pages-manifest.jsx'`）。\n新增页面时必须回来补一行，否则消费方构建会报 `\"Xxx\" is not exported by dist/pages.js`。\n\n## 三、API 端点\n\n请求实例用基座默认桶里的 axios（`baseURL='/api'`），路径自带 `/api/mq` 前缀。\n下表是 `mqApi` 里**实际调用**的端点 —— `/api/mq/**` 是**穷举**的，\n没列出的路径一律 404 并回显可用清单。\n\n| 方法 | 路径 | 上游请求码 | 说明 |\n|---|---|---|---|\n| GET | `/api/mq/__instance` | —（代理自己应答） | 自省：不碰 NameServer 也不碰 Broker |\n| GET | `/api/mq/cluster` | 4 `GET_BROKER_CLUSTER_INFO` | 集群与 broker 地址表 |\n| GET | `/api/mq/topics` | 8 `GET_ALL_TOPIC_LIST` | NameServer 侧 topic 名清单（**只有名字**） |\n| GET | `/api/mq/route?topic=` | 3 `QUERY_TOPIC_ROUTE_INFO` | 单个 topic 的路由 / 队列详情 |\n| GET | `/api/mq/broker/topics?brokerName=` | 8 `GET_ALL_TOPIC_LIST` | Broker 侧 topicConfigTable 的 key 集 |\n| GET | `/api/mq/broker/topic-config?brokerName=` | 320 | topic 配置表 + dataVersion |\n| GET | `/api/mq/broker/consumer-offset?brokerName=` | 321 | 消费位点表 + dataVersion |\n| GET | `/api/mq/broker/delay-offset?brokerName=` | 322 | 延迟消息位点 + dataVersion |\n| GET | `/api/mq/broker/subscription?brokerName=` | 323 | 订阅信息（上游恒返回空 wrapper） |\n| GET | `/api/mq/broker/data-version?brokerName=` | 324 | topicConfig / consumerOffset / delayOffset 三份 DataVersion 的 map |\n\n### 3.1 响应形状\n\n成功：`{tier, target, requestCode, requestName, elapsedMs, responseCode, remark, data, attributionVerified}`。\n失败：HTTP 状态码与 `httpStatus` 同值（502 上游调不通 / 503 端口没人 / 403 拒绝非环回目标 /\n400 参数非法 / 404 端点不存在），body 为 `{error, hint, ...}`。\n\n⚠ 代理层把业务码字段叫 **`responseCode`** 而不是 `code` —— 因为共享的 `defaultUnwrap` 见到\n「同时有 code 和 data 两个键」会按 z-opc 的 `Result` 信封解包，而 z-mq 成功回的是 `code=0`，\n于是每个正常响应都会被 reject 成「请求失败」。\n\n`responseCode` 是 z-mq 的**业务码**不是 HTTP 码：`0=SUCCESS`、`1=SYSTEM_ERROR`、\n`2=SYSTEM_BUSY`、`3=REQUEST_CODE_NOT_SUPPORTED`、`4=TRANSACTION_FAILED`。\nHTTP 是 200 而 `responseCode=3` 是常见情况 —— 只看 HTTP 状态会把「这个码上游没实现」\n当成「集群是空的」。\n\n## 四、页面说明\n\n### 4.1 实例与端口（`InstanceStatus.jsx`）\n\n顶部用 `PageHeader` 标注「自省接口 `GET /api/mq/__instance`（由 MqProxyController 自己应答，\n不经过 z-mq）+ 只读请求码实发探针」。自省不通分三种成因分别提示：\n302 = 没带会话 Cookie；404 = `MainWebConfig.spaPaths` 或 `/api/mq` 映射没接上。\n\n### 4.2 集群与 Broker（`ClusterPage.jsx`）\n\nNameServer 码 4 的 `brokerAddrTable` / `clusterAddrTable`。这一档**不需要 broker 注册**就能应答；\n不通说明 `:namesrvPort` 上根本没有能答话的进程，或答话的那个跟本 JVM 不是一家的（见顶部红条）。\n\n### 4.3 Topic 列表（`TopicListPage.jsx`）\n\n**NameServer 码 8 与 Broker 码 8 的差集**（namesrv 取 `topicQueueTable.keySet()`，\nbroker 取 `topicConfigTable.keySet()`，两者天然可能不一致），再按需打码 3 拿路由详情。\n差值显式列出 —— 不许只画其中一边就宣称「这是全量 topic」。\n\n### 4.4 Broker 元数据（`BrokerMetadataPage.jsx`）\n\n五档（320 topic 配置 / 321 消费位点 / 322 延迟位点 / 323 订阅 / 324 dataVersion）。\n地址全靠码 4 发现且**只允许环回**；发现不到就是整页 503 —— 那是「集群里没有注册过的 broker」，\n不是这五档坏了。\n\n### 4.5 组件文档\n\n本文件。内容以 `?raw` 在编译期内联，零额外请求；左侧 240px TOC 随滚动联动。\n\n## 五、配置\n\n| 配置项 | 默认值 | 说明 |\n|---|---|---|\n| 请求 baseURL | `'/api'`（基座默认桶） | 路径自带 `/api/mq` 前缀 |\n| `ZMQ_CODE` | 0–4 的码表 | `SUCCESS` / `SYSTEM_ERROR` / `SYSTEM_BUSY` / `REQUEST_CODE_NOT_SUPPORTED` / `TRANSACTION_FAILED` |\n| `isMqSuccess(res)` | `res.responseCode === 0` | 判断业务成功（不是 HTTP 成功） |\n| `mqErrorText(e)` | — | 把三层错误来源都透出可读文案 |\n| `mqUpstreamErrorText(res)` | — | 成功 HTTP 但业务码非 0 时的文案（**不能当空表渲染**） |\n| `useInstance()` | — | 四张详版页共用的自省读面（`useCallback` 依赖取空，避免 effect 无限重跑） |\n\n## 六、已知限制\n\n- **只读面**：建删 topic（码 5/7/400）、KV 写（10/11）、重置位点（410）、终止消费者（411）、\n  发消息（200/201/202）**全部不可达** —— 代理层没有能传「码」的入口，页面上也没有任何\n  清空 / 删除 / 重启按钮。\n- **三格故意没做**（1.2.0 里没注册 processor，页面宁可不给这一格也不放假数据）：\n  消费者在线状态（`CONSUMER_HEARTBEAT` 221/222）、消息查询（230/231）、Broker 运行配置（402）。\n  `PULL_MESSAGE`(210) 虽注册，但会被长轮询挂住连接，读面也不碰。\n- **码 323（subscription）恒空**：上游直接返回了一个空的 `ConsumerOffsetSerializeWrapper`\n  （body 里根本没有 `subscription` 字样），结构也对不上 —— **绝对不许渲染成「集群里没有订阅组」**。\n- **码 322（delay-offset）与码 321 共用同一个 wrapper 类**，差别只有 dataVersion；\n  源码注释说「延迟消息 offset 与 Topic 自身 offset 共享」，所以这一格显示的不是独立的延迟队列统计。\n- **码 1 既表示「系统错误」也表示「topic 不存在」**（查不存在的 topic 时上游回\n  `responseCode=1` + remark `topic X not exist`），只能靠 remark 文本分开。\n- **三个常量别按 RocketMQ 的直觉解释**：\n  `perm` 是 `READ=1 / WRITE=2 / READ_WRITE=3 / INHERIT=0`（**不是** RocketMQ 的 4/2/6，\n  新建 topic 实测就是 3）；`queueDatas[].topicSysFlag` 恒 0（上游压根没 set，是「上游没填」）；\n  `brokerDatas[].cluster` 可能是 `null`（`BrokerData.cluster` 没被填），要显示成「上游没填」。\n- **地址只允许环回**：代理层按环回白名单拒外部地址 —— 这是正确行为，不是故障。\n- **页面空白有两种完全不同的成因**：内嵌子进程没 bind 上，或者 bind 上了但集群真的没 topic。\n  四张详版页顶部都会拉一次 `GET /api/mq/__instance` 把结论摊在数据上方。\n- z-mq 的内嵌方式是**子进程**（z-mq 的 Main 会 join 卡死 Tomcat），不能用「属主 == 8888 那个 pid」\n  当归属证明，归属判定口径见 `services/api.js` 的 `isUnattributed()`。\n- `Instance.jsx` / `Topics.jsx` 是简版页，`InstanceStatus.jsx` / `TopicListPage.jsx` 是详版页，\n  两套并存、路由不同（前者 `/z-mq/instance`、`/z-mq/topics`，后者 `/z-mq/*/instance`、`/z-mq/*/topics`）。\n- 组件包必须 external 基座；`vite.config.js` 的 `external` 是精确字符串匹配，\n  `@yuku123/z-frontend-common/ui` 与 `/ui/echarts` 两个子路径必须逐个列出，漏了基座会被复制进 dist。\n\n## 七、变更记录\n\n| 版本 | 变更 |\n|---|---|\n| 0.1.1 | 新增组件文档页 `ComponentDocPage`（lead 008 §7）：`src/DOC.md` + 基座 `ComponentDoc` 薄包装 |\n| 0.1.0 | 首版：实例 / 集群 / Topic / Broker 元数据四张详版页 + 归属提示条 + `mqApi` |\n";
function br() {
  return /* @__PURE__ */ r.jsx(ze, { markdown: nr, title: "组件文档 — z-mq" });
}
export {
  br as C,
  Le as K,
  fr as M,
  pr as T,
  ke as Z,
  mr as a,
  ur as b,
  U as c,
  Ce as d,
  K as e,
  dr as i,
  r as j,
  F as m,
  De as r
};
