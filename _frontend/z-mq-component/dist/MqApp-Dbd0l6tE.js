import Ie, { useState as S, useRef as Ae, useEffect as ne, useCallback as Y, useMemo as Re } from "react";
import { useNavigate as Be, Routes as $e, Route as ee, Navigate as ze } from "react-router-dom";
import { Alert as k, Breadcrumb as Me, Button as z, Card as I, Descriptions as Q, Tag as x, Space as q, Table as H, Row as xe, Col as A, Statistic as B, Select as qe } from "antd";
import { InboxOutlined as Le, ReloadOutlined as oe } from "@ant-design/icons";
import $ from "@yuku123/z-frontend-common";
const U = {
  /** 自省：由 MqProxyController 自己应答，不经过 z-mq，所以内嵌全挂时它也必须能通 */
  instance: () => $.get("/mq/__instance"),
  cluster: () => $.get("/mq/cluster"),
  topics: () => $.get("/mq/topics"),
  route: (r) => $.get("/mq/route", { params: { topic: r } }),
  brokerTopics: (r) => $.get("/mq/broker/topics", { params: r ? { brokerName: r } : {} }),
  brokerTopicConfig: (r) => $.get("/mq/broker/topic-config", { params: r ? { brokerName: r } : {} }),
  brokerConsumerOffset: (r) => $.get("/mq/broker/consumer-offset", { params: r ? { brokerName: r } : {} }),
  brokerDelayOffset: (r) => $.get("/mq/broker/delay-offset", { params: r ? { brokerName: r } : {} }),
  brokerSubscription: (r) => $.get("/mq/broker/subscription", { params: r ? { brokerName: r } : {} }),
  brokerDataVersion: (r) => $.get("/mq/broker/data-version", { params: r ? { brokerName: r } : {} })
}, be = {
  0: "SUCCESS",
  1: "SYSTEM_ERROR",
  2: "SYSTEM_BUSY",
  3: "REQUEST_CODE_NOT_SUPPORTED",
  4: "TRANSACTION_FAILED"
};
function dr(r) {
  return !!r && r.responseCode === 0;
}
function L(r) {
  var o;
  const t = (o = r == null ? void 0 : r.response) == null ? void 0 : o.data;
  if (t && typeof t == "object") {
    const a = t.message || t.error;
    if (a) return `${a}${t.hint ? ` — ${t.hint}` : ""}`;
    if (t.remark) return `z-mq 返回 responseCode=${t.responseCode} (${be[t.responseCode] || "?"}) ${t.remark}`;
  }
  return typeof t == "string" && t.trim() ? t.slice(0, 300) : (r == null ? void 0 : r.message) || String(r);
}
function K(r) {
  return !r || r.responseCode === 0 ? null : `上游返回 responseCode=${r.responseCode}（${be[r.responseCode] || "未知码"}）${r.remark ? `：${r.remark}` : ""}` + (r.responseCode === 3 ? ' —— 这个请求码在 z-mq 当前版本没注册，是"上游没实现"，不是"集群里没有"' : "");
}
function ur(r) {
  return !!(r && (r.attributionUnverified || r.attributionVerified === !1));
}
function mr(r) {
  var o;
  const t = (o = r == null ? void 0 : r.response) == null ? void 0 : o.status;
  return t === 503 || t === 502 ? !0 : /没有任何监听者|不是本 JVM|Address already in use|not bound|Connect/.test(String(L(r)));
}
const De = {
  "broker/subscription": "BrokerOutAPI.handleGetAllSubscriptionGroup 的源码注释写着「MVP: 订阅组配置暂未单独存储, 返回空 wrapper + 默认版本号」，而且它实际 new 出来的是一个空的 ConsumerOffsetSerializeWrapper（body 里连 subscription 字段都没有）⇒ 这张表恒空，不代表集群里没有订阅组",
  "broker/delay-offset": "源码注释「MVP: 延迟消息 offset 与 Topic 自身 offset 共享 (无 SCHEDULE_TOPIC_XXXX 隔离队列)。返回与 GET_ALL_CONSUMER_OFFSET 一致即可」⇒ 这一格的 offsetTable 和 321 那份是同一份数据，只有 dataVersion 不同",
  "broker/consumer-offset": 'collectOffsetTable 的注释「简化: 遍历可能的 queueId (MVP 用 0-15)」且取的是各 group 的 max ⇒ 显示的是"该 topic 前 16 个队列里出现过的最大 offset"，不是某个消费组的真实位点'
};
function Ve(r) {
  if (!r || r.responseCode === 0) return null;
  const t = String(r.remark || "");
  return /not exist/i.test(t) ? t : null;
}
function Ee(r) {
  if (!r || typeof r != "object") return "—";
  const t = r.counter ?? r.counterValue, o = r.timestamp ? new Date(r.timestamp).toLocaleString("zh-CN", { hour12: !1 }) : "-";
  return `counter=${t ?? "?"} · timestamp=${o}`;
}
const pr = /^[A-Za-z0-9_\-.:;%#]{1,120}$/;
var le = { exports: {} }, re = {};
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
  var r = Symbol.for("react.transitional.element"), t = Symbol.for("react.fragment");
  function o(a, s, u) {
    var d = null;
    if (u !== void 0 && (d = "" + u), s.key !== void 0 && (d = "" + s.key), "key" in s) {
      u = {};
      for (var l in s)
        l !== "key" && (u[l] = s[l]);
    } else u = s;
    return s = u.ref, {
      $$typeof: r,
      type: a,
      key: d,
      ref: s !== void 0 ? s : null,
      props: u
    };
  }
  return re.Fragment = t, re.jsx = o, re.jsxs = o, re;
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
var Ce;
function Fe() {
  return Ce || (Ce = 1, process.env.NODE_ENV !== "production" && (function() {
    function r(n) {
      if (n == null) return null;
      if (typeof n == "function")
        return n.$$typeof === Z ? null : n.displayName || n.name || null;
      if (typeof n == "string") return n;
      switch (n) {
        case i:
          return "Fragment";
        case b:
          return "Profiler";
        case c:
          return "StrictMode";
        case F:
          return "Suspense";
        case W:
          return "SuspenseList";
        case y:
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
          case C:
            return n.displayName || "Context";
          case j:
            return (n._context.displayName || "Context") + ".Consumer";
          case P:
            var m = n.render;
            return n = n.displayName, n || (n = m.displayName || m.name || "", n = n !== "" ? "ForwardRef(" + n + ")" : "ForwardRef"), n;
          case ie:
            return m = n.displayName || null, m !== null ? m : r(n.type) || "Memo";
          case J:
            m = n._payload, n = n._init;
            try {
              return r(n(m));
            } catch {
            }
        }
      return null;
    }
    function t(n) {
      return "" + n;
    }
    function o(n) {
      try {
        t(n);
        var m = !1;
      } catch {
        m = !0;
      }
      if (m) {
        m = console;
        var g = m.error, T = typeof Symbol == "function" && Symbol.toStringTag && n[Symbol.toStringTag] || n.constructor.name || "Object";
        return g.call(
          m,
          "The provided key is an unsupported type %s. This value must be coerced to a string before using it here.",
          T
        ), t(n);
      }
    }
    function a(n) {
      if (n === i) return "<>";
      if (typeof n == "object" && n !== null && n.$$typeof === J)
        return "<...>";
      try {
        var m = r(n);
        return m ? "<" + m + ">" : "<...>";
      } catch {
        return "<...>";
      }
    }
    function s() {
      var n = me.A;
      return n === null ? null : n.getOwner();
    }
    function u() {
      return Error("react-stack-top-frame");
    }
    function d(n) {
      if (ye.call(n, "key")) {
        var m = Object.getOwnPropertyDescriptor(n, "key").get;
        if (m && m.isReactWarning) return !1;
      }
      return n.key !== void 0;
    }
    function l(n, m) {
      function g() {
        ke || (ke = !0, console.error(
          "%s: `key` is not a prop. Trying to access it will result in `undefined` being returned. If you need to access the same value within the child component, you should pass it as a different prop. (https://react.dev/link/special-props)",
          m
        ));
      }
      g.isReactWarning = !0, Object.defineProperty(n, "key", {
        get: g,
        configurable: !0
      });
    }
    function f() {
      var n = r(this.type);
      return ge[n] || (ge[n] = !0, console.error(
        "Accessing element.ref was removed in React 19. ref is now a regular prop. It will be removed from the JSX Element type in a future release."
      )), n = this.props.ref, n !== void 0 ? n : null;
    }
    function v(n, m, g, T, V, M) {
      var E = g.ref;
      return n = {
        $$typeof: h,
        type: n,
        key: m,
        props: g,
        _owner: T
      }, (E !== void 0 ? E : null) !== null ? Object.defineProperty(n, "ref", {
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
        value: M
      }), Object.freeze && (Object.freeze(n.props), Object.freeze(n)), n;
    }
    function w(n, m, g, T, V, M) {
      var E = m.children;
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
      if (ye.call(m, "key")) {
        E = r(n);
        var G = Object.keys(m).filter(function(Pe) {
          return Pe !== "key";
        });
        T = 0 < G.length ? "{key: someKey, " + G.join(": ..., ") + ": ...}" : "{key: someKey}", Te[E + T] || (G = 0 < G.length ? "{" + G.join(": ..., ") + ": ...}" : "{}", console.error(
          `A props object containing a "key" prop is being spread into JSX:
  let props = %s;
  <%s {...props} />
React keys must be passed directly to JSX without using spread:
  let props = %s;
  <%s key={someKey} {...props} />`,
          T,
          E,
          G,
          E
        ), Te[E + T] = !0);
      }
      if (E = null, g !== void 0 && (o(g), E = "" + g), d(m) && (o(m.key), E = "" + m.key), "key" in m) {
        g = {};
        for (var fe in m)
          fe !== "key" && (g[fe] = m[fe]);
      } else g = m;
      return E && l(
        g,
        typeof n == "function" ? n.displayName || n.name || "Unknown" : n
      ), v(
        n,
        E,
        g,
        s(),
        V,
        M
      );
    }
    function O(n) {
      _(n) ? n._store && (n._store.validated = 1) : typeof n == "object" && n !== null && n.$$typeof === J && (n._payload.status === "fulfilled" ? _(n._payload.value) && n._payload.value._store && (n._payload.value._store.validated = 1) : n._store && (n._store.validated = 1));
    }
    function _(n) {
      return typeof n == "object" && n !== null && n.$$typeof === h;
    }
    var p = Ie, h = Symbol.for("react.transitional.element"), D = Symbol.for("react.portal"), i = Symbol.for("react.fragment"), c = Symbol.for("react.strict_mode"), b = Symbol.for("react.profiler"), j = Symbol.for("react.consumer"), C = Symbol.for("react.context"), P = Symbol.for("react.forward_ref"), F = Symbol.for("react.suspense"), W = Symbol.for("react.suspense_list"), ie = Symbol.for("react.memo"), J = Symbol.for("react.lazy"), y = Symbol.for("react.activity"), R = Symbol.for("react.view_transition"), Z = Symbol.for("react.client.reference"), me = p.__CLIENT_INTERNALS_DO_NOT_USE_OR_WARN_USERS_THEY_CANNOT_UPGRADE, ye = Object.prototype.hasOwnProperty, Ne = Array.isArray, pe = console.createTask ? console.createTask : function() {
      return null;
    };
    p = {
      react_stack_bottom_frame: function(n) {
        return n();
      }
    };
    var ke, ge = {}, ve = p.react_stack_bottom_frame.bind(
      p,
      u
    )(), Se = pe(a(u)), Te = {};
    te.Fragment = i, te.jsx = function(n, m, g) {
      var T = 1e4 > me.recentlyCreatedOwnerStacks++;
      if (T) {
        var V = Error.stackTraceLimit;
        Error.stackTraceLimit = 10;
        var M = Error("react-stack-top-frame");
        Error.stackTraceLimit = V;
      } else M = ve;
      return w(
        n,
        m,
        g,
        !1,
        M,
        T ? pe(a(n)) : Se
      );
    }, te.jsxs = function(n, m, g) {
      var T = 1e4 > me.recentlyCreatedOwnerStacks++;
      if (T) {
        var V = Error.stackTraceLimit;
        Error.stackTraceLimit = 10;
        var M = Error("react-stack-top-frame");
        Error.stackTraceLimit = V;
      } else M = ve;
      return w(
        n,
        m,
        g,
        !0,
        M,
        T ? pe(a(n)) : Se
      );
    };
  })()), te;
}
var _e;
function Ye() {
  return _e || (_e = 1, process.env.NODE_ENV === "production" ? le.exports = Ue() : le.exports = Fe()), le.exports;
}
var e = Ye();
function ae() {
  const [r, t] = S(null), [o, a] = S(!1), [s, u] = S(""), d = Ae(!0);
  ne(() => () => {
    d.current = !1;
  }, []);
  const l = Y(async () => {
    try {
      const f = await U.instance();
      if (!d.current) return;
      t(f), a(!1), u("");
    } catch (f) {
      if (!d.current) return;
      t(null), a(!0), u(L(f));
    }
  }, []);
  return { instance: r, instanceError: o, instanceErrorText: s, poll: l };
}
function ce({ data: r }) {
  if (!r) return null;
  const { namesrvForeignListenerSuspected: t, brokerForeignListenerSuspected: o, namesrvPort: a, brokerPort: s } = r;
  if (!t && !o) return null;
  const u = [
    t ? `NameServer :${a}（属主 PID ${r.namesrvPortOwnerPid}，我的子进程 PID ${r.namesrvChildPid}）` : null,
    o ? `Broker :${s}（属主 PID ${r.brokerPortOwnerPid}，我的子进程 PID ${r.brokerChildPid}）` : null
  ].filter(Boolean).join("；");
  return /* @__PURE__ */ e.jsx(
    k,
    {
      type: "error",
      showIcon: !0,
      style: { marginBottom: 16 },
      message: "有端口在应答，但应答者不是本次构建 spawn 的子进程 —— 下面所有表格都不算孵化结果",
      description: `外来嫌疑：${u}。` + (r.bindError ? ` 本 JVM 的子进程 bind 失败原文：${r.bindError}` : " 本 JVM 的子进程没有 bind 成功的记录。") + // 端口取实例自省回来的实际值：写死 9876/10911 会在换端口后把人指到别的进程上
      ` 处理方式只有两种：让出 ${a}/${s}，或给 zmq.namesrv-port / zmq.broker-port 换端口。 代理层不会替你去占端口，也不会去 kill 别人的进程。`
    }
  );
}
function X({ res: r }) {
  return !r || !(r.attributionUnverified || r.attributionVerified === !1) ? null : /* @__PURE__ */ e.jsx(
    k,
    {
      type: "warning",
      showIcon: !0,
      style: { marginBottom: 12 },
      message: `本响应来自 ${r.target || "?"}，但归属未通过（应答者不是本 JVM 的子进程）`,
      description: r.attributionNote
    }
  );
}
function de({ data: r }) {
  if (!r) return null;
  const t = [
    r.namesrvChildAlive && !r.namesrvPortOwnedByMyChild ? `NameServer pid=${r.namesrvChildPid}` : null,
    r.brokerChildAlive && !r.brokerPortOwnedByMyChild ? `Broker pid=${r.brokerChildPid}` : null
  ].filter(Boolean);
  return t.length ? /* @__PURE__ */ e.jsx(
    k,
    {
      type: "error",
      showIcon: !0,
      style: { marginBottom: 16 },
      message: `子进程 isAlive()=true 但端口不归它：${t.join("、")} —— 「进程在」不等于「服务在」`,
      description: 'z-mq 的 Main 会 Thread.currentThread().join()，所以 ZMqEmbeddedServerConfig 是 spawn 子进程而不是线程内嵌。bind 失败时 main 线程已死、Netty 非守护线程吊着 JVM 不退，于是 isAlive() 一路报"好"。这就是 __instance 把 *ChildAlive 和 *PortOwnedByMyChild 拆成两格的原因。'
    }
  ) : null;
}
function je({ data: r }) {
  return !r || r.attributionAvailable !== !1 ? null : /* @__PURE__ */ e.jsx(
    k,
    {
      type: "warning",
      showIcon: !0,
      style: { marginBottom: 16 },
      message: "归属无法自证（lsof 探针不可用或已关闭）",
      description: r.attributionMethod + "。此时页面不猜：请人工执行 lsof -nP -iTCP:" + r.namesrvPort + " -sTCP:LISTEN 与本页 jvm/子进程 pid 对照。"
    }
  );
}
const he = {
  // 卡片表面
  surfaceAlt: "#fafbfc",
  text: "#111827",
  // 主文本
  textMuted: "#6b7280"
}, We = {
  md: 12
};
function se({
  icon: r,
  title: t = "暂无数据",
  description: o,
  actionText: a,
  onAction: s,
  size: u = "md",
  style: d
}) {
  const l = { sm: 24, md: 40, lg: 64 }, f = { sm: 14, md: 16, lg: 20 };
  return /* @__PURE__ */ e.jsxs(
    "div",
    {
      role: "status",
      "aria-live": "polite",
      style: {
        padding: l[u] || l.md,
        textAlign: "center",
        background: "#ffffff",
        borderRadius: We.md,
        border: "1px dashed #e5e7eb",
        ...d
      },
      children: [
        /* @__PURE__ */ e.jsx("div", { style: {
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
          width: 56,
          height: 56,
          borderRadius: "50%",
          background: he.surfaceAlt,
          marginBottom: 16,
          fontSize: 28,
          color: "#9ca3af"
        }, children: r || /* @__PURE__ */ e.jsx(Le, {}) }),
        /* @__PURE__ */ e.jsx("div", { style: {
          fontSize: f[u],
          fontWeight: 600,
          color: he.text,
          marginBottom: o ? 6 : 16
        }, children: t }),
        o && /* @__PURE__ */ e.jsx("div", { style: {
          fontSize: 13,
          color: he.textMuted,
          lineHeight: 1.6,
          maxWidth: 360,
          margin: "0 auto 16px"
        }, children: o }),
        a && s && /* @__PURE__ */ e.jsx(
          z,
          {
            type: "primary",
            onClick: s,
            style: {
              background: "linear-gradient(135deg, #7c3aed 0%, #6d28d9 100%)",
              border: "none",
              minHeight: 36
            },
            children: a
          }
        )
      ]
    }
  );
}
function ue({
  title: r,
  subtitle: t,
  breadcrumb: o,
  extra: a,
  back: s,
  onBack: u
}) {
  const d = Be();
  return /* @__PURE__ */ e.jsx("div", { style: {
    marginBottom: 12,
    paddingBottom: 10,
    borderBottom: "1px solid #f1f5f9"
  }, children: /* @__PURE__ */ e.jsxs("div", { style: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 12,
    flexWrap: "wrap"
  }, children: [
    /* @__PURE__ */ e.jsxs("div", { style: {
      display: "flex",
      alignItems: "center",
      gap: 10,
      minWidth: 0,
      flexWrap: "wrap"
    }, children: [
      s && /* @__PURE__ */ e.jsx(
        "button",
        {
          onClick: () => {
            u ? u() : d(-1);
          },
          "aria-label": "返回上一页",
          style: {
            width: 28,
            height: 28,
            borderRadius: 6,
            border: "1px solid #e5e7eb",
            background: "#ffffff",
            color: "#6b7280",
            cursor: "pointer",
            display: "inline-flex",
            alignItems: "center",
            justifyContent: "center",
            fontSize: 13,
            transition: "all 150ms ease",
            flexShrink: 0
          },
          onMouseEnter: (l) => {
            l.currentTarget.style.background = "#f8fafc", l.currentTarget.style.color = "#111827", l.currentTarget.style.borderColor = "#7c3aed";
          },
          onMouseLeave: (l) => {
            l.currentTarget.style.background = "#ffffff", l.currentTarget.style.color = "#6b7280", l.currentTarget.style.borderColor = "#e5e7eb";
          },
          children: "←"
        }
      ),
      o && o.length > 0 ? /* @__PURE__ */ e.jsx(
        Me,
        {
          items: o.map((l, f) => {
            const v = f === o.length - 1;
            return {
              title: l.href && !v ? /* @__PURE__ */ e.jsx("a", { onClick: (w) => {
                w.preventDefault(), d(l.href);
              }, children: l.label }) : /* @__PURE__ */ e.jsx("span", { style: v ? {
                fontWeight: 600,
                color: "#111827"
              } : void 0, children: l.label })
            };
          })
        }
      ) : r && /* @__PURE__ */ e.jsx("span", { role: "heading", "aria-level": 1, style: {
        fontSize: 15,
        fontWeight: 600,
        color: "#111827",
        lineHeight: 1.4
      }, children: r }),
      t && /* @__PURE__ */ e.jsx("span", { title: t, style: {
        fontSize: 12,
        color: "#94a3b8",
        lineHeight: 1.4,
        whiteSpace: "nowrap",
        overflow: "hidden",
        textOverflow: "ellipsis",
        maxWidth: 360
      }, children: t })
    ] }),
    a && /* @__PURE__ */ e.jsx("div", { style: {
      display: "flex",
      gap: 8,
      flexShrink: 0
    }, children: a })
  ] }) });
}
function Je(r) {
  if (!r) return "-";
  try {
    return new Date(r).toLocaleString("zh-CN", { hour12: !1 });
  } catch {
    return String(r);
  }
}
function Ge(r) {
  if (r == null) return "-";
  const t = Math.max(0, Math.floor((Date.now() - r) / 1e3));
  return t < 60 ? `${t} 秒前` : t < 3600 ? `${Math.floor(t / 60)} 分前` : t < 86400 ? `${Math.floor(t / 3600)} 时前` : `${Math.floor(t / 86400)} 天前`;
}
function N(r, t = "是", o = "否", a = "error") {
  return r == null ? /* @__PURE__ */ e.jsx(x, { children: "未知" }) : r ? /* @__PURE__ */ e.jsx(x, { color: "success", children: t }) : /* @__PURE__ */ e.jsx(x, { color: a, children: o });
}
const Qe = [
  { title: "逻辑端点", dataIndex: "ep", key: "ep", width: 240, render: (r) => /* @__PURE__ */ e.jsx("code", { children: r }) },
  { title: "请求码", dataIndex: "code", key: "code", width: 100, render: (r) => /* @__PURE__ */ e.jsx(x, { children: r }) },
  { title: "常量名", dataIndex: "name", key: "name", render: (r) => /* @__PURE__ */ e.jsx("code", { children: r }) }
];
function He() {
  var _;
  const { instance: r, instanceError: t, instanceErrorText: o, poll: a } = ae(), [s, u] = S(null), [d, l] = S(null), [f, v] = S(!1), w = Y(async (p) => {
    v(!0);
    try {
      const h = await U[p]();
      u({ which: p, res: h }), l(null);
    } catch (h) {
      u(null), l(h);
    } finally {
      v(!1), a();
    }
  }, [a]), O = r ? Object.entries(r.readOnlyRequestCodes || {}).map(([p, h]) => ({
    ep: p,
    code: h,
    name: (r.requestCodeNames || {})[String(h)] || "-",
    key: p
  })) : [];
  return /* @__PURE__ */ e.jsxs("div", { children: [
    /* @__PURE__ */ e.jsx(
      ue,
      {
        title: "实例与端口",
        subtitle: "自省接口 GET /api/mq/__instance（由 MqProxyController 自己应答，不经过 z-mq）+ 只读请求码实发探针"
      }
    ),
    /* @__PURE__ */ e.jsx(I, { extra: /* @__PURE__ */ e.jsxs(q, { children: [
      /* @__PURE__ */ e.jsx(z, { icon: /* @__PURE__ */ e.jsx(oe, {}), loading: f, onClick: () => w("cluster"), children: "实发一发 码4 GET_BROKER_CLUSTER_INFO" }),
      /* @__PURE__ */ e.jsx(z, { icon: /* @__PURE__ */ e.jsx(oe, {}), onClick: a, children: "刷新自省" })
    ] }), children: t ? /* @__PURE__ */ e.jsx(
      k,
      {
        type: "error",
        showIcon: !0,
        message: `自省接口调用失败：${o}`,
        description: "/api/mq/__instance 不碰 NameServer 也不碰 Broker，由 z-opc 自己的 controller 直接应答；它都不通说明是 z-opc 侧的路由/认证问题（302 到登录页 = 没带会话 Cookie；404 = MainWebConfig.spaPaths 或 /api/mq 映射没接上）。"
      }
    ) : /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
      /* @__PURE__ */ e.jsx(je, { data: r }),
      /* @__PURE__ */ e.jsx(ce, { data: r }),
      /* @__PURE__ */ e.jsx(de, { data: r }),
      r && !r.embeddedConfigLoaded && /* @__PURE__ */ e.jsx(
        k,
        {
          type: "warning",
          showIcon: !0,
          style: { marginBottom: 16 },
          message: "ZMqEmbeddedServerConfig 未装载",
          description: "zmq.enabled 不为 true 时这个 @Configuration 整个不生效 ⇒ 不会有任何子进程被 spawn，页面所有数据必然拿不到。"
        }
      ),
      /* @__PURE__ */ e.jsx(
        Q,
        {
          bordered: !0,
          size: "small",
          column: 2,
          items: [
            {
              key: "jvm",
              label: "本 JVM (pid@host)",
              children: /* @__PURE__ */ e.jsx("code", { children: (r == null ? void 0 : r.jvm) ?? "-" })
            },
            {
              key: "surface",
              label: "z-mq 的管理面形状",
              children: /* @__PURE__ */ e.jsx(x, { color: "volcano", children: (r == null ? void 0 : r.surface) ?? "-" })
            },
            {
              key: "loaded",
              label: "内嵌配置已装载",
              children: r ? r.embeddedConfigLoaded ? /* @__PURE__ */ e.jsx(x, { color: "success", children: "是" }) : /* @__PURE__ */ e.jsx(x, { color: "error", children: "否（zmq.enabled≠true）" }) : "-"
            },
            {
              key: "addr",
              label: "NameServer 地址 / Broker 端口",
              children: /* @__PURE__ */ e.jsx("code", { children: r ? `${r.namesrvAddr} / ${r.brokerPort}` : "-" })
            },
            {
              key: "nsChild",
              label: "NameServer 子进程已 spawn",
              children: N(r == null ? void 0 : r.namesrvChildSpawned, "是", "否（未 spawn 或 boot jar 未找到）")
            },
            {
              key: "nsPid",
              label: "NameServer 子进程 pid / 存活",
              children: r ? /* @__PURE__ */ e.jsxs("span", { children: [
                /* @__PURE__ */ e.jsx("code", { children: r.namesrvChildPid ?? "n/a" }),
                " ",
                N(r.namesrvChildAlive, "alive", "dead")
              ] }) : "-"
            },
            {
              key: "nsOwner",
              label: "NameServer 端口属主 pid（lsof）",
              children: /* @__PURE__ */ e.jsx("code", { children: (r == null ? void 0 : r.namesrvPortOwnerPid) ?? "取不到" })
            },
            {
              key: "nsOwned",
              label: "NameServer 端口归我的子进程",
              children: N(r == null ? void 0 : r.namesrvPortOwnedByMyChild, "是（这才是孵化成功）", "否")
            },
            {
              key: "brChild",
              label: "Broker 子进程已 spawn",
              children: N(r == null ? void 0 : r.brokerChildSpawned, "是", "否")
            },
            {
              key: "brPid",
              label: "Broker 子进程 pid / 存活",
              children: r ? /* @__PURE__ */ e.jsxs("span", { children: [
                /* @__PURE__ */ e.jsx("code", { children: r.brokerChildPid ?? "n/a" }),
                " ",
                N(r.brokerChildAlive, "alive", "dead")
              ] }) : "-"
            },
            {
              key: "brOwner",
              label: "Broker 端口属主 pid（lsof）",
              children: /* @__PURE__ */ e.jsx("code", { children: (r == null ? void 0 : r.brokerPortOwnerPid) ?? "取不到" })
            },
            {
              key: "brOwned",
              label: "Broker 端口归我的子进程",
              children: N(r == null ? void 0 : r.brokerPortOwnedByMyChild, "是", "否")
            },
            {
              key: "reach",
              label: "两个端口此刻 TCP 可连",
              children: r ? /* @__PURE__ */ e.jsxs(q, { children: [
                N(r.namesrvPortReachable, `:${r.namesrvPort} 通`, `:${r.namesrvPort} 不通`, "warning"),
                N(r.brokerPortReachable, `:${r.brokerPort} 通`, `:${r.brokerPort} 不通`, "warning")
              ] }) : "-"
            },
            {
              key: "store",
              label: "store 目录（存在 / 是否为空）",
              children: r ? /* @__PURE__ */ e.jsxs("span", { children: [
                /* @__PURE__ */ e.jsx("code", { children: r.storePath }),
                " ",
                N(r.storePathExists, "目录在", "目录不存在", "warning"),
                " ",
                r.storePathExists && N(!r.storePathEmpty, "有内容", "空 = 这个实例没落过任何消息", "warning")
              ] }) : "-"
            }
          ]
        }
      ),
      (r == null ? void 0 : r.surfaceNote) && /* @__PURE__ */ e.jsx("div", { style: { marginTop: 12, color: "#64748b", fontSize: 12 }, children: r.surfaceNote })
    ] }) }),
    /* @__PURE__ */ e.jsx(
      I,
      {
        title: "子进程 bind 失败原文（spawn 器落的日志）",
        style: { marginTop: 16 },
        extra: /* @__PURE__ */ e.jsx(x, { color: "blue", children: "ZMqEmbeddedServerConfig 把子进程 stdout/stderr 追加到这里" }),
        children: r ? /* @__PURE__ */ e.jsx(
          Q,
          {
            bordered: !0,
            size: "small",
            column: 1,
            items: ["namesrvSpawnLog", "brokerSpawnLog"].map((p) => {
              const h = r[p] || {};
              return {
                key: p,
                label: p === "namesrvSpawnLog" ? "NameServer" : "Broker",
                children: /* @__PURE__ */ e.jsxs("div", { style: { fontSize: 12 }, children: [
                  /* @__PURE__ */ e.jsxs("div", { children: [
                    /* @__PURE__ */ e.jsx("code", { children: h.path }),
                    " ",
                    N(h.exists, "存在", "不存在", "warning"),
                    " ",
                    h.exists ? /* @__PURE__ */ e.jsxs("span", { style: { color: "#94a3b8" }, children: [
                      "（",
                      Ge(h.lastModified),
                      "写入）"
                    ] }) : null
                  ] }),
                  h.bindErrorLines ? /* @__PURE__ */ e.jsxs("div", { style: { color: "#dc2626" }, children: [
                    "命中 ",
                    h.bindErrorLines,
                    " 行 bind 失败，最后一条：",
                    /* @__PURE__ */ e.jsx("code", { children: h.lastBindError })
                  ] }) : /* @__PURE__ */ e.jsx("div", { style: { color: "#94a3b8" }, children: "日志里没有 bind 失败记录" })
                ] })
              };
            })
          }
        ) : /* @__PURE__ */ e.jsx(k, { type: "info", showIcon: !0, message: "自省接口没通，读不到日志字段" })
      }
    ),
    /* @__PURE__ */ e.jsx(I, { title: "实发一发只读请求码（证明协议适配器本身成立，而不只是在报自省）", style: { marginTop: 16 }, children: /* @__PURE__ */ e.jsxs(q, { direction: "vertical", style: { width: "100%" }, children: [
      /* @__PURE__ */ e.jsxs(q, { wrap: !0, children: [
        /* @__PURE__ */ e.jsx(z, { size: "small", loading: f, onClick: () => w("cluster"), children: "码 4 · 集群注册表" }),
        /* @__PURE__ */ e.jsx(z, { size: "small", loading: f, onClick: () => w("topics"), children: "码 8 · NameServer topic 列表" }),
        /* @__PURE__ */ e.jsx(z, { size: "small", loading: f, onClick: () => w("brokerTopics"), children: "Broker 码 8 · 本 broker topic" }),
        /* @__PURE__ */ e.jsx(z, { size: "small", loading: f, onClick: () => w("brokerDataVersion"), children: "Broker 码 324 · DataVersion" })
      ] }),
      d && /* @__PURE__ */ e.jsx(
        k,
        {
          type: "error",
          showIcon: !0,
          message: `探针失败：${L(d)}`
        }
      ),
      s && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
        /* @__PURE__ */ e.jsx(X, { res: s.res }),
        /* @__PURE__ */ e.jsx(
          Q,
          {
            bordered: !0,
            size: "small",
            column: 3,
            items: [
              { key: "t", label: "打的哪一档", children: /* @__PURE__ */ e.jsx(x, { children: s.res.tier }) },
              { key: "target", label: "目标地址", children: /* @__PURE__ */ e.jsx("code", { children: s.res.target }) },
              { key: "req", label: "请求码", children: /* @__PURE__ */ e.jsxs("code", { children: [
                s.res.requestCode,
                " ",
                s.res.requestName
              ] }) },
              {
                key: "responseCode",
                label: "z-mq 业务码",
                children: /* @__PURE__ */ e.jsxs(x, { color: s.res.responseCode === 0 ? "success" : "error", children: [
                  s.res.responseCode,
                  " · ",
                  be[s.res.responseCode] || "未知码"
                ] })
              },
              { key: "remark", label: "remark", children: s.res.remark || /* @__PURE__ */ e.jsx("span", { style: { color: "#94a3b8" }, children: "—" }) },
              { key: "elapsed", label: "耗时", children: `${s.res.elapsedMs ?? "-"} ms` },
              { key: "attr", label: "归属已验证", children: N(s.res.attributionVerified) },
              { key: "size", label: "响应体字节数", children: JSON.stringify(s.res.data ?? null).length },
              {
                key: "ts",
                label: "本页数据读取时刻",
                children: Je(Date.now())
              }
            ]
          }
        ),
        /* @__PURE__ */ e.jsx("pre", { style: {
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
      !s && !d && /* @__PURE__ */ e.jsx(
        k,
        {
          type: "info",
          showIcon: !0,
          message: "还没发探针",
          description: "自省接口证明的是「z-opc 这一层活着、且知道自己没起」；发一发只读码证明的是「协议适配这条形状走得通」。两个都要。"
        }
      )
    ] }) }),
    /* @__PURE__ */ e.jsxs(I, { title: "本代理可达的只读请求码（穷举，无通用转发口）", style: { marginTop: 16 }, children: [
      /* @__PURE__ */ e.jsx(
        H,
        {
          size: "small",
          rowKey: "ep",
          dataSource: O,
          columns: Qe,
          pagination: !1
        }
      ),
      ((_ = r == null ? void 0 : r.unavailableOnBroker120) == null ? void 0 : _.length) > 0 && /* @__PURE__ */ e.jsx(
        k,
        {
          type: "warning",
          showIcon: !0,
          style: { marginTop: 12 },
          message: "以下能力在 z-mq 1.2.0 上根本没有请求码，页面不做也不假做",
          description: /* @__PURE__ */ e.jsxs("div", { children: [
            /* @__PURE__ */ e.jsx("div", { style: { fontWeight: 600, marginTop: 4 }, children: "Broker 侧未注册：" }),
            /* @__PURE__ */ e.jsx("ul", { style: { margin: "4px 0 0 18px" }, children: r.unavailableOnBroker120.map((p) => /* @__PURE__ */ e.jsx("li", { children: p }, p)) }),
            /* @__PURE__ */ e.jsx("div", { style: { fontWeight: 600, marginTop: 4 }, children: "NameServer 侧未实现：" }),
            /* @__PURE__ */ e.jsx("ul", { style: { margin: "4px 0 0 18px" }, children: (r.unavailableOnNamesrv120 || []).map((p) => /* @__PURE__ */ e.jsx("li", { children: p }, p)) })
          ] })
        }
      )
    ] })
  ] });
}
const Ke = [
  {
    title: "Broker 名",
    dataIndex: "brokerName",
    key: "brokerName",
    width: 180,
    render: (r) => /* @__PURE__ */ e.jsx("code", { children: r })
  },
  { title: "Cluster", dataIndex: "cluster", key: "cluster", width: 160, render: (r) => r || /* @__PURE__ */ e.jsx(x, { children: "未上报" }) },
  {
    title: "Master 地址 (brokerId=0)",
    dataIndex: "masterAddr",
    key: "masterAddr",
    width: 220,
    render: (r) => /* @__PURE__ */ e.jsx("code", { children: r || "-" })
  },
  {
    title: "全部副本 (id → addr)",
    dataIndex: "replicas",
    key: "replicas",
    render: (r) => r.length ? /* @__PURE__ */ e.jsx(q, { direction: "vertical", size: 0, children: r.map((t) => /* @__PURE__ */ e.jsxs("span", { children: [
      /* @__PURE__ */ e.jsxs(x, { color: t.id === "0" ? "blue" : "default", children: [
        "id=",
        t.id
      ] }),
      /* @__PURE__ */ e.jsx("code", { children: t.addr })
    ] }, t.id)) }) : /* @__PURE__ */ e.jsx("span", { style: { color: "#94a3b8" }, children: "—" })
  },
  {
    title: "端口是否等于本 JVM 配置的 broker 端口",
    key: "portMatch",
    width: 240,
    render: (r, t) => t.port ? t.portMatched === null ? /* @__PURE__ */ e.jsx(x, { color: "warning", children: "配置端口未知" }) : t.portMatched ? /* @__PURE__ */ e.jsxs(x, { color: "success", children: [
      "一致（",
      t.port,
      "）"
    ] }) : /* @__PURE__ */ e.jsxs(x, { color: "error", children: [
      "不一致：注册的是 ",
      t.port,
      "，本 JVM 配的是 ",
      t.expected
    ] }) : /* @__PURE__ */ e.jsx(x, { children: "无地址" })
  }
];
function Xe() {
  const { instance: r, poll: t } = ae(), [o, a] = S(null), [s, u] = S(null), [d, l] = S(!1), f = Y(async () => {
    l(!0);
    try {
      const i = await U.cluster();
      a(i), u(null);
    } catch (i) {
      a(null), u(i);
    } finally {
      l(!1);
    }
  }, []);
  ne(() => {
    f(), t();
  }, [f, t]);
  const v = (o == null ? void 0 : o.data) || {}, w = v.brokerAddrTable && typeof v.brokerAddrTable == "object" ? v.brokerAddrTable : {}, O = v.clusterAddrTable && typeof v.clusterAddrTable == "object" ? v.clusterAddrTable : {}, _ = (r == null ? void 0 : r.brokerPort) ?? null, p = Object.keys(w).map((i) => {
    var F;
    const c = w[i] || {}, b = c.brokerAddrs && typeof c.brokerAddrs == "object" ? c.brokerAddrs : {}, j = Object.keys(b).map((W) => ({ id: W, addr: String(b[W]) })), C = b[0] ? String(b[0]) : ((F = j[0]) == null ? void 0 : F.addr) || null, P = C ? Number(C.split(":").pop()) : null;
    return {
      key: i,
      brokerName: c.brokerName || i,
      cluster: c.cluster,
      masterAddr: C,
      replicas: j,
      port: P,
      expected: _,
      portMatched: _ == null ? null : P === _
    };
  }), h = Object.keys(O).map((i) => ({
    key: i,
    cluster: i,
    members: (Array.isArray(O[i]) ? O[i] : []).map(String)
  })), D = o && !s ? K(o) : null;
  return /* @__PURE__ */ e.jsxs("div", { children: [
    /* @__PURE__ */ e.jsx(
      ue,
      {
        title: "集群与 Broker",
        subtitle: "NameServer 码 4 GET_BROKER_CLUSTER_INFO → brokerAddrTable / clusterAddrTable"
      }
    ),
    /* @__PURE__ */ e.jsxs(I, { extra: /* @__PURE__ */ e.jsx(q, { children: /* @__PURE__ */ e.jsx(z, { icon: /* @__PURE__ */ e.jsx(oe, {}), loading: d, onClick: () => {
      f(), t();
    }, children: "刷新" }) }), children: [
      /* @__PURE__ */ e.jsx(ce, { data: r }),
      /* @__PURE__ */ e.jsx(de, { data: r }),
      s && /* @__PURE__ */ e.jsx(
        k,
        {
          type: "error",
          showIcon: !0,
          message: `读 NameServer 失败：${L(s)}`,
          description: `这一档是 nameserver 的只读码，不需要 broker 注册就能应答；它不通说明 :${(r == null ? void 0 : r.namesrvPort) ?? "?"} 上根本没有能答话的进程（或者答话的那个跟本 JVM 不是一家的，见顶部红条）。端口取自 /api/mq/__instance，写死端口会在换端口后把人指到别的进程上。`
        }
      ),
      D && /* @__PURE__ */ e.jsx(k, { type: "warning", showIcon: !0, style: { marginBottom: 12 }, message: D }),
      o && !s && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
        /* @__PURE__ */ e.jsx(X, { res: o }),
        /* @__PURE__ */ e.jsxs(xe, { gutter: 16, style: { marginBottom: 12 }, children: [
          /* @__PURE__ */ e.jsx(A, { span: 6, children: /* @__PURE__ */ e.jsx(B, { title: "已注册 Broker 数", value: p.length }) }),
          /* @__PURE__ */ e.jsx(A, { span: 6, children: /* @__PURE__ */ e.jsx(B, { title: "Cluster 数", value: h.length }) }),
          /* @__PURE__ */ e.jsx(A, { span: 6, children: /* @__PURE__ */ e.jsx(B, { title: "请求码", value: `${o.requestCode || "-"} ${o.requestName || ""}` }) }),
          /* @__PURE__ */ e.jsx(A, { span: 6, children: /* @__PURE__ */ e.jsx(B, { title: "往返耗时", value: o.elapsedMs ?? "-", suffix: "ms" }) })
        ] }),
        p.length === 0 ? /* @__PURE__ */ e.jsx(
          se,
          {
            title: "brokerAddrTable 是空的",
            description: "没有任何 broker 向这台 NameServer 发过 REGISTER_BROKER（码 1）。两种成因要分开：① broker 子进程没起 / 没 bind 上（看顶部红条与「实例与端口」页的 bind 原文）；② broker 起了但配的 zmq.namesrv-addr 指向的是另一台 NameServer。本页不给「注册 broker」按钮 —— 那是写面，另立卡。"
          }
        ) : /* @__PURE__ */ e.jsx(
          H,
          {
            size: "small",
            rowKey: "key",
            loading: d,
            dataSource: p,
            columns: Ke,
            pagination: !1
          }
        )
      ] }),
      !o && !s && /* @__PURE__ */ e.jsx(se, { title: "还没读到数据", description: "刷新以发送一次只读请求码 4。" })
    ] }),
    h.length > 0 && /* @__PURE__ */ e.jsx(I, { title: "Cluster → Broker 归属", style: { marginTop: 16 }, children: /* @__PURE__ */ e.jsx(
      Q,
      {
        bordered: !0,
        size: "small",
        column: 1,
        items: h.map((i) => ({
          key: i.key,
          label: i.cluster,
          children: /* @__PURE__ */ e.jsx(q, { wrap: !0, children: i.members.map((c) => /* @__PURE__ */ e.jsx(x, { children: c }, c)) })
        }))
      }
    ) }),
    /* @__PURE__ */ e.jsx(I, { title: "上游语义提示", style: { marginTop: 16 }, children: /* @__PURE__ */ e.jsxs("div", { style: { color: "#64748b", fontSize: 12, lineHeight: 1.9 }, children: [
      /* @__PURE__ */ e.jsxs("div", { children: [
        "· ",
        /* @__PURE__ */ e.jsx("code", { children: "brokerAddrTable" }),
        " 记的是「谁发过 REGISTER_BROKER 心跳」，不是「谁在跑」。进程活着但心跳线程没起 ⇒ 这里为空。"
      ] }),
      /* @__PURE__ */ e.jsxs("div", { children: [
        "· 上表最后一列是本页自带的第二重归属校验：注册的 broker 端口应当等于",
        /* @__PURE__ */ e.jsx("code", { children: "zmq.broker-port" }),
        "。不等就说明",
        /* @__PURE__ */ e.jsx("b", { children: "这台 NameServer 记的是另一套集群" }),
        "， 后面 broker 页拿到的地址会被代理层的环回白名单挡掉（403），那是正确行为不是 bug。"
      ] }),
      /* @__PURE__ */ e.jsxs("div", { children: [
        "· 代理层只会拨环回地址（",
        /* @__PURE__ */ e.jsx("code", { children: "127.0.0.1" }),
        " / ",
        /* @__PURE__ */ e.jsx("code", { children: "localhost" }),
        " / ",
        /* @__PURE__ */ e.jsx("code", { children: "::1" }),
        "）， 非环回的注册地址一律拒绝，这既是安全边界也是防误连别机的护栏。"
      ] })
    ] }) })
  ] });
}
const Ze = [
  {
    title: "Topic",
    dataIndex: "topic",
    key: "topic",
    render: (r) => /* @__PURE__ */ e.jsx("code", { children: r })
  },
  {
    title: "NameServer (码 8)",
    dataIndex: "onNamesrv",
    key: "onNamesrv",
    width: 160,
    render: (r) => r ? /* @__PURE__ */ e.jsx(x, { color: "success", children: "已注册" }) : /* @__PURE__ */ e.jsx(x, { color: "error", children: "没有" })
  },
  {
    title: "Broker (码 8)",
    dataIndex: "onBroker",
    key: "onBroker",
    width: 140,
    render: (r, t) => r ? /* @__PURE__ */ e.jsx(x, { color: "success", children: "有配置" }) : t.brokerUnavailable ? /* @__PURE__ */ e.jsx(x, { children: "未取到" }) : /* @__PURE__ */ e.jsx(x, { color: "error", children: "没有" })
  },
  {
    title: "一致性",
    key: "consistency",
    width: 200,
    render: (r, t) => t.onNamesrv && t.onBroker ? /* @__PURE__ */ e.jsx(x, { color: "success", children: "两侧一致" }) : /* @__PURE__ */ e.jsx(x, { color: "warning", children: t.onNamesrv ? "只在 NameServer（疑似残留注册）" : "只在 Broker（未向 NameServer 注册）" })
  }
];
function er() {
  var ie, J;
  const { instance: r, poll: t } = ae(), [o, a] = S(null), [s, u] = S(null), [d, l] = S(null), [f, v] = S(null), [w, O] = S(!1), [_, p] = S({}), h = Y(async () => {
    O(!0), l(null), v(null), await Promise.all([
      U.topics().then(a).catch((y) => {
        a(null), l(y);
      }),
      U.brokerTopics().then(u).catch((y) => {
        u(null), v(y);
      })
    ]), O(!1), t();
  }, [t]);
  ne(() => {
    h();
  }, [h]);
  const D = Y(async (y) => {
    if (!_[y]) {
      p((R) => ({ ...R, [y]: { loading: !0 } }));
      try {
        const R = await U.route(y);
        p((Z) => ({ ...Z, [y]: R }));
      } catch (R) {
        p((Z) => ({ ...Z, [y]: { loadError: L(R) } }));
      }
    }
  }, [_]), i = o && o.responseCode === 0 && Array.isArray((ie = o.data) == null ? void 0 : ie.topics) ? o.data.topics.map(String) : null, c = s && s.responseCode === 0 && Array.isArray((J = s.data) == null ? void 0 : J.topics) ? s.data.topics.map(String) : null, b = Re(() => {
    const y = /* @__PURE__ */ new Set([...i || [], ...c || []]);
    return Array.from(y).sort().map((R) => ({
      key: R,
      topic: R,
      onNamesrv: i ? i.includes(R) : !1,
      onBroker: c ? c.includes(R) : !1,
      brokerUnavailable: !c
    }));
  }, [i, c]), j = b.filter((y) => y.onNamesrv && !y.onBroker).length, C = b.filter((y) => !y.onNamesrv && y.onBroker).length, P = o && !d ? K(o) : null, F = s && !f ? K(s) : null, W = !i && !c;
  return /* @__PURE__ */ e.jsxs("div", { children: [
    /* @__PURE__ */ e.jsx(
      ue,
      {
        title: "Topic 列表",
        subtitle: "NameServer 码 8 GET_ALL_TOPIC_LIST ∥ Broker 码 8 GET_ALL_TOPIC_LIST → 差集 + 按需码 3 路由详情"
      }
    ),
    /* @__PURE__ */ e.jsxs(I, { extra: /* @__PURE__ */ e.jsx(q, { children: /* @__PURE__ */ e.jsx(z, { icon: /* @__PURE__ */ e.jsx(oe, {}), loading: w, onClick: h, children: "刷新" }) }), children: [
      /* @__PURE__ */ e.jsx(ce, { data: r }),
      /* @__PURE__ */ e.jsx(de, { data: r }),
      /* @__PURE__ */ e.jsx(je, { data: r }),
      d && /* @__PURE__ */ e.jsx(
        k,
        {
          type: "error",
          showIcon: !0,
          style: { marginBottom: 12 },
          message: `读 NameServer 的 topic 列表失败：${L(d)}`
        }
      ),
      f && /* @__PURE__ */ e.jsx(
        k,
        {
          type: "error",
          showIcon: !0,
          style: { marginBottom: 12 },
          message: `读 Broker 的 topicConfigTable 失败：${L(f)}`,
          description: "Broker 侧的地址全靠 NameServer 的码 4 发现，且只允许环回。这一档失败通常说明集群里根本没有注册过 broker（见「集群与 Broker」页），或者代理层按环回白名单拒了外部地址 —— 后者是正确行为。"
        }
      ),
      P && /* @__PURE__ */ e.jsx(k, { type: "warning", showIcon: !0, style: { marginBottom: 12 }, message: `NameServer：${P}` }),
      F && /* @__PURE__ */ e.jsx(k, { type: "warning", showIcon: !0, style: { marginBottom: 12 }, message: `Broker：${F}` }),
      o && !d && /* @__PURE__ */ e.jsx(X, { res: o }),
      s && !f && /* @__PURE__ */ e.jsx(X, { res: s }),
      /* @__PURE__ */ e.jsxs(xe, { gutter: 16, style: { marginBottom: 12 }, children: [
        /* @__PURE__ */ e.jsx(A, { span: 6, children: /* @__PURE__ */ e.jsx(B, { title: "NameServer 侧 topic 数", value: i ? i.length : "—" }) }),
        /* @__PURE__ */ e.jsx(A, { span: 6, children: /* @__PURE__ */ e.jsx(B, { title: "Broker 侧 topic 数", value: c ? c.length : "—" }) }),
        /* @__PURE__ */ e.jsx(A, { span: 6, children: /* @__PURE__ */ e.jsx(
          B,
          {
            title: "只在 NameServer",
            value: c ? j : "—",
            valueStyle: j && c ? { color: "#b45309" } : void 0
          }
        ) }),
        /* @__PURE__ */ e.jsx(A, { span: 6, children: /* @__PURE__ */ e.jsx(
          B,
          {
            title: "只在 Broker",
            value: c ? C : "—",
            valueStyle: C && c ? { color: "#b45309" } : void 0
          }
        ) })
      ] }),
      W ? /* @__PURE__ */ e.jsx(
        se,
        {
          title: "两侧都没取到 topic 列表",
          description: "这不是「集群里没有 topic」的证据：上面任一档报错、或上游回了非 0 业务码（HTTP 仍是 200），都到这里显示为取不到。先读上面的红/黄条定性，再决定要不要怀疑数据。"
        }
      ) : /* @__PURE__ */ e.jsx(
        H,
        {
          size: "small",
          rowKey: "key",
          loading: w,
          dataSource: b,
          columns: Ze,
          pagination: { pageSize: 20, hideOnSinglePage: !0 },
          expandable: {
            onExpand: (y, R) => {
              y && D(R.topic);
            },
            expandedRowRender: (y) => /* @__PURE__ */ e.jsx(rr, { state: _[y.topic] })
          }
        }
      )
    ] }),
    /* @__PURE__ */ e.jsx(I, { title: "这一页的两个语义坑", style: { marginTop: 16 }, children: /* @__PURE__ */ e.jsxs("div", { style: { color: "#64748b", fontSize: 12, lineHeight: 1.9 }, children: [
      /* @__PURE__ */ e.jsxs("div", { children: [
        "· 码 8 只回",
        /* @__PURE__ */ e.jsx("b", { children: "名字" }),
        "，队列数 / perm / 集群归属要展开走码 3；展开是懒加载，所以打开本页不会打出 N 个请求。"
      ] }),
      /* @__PURE__ */ e.jsxs("div", { children: [
        "· 码 3 查不存在的 topic 时上游回的是 ",
        /* @__PURE__ */ e.jsx("code", { children: "responseCode=1 SYSTEM_ERROR" }),
        " + remark 「topic X not exist」—— ",
        /* @__PURE__ */ e.jsx("b", { children: "同一个码 1 既表示系统错误也表示 topic 不存在" }),
        '， 只有 remark 能分开，所以下面把 remark 原文贴出来而不是翻译成"出错了"。'
      ] })
    ] }) })
  ] });
}
function rr({ state: r }) {
  var u, d;
  if (!r) return /* @__PURE__ */ e.jsx("span", { style: { color: "#94a3b8", fontSize: 12 }, children: "展开时向 NameServer 发送码 3 GET_ROUTEINFO_BY_TOPIC。" });
  if (r.loading) return /* @__PURE__ */ e.jsx("span", { style: { color: "#94a3b8", fontSize: 12 }, children: "查询中…" });
  if (r.loadError) return /* @__PURE__ */ e.jsx(k, { type: "error", showIcon: !0, message: `路由查询失败：${r.loadError}` });
  const t = Ve(r), o = K(r), a = Array.isArray((u = r.data) == null ? void 0 : u.queueDatas) ? r.data.queueDatas : [], s = Array.isArray((d = r.data) == null ? void 0 : d.brokerDatas) ? r.data.brokerDatas : [];
  return /* @__PURE__ */ e.jsxs("div", { children: [
    t && /* @__PURE__ */ e.jsx(
      k,
      {
        type: "warning",
        showIcon: !0,
        style: { marginBottom: 8 },
        message: `NameServer 报这个 topic 没有路由（原文：${t}）`,
        description: "上游用的是 SYSTEM_ERROR(1) + remark，不是「查到但是空」，所以这一格不能渲染成空表。"
      }
    ),
    !t && o && /* @__PURE__ */ e.jsx(k, { type: "error", showIcon: !0, style: { marginBottom: 8 }, message: o }),
    r.data && /* @__PURE__ */ e.jsx(X, { res: r }),
    r.data && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
      /* @__PURE__ */ e.jsx(
        Q,
        {
          bordered: !0,
          size: "small",
          column: 3,
          style: { marginBottom: 8 },
          items: [
            { key: "target", label: "目标", children: /* @__PURE__ */ e.jsx("code", { children: r.target || "-" }) },
            { key: "order", label: "order", children: String(r.data.order ?? "-") },
            { key: "elapsed", label: "往返", children: `${r.elapsedMs ?? "-"} ms` }
          ]
        }
      ),
      /* @__PURE__ */ e.jsx(
        H,
        {
          size: "small",
          rowKey: (l) => `${l.brokerName}-${l.queueId ?? ""}`,
          dataSource: a,
          pagination: !1,
          columns: [
            { title: "Broker", dataIndex: "brokerName", key: "brokerName", render: (l) => /* @__PURE__ */ e.jsx("code", { children: l }) },
            { title: "readQueueNums", dataIndex: "readQueueNums", key: "readQueueNums" },
            { title: "writeQueueNums", dataIndex: "writeQueueNums", key: "writeQueueNums" },
            { title: "perm", dataIndex: "perm", key: "perm" },
            { title: "topicSysFlag", dataIndex: "topicSysFlag", key: "topicSysFlag" }
          ]
        }
      ),
      s.length > 0 && /* @__PURE__ */ e.jsxs("div", { style: { marginTop: 8, color: "#64748b", fontSize: 12 }, children: [
        "brokerDatas：",
        s.map((l) => `${l.brokerName}→${JSON.stringify(l.brokerAddrs || {})}`).join("，")
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
], tr = [
  { title: "Topic", dataIndex: "topic", key: "topic", render: (r) => /* @__PURE__ */ e.jsx("code", { children: r }) },
  { title: "readQueueNums", dataIndex: "readQueueNums", key: "readQueueNums", width: 130 },
  { title: "writeQueueNums", dataIndex: "writeQueueNums", key: "writeQueueNums", width: 140 },
  { title: "perm", dataIndex: "perm", key: "perm", width: 90 },
  {
    title: "order",
    dataIndex: "order",
    key: "order",
    width: 90,
    render: (r) => r ? /* @__PURE__ */ e.jsx(x, { color: "blue", children: "true" }) : /* @__PURE__ */ e.jsx(x, { children: "false" })
  },
  { title: "unit", dataIndex: "unit", key: "unit", width: 110, render: (r) => r ?? "—" }
];
function nr() {
  const { instance: r, poll: t } = ae(), [o, a] = S([]), [s, u] = S(null), [d, l] = S(null), [f, v] = S({}), [w, O] = S(!1), _ = Y(async () => {
    var i;
    try {
      const c = await U.cluster(), b = ((i = c == null ? void 0 : c.data) == null ? void 0 : i.brokerAddrTable) || {}, j = Object.keys(b);
      return a(j), u((C) => C && j.includes(C) ? C : j[0] || null), l(c && c.responseCode !== 0 ? new Error(K(c) || "码 4 返回非 0") : null), j;
    } catch (c) {
      return a([]), u(null), l(c), [];
    }
  }, []), p = Y(async (i) => {
    O(!0), await Promise.all(Oe.map(async (c) => {
      v((b) => ({ ...b, [c.key]: { loading: !0 } }));
      try {
        const b = await U[c.api](i || void 0);
        v((j) => ({ ...j, [c.key]: b }));
      } catch (b) {
        v((j) => ({ ...j, [c.key]: { loadError: L(b), raw: b } }));
      }
    })), O(!1), t();
  }, [t]);
  ne(() => {
    _();
  }, [_]), ne(() => {
    p(s || void 0);
  }, [s, p]);
  const h = (i) => {
    var b;
    const c = (b = i == null ? void 0 : i.data) == null ? void 0 : b.offsetTable;
    return !c || typeof c != "object" ? null : Object.keys(c).flatMap((j) => {
      const C = c[j];
      return !C || typeof C != "object" ? [] : Object.keys(C).map((P) => ({
        key: `${j}-${P}`,
        topic: j,
        queueId: P,
        maxOffset: C[P]
      }));
    });
  }, D = Re(() => {
    var c, b;
    const i = (b = (c = f.topicConfig) == null ? void 0 : c.data) == null ? void 0 : b.topicConfigTable;
    return !i || typeof i != "object" ? null : Object.keys(i).map((j) => ({ key: j, topic: j, ...i[j] || {} }));
  }, [f.topicConfig]);
  return /* @__PURE__ */ e.jsxs("div", { children: [
    /* @__PURE__ */ e.jsx(
      ue,
      {
        title: "Broker 元数据",
        subtitle: "BrokerOutAPI 码 320/321/322/323/324 · 地址由 NameServer 码 4 发现且只允许环回"
      }
    ),
    /* @__PURE__ */ e.jsxs(I, { extra: /* @__PURE__ */ e.jsxs(q, { children: [
      /* @__PURE__ */ e.jsx("span", { style: { color: "#64748b", fontSize: 12 }, children: "Broker" }),
      /* @__PURE__ */ e.jsx(
        qe,
        {
          size: "small",
          style: { width: 240 },
          value: s,
          onChange: u,
          placeholder: o.length ? void 0 : "码 4 没有发现任何 broker",
          options: o.map((i) => ({ value: i, label: i }))
        }
      ),
      /* @__PURE__ */ e.jsx(z, { icon: /* @__PURE__ */ e.jsx(oe, {}), loading: w, onClick: () => p(s || void 0), children: "刷新五档" })
    ] }), children: [
      /* @__PURE__ */ e.jsx(ce, { data: r }),
      /* @__PURE__ */ e.jsx(de, { data: r }),
      /* @__PURE__ */ e.jsx(je, { data: r }),
      d && /* @__PURE__ */ e.jsx(
        k,
        {
          type: "error",
          showIcon: !0,
          style: { marginBottom: 12 },
          message: `无法从 NameServer 发现 broker：${L(d)}`,
          description: "下面五档的地址全靠码 4，发现不到就只能整页 503 —— 那是「集群里没有注册过的 broker」，不是这五档本身坏了。"
        }
      ),
      /* @__PURE__ */ e.jsxs(xe, { gutter: 16, children: [
        /* @__PURE__ */ e.jsx(A, { span: 8, children: /* @__PURE__ */ e.jsx(B, { title: "码 4 发现的 broker 数", value: o.length }) }),
        /* @__PURE__ */ e.jsx(A, { span: 8, children: /* @__PURE__ */ e.jsx(B, { title: "当前取数对象", value: s || "—" }) }),
        /* @__PURE__ */ e.jsx(A, { span: 8, children: /* @__PURE__ */ e.jsx(
          B,
          {
            title: "store 路径存在",
            value: (r == null ? void 0 : r.storePathExists) == null ? "未知" : r.storePathExists ? "是" : "否"
          }
        ) })
      ] })
    ] }),
    Oe.map((i) => /* @__PURE__ */ e.jsx(
      or,
      {
        section: i,
        state: f[i.key],
        rows: i.key === "topicConfig" ? D : i.key === "consumerOffset" || i.key === "delayOffset" ? h(f[i.key]) : null
      },
      i.key
    ))
  ] });
}
function or({ section: r, state: t, rows: o }) {
  const a = t == null ? void 0 : t.loadError, s = t && !a && t.responseCode !== void 0 ? K(t) : null, u = De[r.path], d = t == null ? void 0 : t.data;
  return /* @__PURE__ */ e.jsxs(
    I,
    {
      title: r.label,
      style: { marginTop: 16 },
      size: "small",
      extra: t != null && t.target ? /* @__PURE__ */ e.jsx(x, { children: String(t.target) }) : null,
      children: [
        !t || t.loading ? /* @__PURE__ */ e.jsx("span", { style: { color: "#94a3b8", fontSize: 12 }, children: "取数中…" }) : null,
        a && /* @__PURE__ */ e.jsx(k, { type: "error", showIcon: !0, message: `调用失败：${a}` }),
        s && /* @__PURE__ */ e.jsx(k, { type: "warning", showIcon: !0, style: { marginBottom: 12 }, message: s }),
        t && !a && /* @__PURE__ */ e.jsx(X, { res: t }),
        u && /* @__PURE__ */ e.jsx(
          k,
          {
            type: "info",
            showIcon: !0,
            style: { marginBottom: 12 },
            message: "上游语义（不是本页的推断）",
            description: u
          }
        ),
        t && !a && d && r.key === "topicConfig" && (o && o.length ? /* @__PURE__ */ e.jsx(
          H,
          {
            size: "small",
            rowKey: "key",
            dataSource: o,
            columns: tr,
            pagination: { pageSize: 10, hideOnSinglePage: !0 }
          }
        ) : /* @__PURE__ */ e.jsx(se, { title: "topicConfigTable 是空的", description: "本 broker 没有注册任何 topic 配置。" })),
        t && !a && d && (r.key === "consumerOffset" || r.key === "delayOffset") && (o && o.length ? /* @__PURE__ */ e.jsx(
          H,
          {
            size: "small",
            rowKey: "key",
            dataSource: o,
            pagination: { pageSize: 10, hideOnSinglePage: !0 },
            columns: [
              { title: "Topic", dataIndex: "topic", key: "topic", render: (l) => /* @__PURE__ */ e.jsx("code", { children: l }) },
              { title: "queueId", dataIndex: "queueId", key: "queueId", width: 110 },
              { title: "maxOffset", dataIndex: "maxOffset", key: "maxOffset", width: 140 }
            ]
          }
        ) : /* @__PURE__ */ e.jsx(
          se,
          {
            title: "offsetTable 是空的",
            description: "注意上面那条「上游语义」：位点表为空可能是真没有消息，也可能只是这条链路上还没产生过写入。"
          }
        )),
        t && !a && d && r.key === "subscription" && /* @__PURE__ */ e.jsxs("div", { style: { color: "#64748b", fontSize: 12 }, children: [
          "body 原文（注意它连 ",
          /* @__PURE__ */ e.jsx("code", { children: "subscription" }),
          " 字段都没有，是一个空的",
          /* @__PURE__ */ e.jsx("code", { children: "ConsumerOffsetSerializeWrapper" }),
          "）：",
          /* @__PURE__ */ e.jsx("pre", { style: { whiteSpace: "pre-wrap", marginTop: 8, fontSize: 12 }, children: JSON.stringify(d, null, 2) })
        ] }),
        t && !a && d && r.key === "dataVersion" && /* @__PURE__ */ e.jsx(
          Q,
          {
            bordered: !0,
            size: "small",
            column: 1,
            items: Object.keys(d).map((l) => ({
              key: l,
              label: l,
              children: Ee(d[l])
            }))
          }
        ),
        t && !a && (d == null ? void 0 : d.dataVersion) && r.key !== "dataVersion" && /* @__PURE__ */ e.jsxs("div", { style: { marginTop: 10, color: "#64748b", fontSize: 12 }, children: [
          "dataVersion：",
          Ee(d.dataVersion)
        ] })
      ]
    }
  );
}
function fr() {
  return /* @__PURE__ */ e.jsxs($e, { children: [
    /* @__PURE__ */ e.jsx(ee, { index: !0, element: /* @__PURE__ */ e.jsx(ze, { to: "instance", replace: !0 }) }),
    /* @__PURE__ */ e.jsx(ee, { path: "instance", element: /* @__PURE__ */ e.jsx(He, {}) }),
    /* @__PURE__ */ e.jsx(ee, { path: "cluster", element: /* @__PURE__ */ e.jsx(Xe, {}) }),
    /* @__PURE__ */ e.jsx(ee, { path: "topics", element: /* @__PURE__ */ e.jsx(er, {}) }),
    /* @__PURE__ */ e.jsx(ee, { path: "broker", element: /* @__PURE__ */ e.jsx(nr, {}) })
  ] });
}
export {
  De as K,
  fr as M,
  pr as T,
  be as Z,
  mr as a,
  ur as b,
  L as c,
  Ee as d,
  K as e,
  dr as i,
  e as j,
  U as m,
  Ve as r
};
