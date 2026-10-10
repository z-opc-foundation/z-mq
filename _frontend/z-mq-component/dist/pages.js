import pe, { useState as g, useEffect as $ } from "react";
import { ReloadOutlined as X, HomeOutlined as be, DashboardOutlined as he, AppstoreOutlined as ye } from "@ant-design/icons";
import { Typography as A, Space as O, Button as H, Alert as Z, Spin as ge, Row as Q, Col as S, Card as j, Statistic as z, Descriptions as U, Table as xe, Tag as ve } from "antd";
import { m as K } from "./api-o8tZUY9i.js";
import { c as Me } from "./api-o8tZUY9i.js";
import { useNavigate as je } from "react-router-dom";
var k = { exports: {} }, T = {};
/**
 * @license React
 * react-jsx-runtime.production.js
 *
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
var B;
function Ee() {
  if (B) return T;
  B = 1;
  var o = Symbol.for("react.transitional.element"), u = Symbol.for("react.fragment");
  function d(m, n, f) {
    var p = null;
    if (f !== void 0 && (p = "" + f), n.key !== void 0 && (p = "" + n.key), "key" in n) {
      f = {};
      for (var s in n)
        s !== "key" && (f[s] = n[s]);
    } else f = n;
    return n = f.ref, {
      $$typeof: o,
      type: m,
      key: p,
      ref: n !== void 0 ? n : null,
      props: f
    };
  }
  return T.Fragment = u, T.jsx = d, T.jsxs = d, T;
}
var _ = {};
/**
 * @license React
 * react-jsx-runtime.development.js
 *
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
var V;
function Te() {
  return V || (V = 1, process.env.NODE_ENV !== "production" && (function() {
    function o(e) {
      if (e == null) return null;
      if (typeof e == "function")
        return e.$$typeof === fe ? null : e.displayName || e.name || null;
      if (typeof e == "string") return e;
      switch (e) {
        case w:
          return "Fragment";
        case te:
          return "Profiler";
        case re:
          return "StrictMode";
        case se:
          return "Suspense";
        case ie:
          return "SuspenseList";
        case ce:
          return "Activity";
        case ue:
          return "ViewTransition";
      }
      if (typeof e == "object")
        switch (typeof e.tag == "number" && console.error(
          "Received an unexpected object in getComponentNameFromType(). This is likely a bug in React. Please file an issue."
        ), e.$$typeof) {
          case ee:
            return "Portal";
          case ae:
            return e.displayName || "Context";
          case ne:
            return (e._context.displayName || "Context") + ".Consumer";
          case oe:
            var t = e.render;
            return e = e.displayName, e || (e = t.displayName || t.name || "", e = e !== "" ? "ForwardRef(" + e + ")" : "ForwardRef"), e;
          case le:
            return t = e.displayName || null, t !== null ? t : o(e.type) || "Memo";
          case P:
            t = e._payload, e = e._init;
            try {
              return o(e(t));
            } catch {
            }
        }
      return null;
    }
    function u(e) {
      return "" + e;
    }
    function d(e) {
      try {
        u(e);
        var t = !1;
      } catch {
        t = !0;
      }
      if (t) {
        t = console;
        var a = t.error, l = typeof Symbol == "function" && Symbol.toStringTag && e[Symbol.toStringTag] || e.constructor.name || "Object";
        return a.call(
          t,
          "The provided key is an unsupported type %s. This value must be coerced to a string before using it here.",
          l
        ), u(e);
      }
    }
    function m(e) {
      if (e === w) return "<>";
      if (typeof e == "object" && e !== null && e.$$typeof === P)
        return "<...>";
      try {
        var t = o(e);
        return t ? "<" + t + ">" : "<...>";
      } catch {
        return "<...>";
      }
    }
    function n() {
      var e = N.A;
      return e === null ? null : e.getOwner();
    }
    function f() {
      return Error("react-stack-top-frame");
    }
    function p(e) {
      if (L.call(e, "key")) {
        var t = Object.getOwnPropertyDescriptor(e, "key").get;
        if (t && t.isReactWarning) return !1;
      }
      return e.key !== void 0;
    }
    function s(e, t) {
      function a() {
        F || (F = !0, console.error(
          "%s: `key` is not a prop. Trying to access it will result in `undefined` being returned. If you need to access the same value within the child component, you should pass it as a different prop. (https://react.dev/link/special-props)",
          t
        ));
      }
      a.isReactWarning = !0, Object.defineProperty(e, "key", {
        get: a,
        configurable: !0
      });
    }
    function i() {
      var e = o(this.type);
      return D[e] || (D[e] = !0, console.error(
        "Accessing element.ref was removed in React 19. ref is now a regular prop. It will be removed from the JSX Element type in a future release."
      )), e = this.props.ref, e !== void 0 ? e : null;
    }
    function y(e, t, a, l, h, b) {
      var c = a.ref;
      return e = {
        $$typeof: q,
        type: e,
        key: t,
        props: a,
        _owner: l
      }, (c !== void 0 ? c : null) !== null ? Object.defineProperty(e, "ref", {
        enumerable: !1,
        get: i
      }) : Object.defineProperty(e, "ref", { enumerable: !1, value: null }), e._store = {}, Object.defineProperty(e._store, "validated", {
        configurable: !1,
        enumerable: !1,
        writable: !0,
        value: 0
      }), Object.defineProperty(e, "_debugInfo", {
        configurable: !1,
        enumerable: !1,
        writable: !0,
        value: null
      }), Object.defineProperty(e, "_debugStack", {
        configurable: !1,
        enumerable: !1,
        writable: !0,
        value: h
      }), Object.defineProperty(e, "_debugTask", {
        configurable: !1,
        enumerable: !1,
        writable: !0,
        value: b
      }), Object.freeze && (Object.freeze(e.props), Object.freeze(e)), e;
    }
    function x(e, t, a, l, h, b) {
      var c = t.children;
      if (c !== void 0)
        if (l)
          if (de(c)) {
            for (l = 0; l < c.length; l++)
              E(c[l]);
            Object.freeze && Object.freeze(c);
          } else
            console.error(
              "React.jsx: Static children should always be an array. You are likely explicitly calling React.jsxs or React.jsxDEV. Use the Babel transform instead."
            );
        else E(c);
      if (L.call(t, "key")) {
        c = o(e);
        var v = Object.keys(t).filter(function(me) {
          return me !== "key";
        });
        l = 0 < v.length ? "{key: someKey, " + v.join(": ..., ") + ": ...}" : "{key: someKey}", J[c + l] || (v = 0 < v.length ? "{" + v.join(": ..., ") + ": ...}" : "{}", console.error(
          `A props object containing a "key" prop is being spread into JSX:
  let props = %s;
  <%s {...props} />
React keys must be passed directly to JSX without using spread:
  let props = %s;
  <%s key={someKey} {...props} />`,
          l,
          c,
          v,
          c
        ), J[c + l] = !0);
      }
      if (c = null, a !== void 0 && (d(a), c = "" + a), p(t) && (d(t.key), c = "" + t.key), "key" in t) {
        a = {};
        for (var I in t)
          I !== "key" && (a[I] = t[I]);
      } else a = t;
      return c && s(
        a,
        typeof e == "function" ? e.displayName || e.name || "Unknown" : e
      ), y(
        e,
        c,
        a,
        n(),
        h,
        b
      );
    }
    function E(e) {
      Y(e) ? e._store && (e._store.validated = 1) : typeof e == "object" && e !== null && e.$$typeof === P && (e._payload.status === "fulfilled" ? Y(e._payload.value) && e._payload.value._store && (e._payload.value._store.validated = 1) : e._store && (e._store.validated = 1));
    }
    function Y(e) {
      return typeof e == "object" && e !== null && e.$$typeof === q;
    }
    var R = pe, q = Symbol.for("react.transitional.element"), ee = Symbol.for("react.portal"), w = Symbol.for("react.fragment"), re = Symbol.for("react.strict_mode"), te = Symbol.for("react.profiler"), ne = Symbol.for("react.consumer"), ae = Symbol.for("react.context"), oe = Symbol.for("react.forward_ref"), se = Symbol.for("react.suspense"), ie = Symbol.for("react.suspense_list"), le = Symbol.for("react.memo"), P = Symbol.for("react.lazy"), ce = Symbol.for("react.activity"), ue = Symbol.for("react.view_transition"), fe = Symbol.for("react.client.reference"), N = R.__CLIENT_INTERNALS_DO_NOT_USE_OR_WARN_USERS_THEY_CANNOT_UPGRADE, L = Object.prototype.hasOwnProperty, de = Array.isArray, C = console.createTask ? console.createTask : function() {
      return null;
    };
    R = {
      react_stack_bottom_frame: function(e) {
        return e();
      }
    };
    var F, D = {}, W = R.react_stack_bottom_frame.bind(
      R,
      f
    )(), M = C(m(f)), J = {};
    _.Fragment = w, _.jsx = function(e, t, a) {
      var l = 1e4 > N.recentlyCreatedOwnerStacks++;
      if (l) {
        var h = Error.stackTraceLimit;
        Error.stackTraceLimit = 10;
        var b = Error("react-stack-top-frame");
        Error.stackTraceLimit = h;
      } else b = W;
      return x(
        e,
        t,
        a,
        !1,
        b,
        l ? C(m(e)) : M
      );
    }, _.jsxs = function(e, t, a) {
      var l = 1e4 > N.recentlyCreatedOwnerStacks++;
      if (l) {
        var h = Error.stackTraceLimit;
        Error.stackTraceLimit = 10;
        var b = Error("react-stack-top-frame");
        Error.stackTraceLimit = h;
      } else b = W;
      return x(
        e,
        t,
        a,
        !0,
        b,
        l ? C(m(e)) : M
      );
    };
  })()), _;
}
var G;
function _e() {
  return G || (G = 1, process.env.NODE_ENV === "production" ? k.exports = Ee() : k.exports = Te()), k.exports;
}
var r = _e();
const { Title: Re, Paragraph: ke } = A;
function Se() {
  const [o, u] = g(null), [d, m] = g(!1), [n, f] = g(null), p = async () => {
    m(!0);
    try {
      u(await K.instance()), f(null);
    } catch (s) {
      f((s == null ? void 0 : s.message) || String(s)), u(null);
    } finally {
      m(!1);
    }
  };
  return $(() => {
    p();
    const s = setInterval(p, 15e3);
    return () => clearInterval(s);
  }, []), /* @__PURE__ */ r.jsxs("div", { children: [
    /* @__PURE__ */ r.jsxs(O, { style: { marginBottom: 16 }, children: [
      /* @__PURE__ */ r.jsx(Re, { level: 4, style: { margin: 0 }, children: "实例与端口" }),
      /* @__PURE__ */ r.jsx(H, { icon: /* @__PURE__ */ r.jsx(X, {}), onClick: p, loading: d, children: "刷新" }),
      /* @__PURE__ */ r.jsx(A.Text, { type: "secondary", children: "15s 自动刷新" })
    ] }),
    /* @__PURE__ */ r.jsx(ke, { type: "secondary", children: "/mq/__instance 自省：自省接口通不通 / 内嵌 broker bind 没上 / 端口属主是谁 / bind 失败原文，四件事拆开答。" }),
    n && /* @__PURE__ */ r.jsx(Z, { type: "error", showIcon: !0, style: { marginBottom: 16 }, message: "后端未连接", description: n }),
    d && !o && /* @__PURE__ */ r.jsx(ge, {}),
    o && /* @__PURE__ */ r.jsxs(r.Fragment, { children: [
      /* @__PURE__ */ r.jsxs(Q, { gutter: 16, style: { marginBottom: 16 }, children: [
        /* @__PURE__ */ r.jsx(S, { span: 8, children: /* @__PURE__ */ r.jsx(j, { children: /* @__PURE__ */ r.jsx(z, { title: "nameserver 存活", value: o.nameserverAlive ? "是" : "否" }) }) }),
        /* @__PURE__ */ r.jsx(S, { span: 8, children: /* @__PURE__ */ r.jsx(j, { children: /* @__PURE__ */ r.jsx(z, { title: "broker 存活", value: o.brokerAlive ? "是" : "否" }) }) }),
        /* @__PURE__ */ r.jsx(S, { span: 8, children: /* @__PURE__ */ r.jsx(j, { children: /* @__PURE__ */ r.jsx(
          z,
          {
            title: "bind 状态",
            value: o.bound ? "已 bind" : "未 bind",
            valueStyle: { color: o.bound ? "#3f8600" : "#cf1322" }
          }
        ) }) })
      ] }),
      /* @__PURE__ */ r.jsx(j, { title: "实例详情", children: /* @__PURE__ */ r.jsx(U, { column: 2, bordered: !0, size: "small", children: Object.entries(o).filter(([s]) => !["bindError"].includes(s)).map(([s, i]) => /* @__PURE__ */ r.jsx(U.Item, { label: s, children: typeof i == "object" ? JSON.stringify(i) : String(i) }, s)) }) })
    ] })
  ] });
}
const { Title: Oe, Paragraph: Ae } = A;
function we() {
  const [o, u] = g([]), [d, m] = g(!1), [n, f] = g(null), p = async () => {
    m(!0);
    try {
      const i = await K.topics(), y = Array.isArray(i) ? i : (i == null ? void 0 : i.topics) || [];
      u(y.map((x, E) => typeof x == "string" ? { key: E, name: x } : { key: E, ...x })), f(null);
    } catch (i) {
      f((i == null ? void 0 : i.message) || String(i));
    } finally {
      m(!1);
    }
  };
  $(() => {
    p();
  }, []);
  const s = Object.keys(o[0] || { name: "" }).map((i) => ({
    title: i,
    dataIndex: i,
    key: i,
    ellipsis: !0,
    render: (y) => typeof y == "object" ? JSON.stringify(y) : String(y ?? "—")
  }));
  return /* @__PURE__ */ r.jsxs("div", { children: [
    /* @__PURE__ */ r.jsxs(O, { style: { marginBottom: 16 }, children: [
      /* @__PURE__ */ r.jsx(Oe, { level: 4, style: { margin: 0 }, children: "Topic 清单" }),
      /* @__PURE__ */ r.jsx(H, { icon: /* @__PURE__ */ r.jsx(X, {}), onClick: p, loading: d, children: "刷新" })
    ] }),
    /* @__PURE__ */ r.jsx(Ae, { type: "secondary", children: "集群当前所有 topic（/mq/topics，从 broker 侧拉取）。" }),
    n && /* @__PURE__ */ r.jsx(Z, { type: "error", showIcon: !0, style: { marginBottom: 16 }, message: "后端未连接", description: n }),
    /* @__PURE__ */ r.jsx(
      xe,
      {
        rowKey: "key",
        dataSource: o,
        columns: s.length ? s : [{ title: "name", dataIndex: "name" }],
        loading: d && !o.length,
        size: "small",
        pagination: { pageSize: 20 }
      }
    )
  ] });
}
const { Title: Pe, Paragraph: Ne } = A;
function Ce() {
  const o = je(), [u, d] = g(null);
  $(() => {
    const n = localStorage.getItem("userInfo");
    if (n)
      try {
        d(JSON.parse(n));
      } catch {
        d({ name: n });
      }
  }, []);
  const m = Ie.filter((n) => n.key !== "/z-mq/home");
  return /* @__PURE__ */ r.jsxs("div", { children: [
    /* @__PURE__ */ r.jsx(j, { style: { marginBottom: 16, background: "linear-gradient(135deg, #7c3aed 0%, #6d28d9 100%)", border: "none" }, children: /* @__PURE__ */ r.jsxs(O, { direction: "vertical", size: 4, style: { color: "#fff" }, children: [
      /* @__PURE__ */ r.jsxs(Pe, { level: 3, style: { color: "#fff", margin: 0 }, children: [
        "欢迎",
        u != null && u.name ? `，${u.name}` : ""
      ] }),
      /* @__PURE__ */ r.jsx(Ne, { style: { color: "rgba(255,255,255,0.85)", margin: 0 }, children: "z-mq 消息队列 管理台" }),
      (u == null ? void 0 : u.role) && /* @__PURE__ */ r.jsx(ve, { style: { marginTop: 8, background: "rgba(255,255,255,0.2)", color: "#fff", border: "none" }, children: u.role })
    ] }) }),
    /* @__PURE__ */ r.jsx(Q, { gutter: [16, 16], children: m.map((n) => /* @__PURE__ */ r.jsx(S, { xs: 24, sm: 12, md: 12, lg: 8, children: /* @__PURE__ */ r.jsx(j, { hoverable: !0, onClick: () => o(n.key), style: { borderTop: "3px solid #7c3aed" }, children: /* @__PURE__ */ r.jsxs(O, { align: "start", size: 12, children: [
      /* @__PURE__ */ r.jsx("div", { style: {
        width: 44,
        height: 44,
        borderRadius: 8,
        flexShrink: 0,
        background: "rgba(124,58,237,0.08)",
        color: "#7c3aed",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontSize: 20
      }, children: n.icon }),
      /* @__PURE__ */ r.jsxs("div", { style: { minWidth: 0 }, children: [
        /* @__PURE__ */ r.jsx("div", { style: { fontSize: 15, fontWeight: 600, color: "#0f172a" }, children: n.label }),
        /* @__PURE__ */ r.jsx("div", { style: { fontSize: 12, color: "#94a3b8", marginTop: 2 }, children: n.key })
      ] })
    ] }) }) }, n.key)) })
  ] });
}
const Ie = [
  { key: "/z-mq/home", label: "首页", icon: /* @__PURE__ */ r.jsx(be, {}) },
  { key: "/z-mq/instance", label: "实例与端口", icon: /* @__PURE__ */ r.jsx(he, {}) },
  { key: "/z-mq/topics", label: "Topic 清单", icon: /* @__PURE__ */ r.jsx(ye, {}) }
], Fe = [
  { path: "/z-mq/home", Component: Ce },
  { path: "/z-mq/instance", Component: Se },
  { path: "/z-mq/topics", Component: we }
];
export {
  Se as Instance,
  we as Topics,
  Me as configureMq,
  Ie as menuItems,
  Fe as routes
};
