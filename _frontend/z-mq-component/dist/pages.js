import me, { useState as y, useEffect as V } from "react";
import { ReloadOutlined as G, DashboardOutlined as pe, AppstoreOutlined as Ee } from "@ant-design/icons";
import { Typography as I, Space as X, Button as H, Alert as Z, Spin as be, Row as _e, Col as N, Card as g, Statistic as C, Descriptions as z, Table as Te } from "antd";
import { m as Q } from "./api-o8tZUY9i.js";
import { c as Le } from "./api-o8tZUY9i.js";
var k = { exports: {} }, j = {};
/**
 * @license React
 * react-jsx-runtime.production.js
 *
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
var J;
function ve() {
  if (J) return j;
  J = 1;
  var s = Symbol.for("react.transitional.element"), p = Symbol.for("react.fragment");
  function d(m, c, u) {
    var f = null;
    if (u !== void 0 && (f = "" + u), c.key !== void 0 && (f = "" + c.key), "key" in c) {
      u = {};
      for (var a in c)
        a !== "key" && (u[a] = c[a]);
    } else u = c;
    return c = u.ref, {
      $$typeof: s,
      type: m,
      key: f,
      ref: c !== void 0 ? c : null,
      props: u
    };
  }
  return j.Fragment = p, j.jsx = d, j.jsxs = d, j;
}
var R = {};
/**
 * @license React
 * react-jsx-runtime.development.js
 *
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
var U;
function ye() {
  return U || (U = 1, process.env.NODE_ENV !== "production" && (function() {
    function s(e) {
      if (e == null) return null;
      if (typeof e == "function")
        return e.$$typeof === ue ? null : e.displayName || e.name || null;
      if (typeof e == "string") return e;
      switch (e) {
        case S:
          return "Fragment";
        case re:
          return "Profiler";
        case ee:
          return "StrictMode";
        case oe:
          return "Suspense";
        case se:
          return "SuspenseList";
        case le:
          return "Activity";
        case ce:
          return "ViewTransition";
      }
      if (typeof e == "object")
        switch (typeof e.tag == "number" && console.error(
          "Received an unexpected object in getComponentNameFromType(). This is likely a bug in React. Please file an issue."
        ), e.$$typeof) {
          case K:
            return "Portal";
          case ne:
            return e.displayName || "Context";
          case te:
            return (e._context.displayName || "Context") + ".Consumer";
          case ae:
            var r = e.render;
            return e = e.displayName, e || (e = r.displayName || r.name || "", e = e !== "" ? "ForwardRef(" + e + ")" : "ForwardRef"), e;
          case ie:
            return r = e.displayName || null, r !== null ? r : s(e.type) || "Memo";
          case O:
            r = e._payload, e = e._init;
            try {
              return s(e(r));
            } catch {
            }
        }
      return null;
    }
    function p(e) {
      return "" + e;
    }
    function d(e) {
      try {
        p(e);
        var r = !1;
      } catch {
        r = !0;
      }
      if (r) {
        r = console;
        var n = r.error, i = typeof Symbol == "function" && Symbol.toStringTag && e[Symbol.toStringTag] || e.constructor.name || "Object";
        return n.call(
          r,
          "The provided key is an unsupported type %s. This value must be coerced to a string before using it here.",
          i
        ), p(e);
      }
    }
    function m(e) {
      if (e === S) return "<>";
      if (typeof e == "object" && e !== null && e.$$typeof === O)
        return "<...>";
      try {
        var r = s(e);
        return r ? "<" + r + ">" : "<...>";
      } catch {
        return "<...>";
      }
    }
    function c() {
      var e = A.A;
      return e === null ? null : e.getOwner();
    }
    function u() {
      return Error("react-stack-top-frame");
    }
    function f(e) {
      if ($.call(e, "key")) {
        var r = Object.getOwnPropertyDescriptor(e, "key").get;
        if (r && r.isReactWarning) return !1;
      }
      return e.key !== void 0;
    }
    function a(e, r) {
      function n() {
        F || (F = !0, console.error(
          "%s: `key` is not a prop. Trying to access it will result in `undefined` being returned. If you need to access the same value within the child component, you should pass it as a different prop. (https://react.dev/link/special-props)",
          r
        ));
      }
      n.isReactWarning = !0, Object.defineProperty(e, "key", {
        get: n,
        configurable: !0
      });
    }
    function o() {
      var e = s(this.type);
      return D[e] || (D[e] = !0, console.error(
        "Accessing element.ref was removed in React 19. ref is now a regular prop. It will be removed from the JSX Element type in a future release."
      )), e = this.props.ref, e !== void 0 ? e : null;
    }
    function _(e, r, n, i, b, E) {
      var l = n.ref;
      return e = {
        $$typeof: L,
        type: e,
        key: r,
        props: n,
        _owner: i
      }, (l !== void 0 ? l : null) !== null ? Object.defineProperty(e, "ref", {
        enumerable: !1,
        get: o
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
        value: b
      }), Object.defineProperty(e, "_debugTask", {
        configurable: !1,
        enumerable: !1,
        writable: !0,
        value: E
      }), Object.freeze && (Object.freeze(e.props), Object.freeze(e)), e;
    }
    function T(e, r, n, i, b, E) {
      var l = r.children;
      if (l !== void 0)
        if (i)
          if (fe(l)) {
            for (i = 0; i < l.length; i++)
              x(l[i]);
            Object.freeze && Object.freeze(l);
          } else
            console.error(
              "React.jsx: Static children should always be an array. You are likely explicitly calling React.jsxs or React.jsxDEV. Use the Babel transform instead."
            );
        else x(l);
      if ($.call(r, "key")) {
        l = s(e);
        var v = Object.keys(r).filter(function(de) {
          return de !== "key";
        });
        i = 0 < v.length ? "{key: someKey, " + v.join(": ..., ") + ": ...}" : "{key: someKey}", W[l + i] || (v = 0 < v.length ? "{" + v.join(": ..., ") + ": ...}" : "{}", console.error(
          `A props object containing a "key" prop is being spread into JSX:
  let props = %s;
  <%s {...props} />
React keys must be passed directly to JSX without using spread:
  let props = %s;
  <%s key={someKey} {...props} />`,
          i,
          l,
          v,
          l
        ), W[l + i] = !0);
      }
      if (l = null, n !== void 0 && (d(n), l = "" + n), f(r) && (d(r.key), l = "" + r.key), "key" in r) {
        n = {};
        for (var P in r)
          P !== "key" && (n[P] = r[P]);
      } else n = r;
      return l && a(
        n,
        typeof e == "function" ? e.displayName || e.name || "Unknown" : e
      ), _(
        e,
        l,
        n,
        c(),
        b,
        E
      );
    }
    function x(e) {
      Y(e) ? e._store && (e._store.validated = 1) : typeof e == "object" && e !== null && e.$$typeof === O && (e._payload.status === "fulfilled" ? Y(e._payload.value) && e._payload.value._store && (e._payload.value._store.validated = 1) : e._store && (e._store.validated = 1));
    }
    function Y(e) {
      return typeof e == "object" && e !== null && e.$$typeof === L;
    }
    var h = me, L = Symbol.for("react.transitional.element"), K = Symbol.for("react.portal"), S = Symbol.for("react.fragment"), ee = Symbol.for("react.strict_mode"), re = Symbol.for("react.profiler"), te = Symbol.for("react.consumer"), ne = Symbol.for("react.context"), ae = Symbol.for("react.forward_ref"), oe = Symbol.for("react.suspense"), se = Symbol.for("react.suspense_list"), ie = Symbol.for("react.memo"), O = Symbol.for("react.lazy"), le = Symbol.for("react.activity"), ce = Symbol.for("react.view_transition"), ue = Symbol.for("react.client.reference"), A = h.__CLIENT_INTERNALS_DO_NOT_USE_OR_WARN_USERS_THEY_CANNOT_UPGRADE, $ = Object.prototype.hasOwnProperty, fe = Array.isArray, w = console.createTask ? console.createTask : function() {
      return null;
    };
    h = {
      react_stack_bottom_frame: function(e) {
        return e();
      }
    };
    var F, D = {}, q = h.react_stack_bottom_frame.bind(
      h,
      u
    )(), M = w(m(u)), W = {};
    R.Fragment = S, R.jsx = function(e, r, n) {
      var i = 1e4 > A.recentlyCreatedOwnerStacks++;
      if (i) {
        var b = Error.stackTraceLimit;
        Error.stackTraceLimit = 10;
        var E = Error("react-stack-top-frame");
        Error.stackTraceLimit = b;
      } else E = q;
      return T(
        e,
        r,
        n,
        !1,
        E,
        i ? w(m(e)) : M
      );
    }, R.jsxs = function(e, r, n) {
      var i = 1e4 > A.recentlyCreatedOwnerStacks++;
      if (i) {
        var b = Error.stackTraceLimit;
        Error.stackTraceLimit = 10;
        var E = Error("react-stack-top-frame");
        Error.stackTraceLimit = b;
      } else E = q;
      return T(
        e,
        r,
        n,
        !0,
        E,
        i ? w(m(e)) : M
      );
    };
  })()), R;
}
var B;
function xe() {
  return B || (B = 1, process.env.NODE_ENV === "production" ? k.exports = ve() : k.exports = ye()), k.exports;
}
var t = xe();
const { Title: je, Paragraph: Re } = I;
function he() {
  const [s, p] = y(null), [d, m] = y(!1), [c, u] = y(null), f = async () => {
    m(!0);
    try {
      p(await Q.instance()), u(null);
    } catch (a) {
      u((a == null ? void 0 : a.message) || String(a)), p(null);
    } finally {
      m(!1);
    }
  };
  return V(() => {
    f();
    const a = setInterval(f, 15e3);
    return () => clearInterval(a);
  }, []), /* @__PURE__ */ t.jsxs("div", { children: [
    /* @__PURE__ */ t.jsxs(X, { style: { marginBottom: 16 }, children: [
      /* @__PURE__ */ t.jsx(je, { level: 4, style: { margin: 0 }, children: "实例与端口" }),
      /* @__PURE__ */ t.jsx(H, { icon: /* @__PURE__ */ t.jsx(G, {}), onClick: f, loading: d, children: "刷新" }),
      /* @__PURE__ */ t.jsx(I.Text, { type: "secondary", children: "15s 自动刷新" })
    ] }),
    /* @__PURE__ */ t.jsx(Re, { type: "secondary", children: "/mq/__instance 自省：自省接口通不通 / 内嵌 broker bind 没上 / 端口属主是谁 / bind 失败原文，四件事拆开答。" }),
    c && /* @__PURE__ */ t.jsx(Z, { type: "error", showIcon: !0, style: { marginBottom: 16 }, message: "后端未连接", description: c }),
    d && !s && /* @__PURE__ */ t.jsx(be, {}),
    s && /* @__PURE__ */ t.jsxs(t.Fragment, { children: [
      /* @__PURE__ */ t.jsxs(_e, { gutter: 16, style: { marginBottom: 16 }, children: [
        /* @__PURE__ */ t.jsx(N, { span: 8, children: /* @__PURE__ */ t.jsx(g, { children: /* @__PURE__ */ t.jsx(C, { title: "nameserver 存活", value: s.nameserverAlive ? "是" : "否" }) }) }),
        /* @__PURE__ */ t.jsx(N, { span: 8, children: /* @__PURE__ */ t.jsx(g, { children: /* @__PURE__ */ t.jsx(C, { title: "broker 存活", value: s.brokerAlive ? "是" : "否" }) }) }),
        /* @__PURE__ */ t.jsx(N, { span: 8, children: /* @__PURE__ */ t.jsx(g, { children: /* @__PURE__ */ t.jsx(
          C,
          {
            title: "bind 状态",
            value: s.bound ? "已 bind" : "未 bind",
            valueStyle: { color: s.bound ? "#3f8600" : "#cf1322" }
          }
        ) }) })
      ] }),
      /* @__PURE__ */ t.jsx(g, { title: "实例详情", children: /* @__PURE__ */ t.jsx(z, { column: 2, bordered: !0, size: "small", children: Object.entries(s).filter(([a]) => !["bindError"].includes(a)).map(([a, o]) => /* @__PURE__ */ t.jsx(z.Item, { label: a, children: typeof o == "object" ? JSON.stringify(o) : String(o) }, a)) }) })
    ] })
  ] });
}
const { Title: ge, Paragraph: ke } = I;
function Se() {
  const [s, p] = y([]), [d, m] = y(!1), [c, u] = y(null), f = async () => {
    m(!0);
    try {
      const o = await Q.topics(), _ = Array.isArray(o) ? o : (o == null ? void 0 : o.topics) || [];
      p(_.map((T, x) => typeof T == "string" ? { key: x, name: T } : { key: x, ...T })), u(null);
    } catch (o) {
      u((o == null ? void 0 : o.message) || String(o));
    } finally {
      m(!1);
    }
  };
  V(() => {
    f();
  }, []);
  const a = Object.keys(s[0] || { name: "" }).map((o) => ({
    title: o,
    dataIndex: o,
    key: o,
    ellipsis: !0,
    render: (_) => typeof _ == "object" ? JSON.stringify(_) : String(_ ?? "—")
  }));
  return /* @__PURE__ */ t.jsxs("div", { children: [
    /* @__PURE__ */ t.jsxs(X, { style: { marginBottom: 16 }, children: [
      /* @__PURE__ */ t.jsx(ge, { level: 4, style: { margin: 0 }, children: "Topic 清单" }),
      /* @__PURE__ */ t.jsx(H, { icon: /* @__PURE__ */ t.jsx(G, {}), onClick: f, loading: d, children: "刷新" })
    ] }),
    /* @__PURE__ */ t.jsx(ke, { type: "secondary", children: "集群当前所有 topic（/mq/topics，从 broker 侧拉取）。" }),
    c && /* @__PURE__ */ t.jsx(Z, { type: "error", showIcon: !0, style: { marginBottom: 16 }, message: "后端未连接", description: c }),
    /* @__PURE__ */ t.jsx(
      Te,
      {
        rowKey: "key",
        dataSource: s,
        columns: a.length ? a : [{ title: "name", dataIndex: "name" }],
        loading: d && !s.length,
        size: "small",
        pagination: { pageSize: 20 }
      }
    )
  ] });
}
const Ne = [
  { key: "/instance", icon: /* @__PURE__ */ t.jsx(pe, {}), label: "实例与端口" },
  { key: "/topics", icon: /* @__PURE__ */ t.jsx(Ee, {}), label: "Topic 清单" }
], Ce = [
  { path: "instance", Component: he },
  { path: "topics", Component: Se }
];
export {
  he as Instance,
  Se as Topics,
  Le as configureMq,
  Ne as menuItems,
  Ce as routeTable
};
