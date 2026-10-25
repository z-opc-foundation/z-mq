import { j as e, m as w, C as O, M as A } from "./ComponentDoc-DnVYPAdH.js";
import { ReloadOutlined as T, UserOutlined as B, LockOutlined as E, HomeOutlined as L, DashboardOutlined as P, AppstoreOutlined as R, ReadOutlined as N } from "@ant-design/icons";
import { useState as a, useEffect as b } from "react";
import { Typography as u, Space as p, Button as k, Alert as C, Spin as D, Row as q, Col as f, Card as c, Statistic as j, Descriptions as v, Tag as J, Form as x, Input as z, message as I } from "antd";
import { ListTable as M } from "@yuku123/z-frontend-common/ui";
import { useNavigate as W } from "react-router-dom";
const { Title: F } = u;
function H() {
  const [s, r] = a(null), [o, l] = a(!1), [n, h] = a(null), d = async () => {
    l(!0);
    try {
      r(await w.instance()), h(null);
    } catch (i) {
      h((i == null ? void 0 : i.message) || String(i)), r(null);
    } finally {
      l(!1);
    }
  };
  return b(() => {
    d();
    const i = setInterval(d, 15e3);
    return () => clearInterval(i);
  }, []), /* @__PURE__ */ e.jsxs("div", { children: [
    /* @__PURE__ */ e.jsxs(p, { style: { marginBottom: 16 }, children: [
      /* @__PURE__ */ e.jsx(F, { level: 4, style: { margin: 0 }, children: "实例与端口" }),
      /* @__PURE__ */ e.jsx(k, { icon: /* @__PURE__ */ e.jsx(T, {}), onClick: d, loading: o, children: "刷新" }),
      /* @__PURE__ */ e.jsx(u.Text, { type: "secondary", children: "15s 自动刷新" })
    ] }),
    n && /* @__PURE__ */ e.jsx(C, { type: "error", showIcon: !0, style: { marginBottom: 16 }, message: "后端未连接", description: n }),
    o && !s && /* @__PURE__ */ e.jsx(D, {}),
    s && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
      /* @__PURE__ */ e.jsxs(q, { gutter: 16, style: { marginBottom: 16 }, children: [
        /* @__PURE__ */ e.jsx(f, { span: 8, children: /* @__PURE__ */ e.jsx(c, { children: /* @__PURE__ */ e.jsx(j, { title: "nameserver 存活", value: s.nameserverAlive ? "是" : "否" }) }) }),
        /* @__PURE__ */ e.jsx(f, { span: 8, children: /* @__PURE__ */ e.jsx(c, { children: /* @__PURE__ */ e.jsx(j, { title: "broker 存活", value: s.brokerAlive ? "是" : "否" }) }) }),
        /* @__PURE__ */ e.jsx(f, { span: 8, children: /* @__PURE__ */ e.jsx(c, { children: /* @__PURE__ */ e.jsx(
          j,
          {
            title: "bind 状态",
            value: s.bound ? "已 bind" : "未 bind",
            valueStyle: { color: s.bound ? "#3f8600" : "#cf1322" }
          }
        ) }) })
      ] }),
      /* @__PURE__ */ e.jsx(c, { title: "实例详情", children: /* @__PURE__ */ e.jsx(v, { column: 2, bordered: !0, size: "small", children: Object.entries(s).filter(([i]) => !["bindError"].includes(i)).map(([i, t]) => /* @__PURE__ */ e.jsx(v.Item, { label: i, children: typeof t == "object" ? JSON.stringify(t) : String(t) }, i)) }) })
    ] })
  ] });
}
const { Title: K } = u;
function U() {
  const [s, r] = a([]), [o, l] = a(!1), [n, h] = a(null), d = async () => {
    l(!0);
    try {
      const t = await w.topics(), g = Array.isArray(t) ? t : (t == null ? void 0 : t.topics) || [];
      r(g.map((y, S) => typeof y == "string" ? { key: S, name: y } : { key: S, ...y })), h(null);
    } catch (t) {
      h((t == null ? void 0 : t.message) || String(t));
    } finally {
      l(!1);
    }
  };
  b(() => {
    d();
  }, []);
  const i = Object.keys(s[0] || { name: "" }).map((t) => ({
    title: t,
    dataIndex: t,
    key: t,
    ellipsis: !0,
    render: (g) => typeof g == "object" ? JSON.stringify(g) : String(g ?? "—")
  }));
  return /* @__PURE__ */ e.jsxs("div", { children: [
    /* @__PURE__ */ e.jsx(p, { style: { marginBottom: 16 }, children: /* @__PURE__ */ e.jsx(K, { level: 4, style: { margin: 0 }, children: "Topic 清单" }) }),
    n && /* @__PURE__ */ e.jsx(C, { type: "error", showIcon: !0, style: { marginBottom: 16 }, message: "后端未连接", description: n }),
    /* @__PURE__ */ e.jsx(
      M,
      {
        rows: s,
        columns: i.length ? i : [{ title: "name", dataIndex: "name" }],
        rowKey: "key",
        loading: o && !s.length,
        searchKeys: "name",
        searchPlaceholder: "按 topic 名过滤",
        cardExtra: /* @__PURE__ */ e.jsx(k, { icon: /* @__PURE__ */ e.jsx(T, {}), onClick: d, loading: o, children: "刷新" })
      }
    )
  ] });
}
const { Title: $, Paragraph: _ } = u;
function G() {
  const s = W(), [r, o] = a(null);
  b(() => {
    const n = localStorage.getItem("userInfo");
    if (n)
      try {
        o(JSON.parse(n));
      } catch {
        o({ name: n });
      }
  }, []);
  const l = Q.filter((n) => n.key !== "/z-mq/home");
  return /* @__PURE__ */ e.jsxs("div", { children: [
    /* @__PURE__ */ e.jsx(c, { style: { marginBottom: 16, background: "linear-gradient(135deg, #7c3aed 0%, #6d28d9 100%)", border: "none" }, children: /* @__PURE__ */ e.jsxs(p, { direction: "vertical", size: 4, style: { color: "#fff" }, children: [
      /* @__PURE__ */ e.jsxs($, { level: 3, style: { color: "#fff", margin: 0 }, children: [
        "欢迎",
        r != null && r.name ? `，${r.name}` : ""
      ] }),
      /* @__PURE__ */ e.jsx(_, { style: { color: "rgba(255,255,255,0.85)", margin: 0 }, children: "z-mq 消息队列 管理台" }),
      (r == null ? void 0 : r.role) && /* @__PURE__ */ e.jsx(J, { style: { marginTop: 8, background: "rgba(255,255,255,0.2)", color: "#fff", border: "none" }, children: r.role })
    ] }) }),
    /* @__PURE__ */ e.jsx(q, { gutter: [16, 16], children: l.map((n) => /* @__PURE__ */ e.jsx(f, { xs: 24, sm: 12, md: 12, lg: 8, children: /* @__PURE__ */ e.jsx(c, { hoverable: !0, onClick: () => s(n.key), style: { borderTop: "3px solid #7c3aed" }, children: /* @__PURE__ */ e.jsxs(p, { align: "start", size: 12, children: [
      /* @__PURE__ */ e.jsx("div", { style: {
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
      /* @__PURE__ */ e.jsxs("div", { style: { minWidth: 0 }, children: [
        /* @__PURE__ */ e.jsx("div", { style: { fontSize: 15, fontWeight: 600, color: "#0f172a" }, children: n.label }),
        /* @__PURE__ */ e.jsx("div", { style: { fontSize: 12, color: "#94a3b8", marginTop: 2 }, children: n.key })
      ] })
    ] }) }) }, n.key)) })
  ] });
}
const m = { username: "admin", password: "123456", user: { name: "admin", role: "管理员" } };
function ne() {
  const [s, r] = a(!1), o = async (l) => {
    r(!0);
    try {
      l.username === m.username && l.password === m.password ? (localStorage.setItem("token", "mock-" + m.username), localStorage.setItem("userInfo", JSON.stringify(m.user)), I.success("登录成功（mock 模式，待 CTC/SSO 接入）"), window.location.href = "/z-mq/home") : I.error("用户名或密码错误（默认 admin/123456）");
    } finally {
      r(!1);
    }
  };
  return /* @__PURE__ */ e.jsx("div", { style: {
    height: "100vh",
    display: "flex",
    justifyContent: "center",
    alignItems: "center",
    background: "linear-gradient(135deg, #eef2ff 0%, #f5f3ff 50%, #eff6ff 100%)"
  }, children: /* @__PURE__ */ e.jsxs(c, { style: { width: 400, boxShadow: "0 10px 40px rgba(124,58,237,0.12)" }, children: [
    /* @__PURE__ */ e.jsxs("div", { style: { textAlign: "center", marginBottom: 20 }, children: [
      /* @__PURE__ */ e.jsx("div", { style: {
        width: 48,
        height: 48,
        borderRadius: 12,
        margin: "0 auto 12px",
        background: "linear-gradient(135deg, #7c3aed, #6d28d9)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        color: "#fff",
        fontSize: 24,
        fontWeight: 700
      }, children: "z" }),
      /* @__PURE__ */ e.jsx("div", { style: { fontSize: 18, fontWeight: 600, color: "#0f172a" }, children: "z-mq 管理台" })
    ] }),
    /* @__PURE__ */ e.jsxs(x, { onFinish: o, size: "large", children: [
      /* @__PURE__ */ e.jsx(x.Item, { name: "username", rules: [{ required: !0, message: "请输入用户名" }], children: /* @__PURE__ */ e.jsx(z, { prefix: /* @__PURE__ */ e.jsx(B, {}), placeholder: "用户名" }) }),
      /* @__PURE__ */ e.jsx(x.Item, { name: "password", rules: [{ required: !0, message: "请输入密码" }], children: /* @__PURE__ */ e.jsx(z.Password, { prefix: /* @__PURE__ */ e.jsx(E, {}), placeholder: "密码" }) }),
      /* @__PURE__ */ e.jsx(x.Item, { style: { marginBottom: 8 }, children: /* @__PURE__ */ e.jsx(
        k,
        {
          type: "primary",
          htmlType: "submit",
          loading: s,
          block: !0,
          style: { background: "linear-gradient(135deg, #7c3aed 0%, #6d28d9 100%)", border: "none" },
          children: "登 录"
        }
      ) }),
      /* @__PURE__ */ e.jsxs("div", { style: { textAlign: "center", color: "#94a3b8", fontSize: 12 }, children: [
        "Mock 模式：用户名 ",
        /* @__PURE__ */ e.jsx("b", { children: m.username }),
        " / 密码 ",
        /* @__PURE__ */ e.jsx("b", { children: m.password })
      ] })
    ] })
  ] }) });
}
const Q = [
  { key: "/z-mq/home", label: "首页", icon: /* @__PURE__ */ e.jsx(L, {}) },
  { key: "/z-mq/instance", label: "实例与端口", icon: /* @__PURE__ */ e.jsx(P, {}) },
  { key: "/z-mq/topics", label: "Topic 清单", icon: /* @__PURE__ */ e.jsx(R, {}) },
  { key: "/z-mq/docs", label: "组件文档", icon: /* @__PURE__ */ e.jsx(N, {}) }
], re = [
  { path: "/z-mq/home", Component: G },
  { path: "/z-mq/instance", Component: H },
  { path: "/z-mq/topics", Component: U },
  { path: "/z-mq/docs", Component: O },
  { path: "/z-mq/:rest*", Component: A }
];
export {
  O as ComponentDocPage,
  G as HomePage,
  H as Instance,
  ne as LoginPage,
  U as Topics,
  Q as menuItems,
  re as routes
};
