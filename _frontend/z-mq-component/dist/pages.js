import { j as e, m as z, C as w, M as q } from "./ComponentDoc-D-MHGc6k.js";
import { ReloadOutlined as T, UserOutlined as A, LockOutlined as E, HomeOutlined as R, DashboardOutlined as B, AppstoreOutlined as _, ReadOutlined as M } from "@ant-design/icons";
import { useState as c, useEffect as b } from "react";
import { Typography as p, Space as f, Button as S, Alert as C, Spin as P, Row as O, Col as x, Card as d, Statistic as y, Descriptions as v, Tag as K, Form as g, Input as I, message as j } from "antd";
import { ListTable as D } from "@yuku123/z-frontend-common/ui";
import { useNavigate as L } from "react-router-dom";
const { Title: W } = p;
function N() {
  const [s, t] = c(null), [i, l] = c(!1), [n, a] = c(null), m = async () => {
    l(!0);
    try {
      t(await z.instance()), a(null);
    } catch (o) {
      a((o == null ? void 0 : o.message) || String(o)), t(null);
    } finally {
      l(!1);
    }
  };
  return b(() => {
    m();
    const o = setInterval(m, 15e3);
    return () => clearInterval(o);
  }, []), /* @__PURE__ */ e.jsxs("div", { children: [
    /* @__PURE__ */ e.jsxs(f, { style: { marginBottom: 16 }, children: [
      /* @__PURE__ */ e.jsx(W, { level: 4, style: { margin: 0 }, children: "实例与端口" }),
      /* @__PURE__ */ e.jsx(S, { icon: /* @__PURE__ */ e.jsx(T, {}), onClick: m, loading: i, children: "刷新" }),
      /* @__PURE__ */ e.jsx(p.Text, { type: "secondary", children: "15s 自动刷新" })
    ] }),
    n && /* @__PURE__ */ e.jsx(C, { type: "error", showIcon: !0, style: { marginBottom: 16 }, message: "后端未连接", description: n }),
    i && !s && /* @__PURE__ */ e.jsx(P, {}),
    s && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
      /* @__PURE__ */ e.jsxs(O, { gutter: 16, style: { marginBottom: 16 }, children: [
        /* @__PURE__ */ e.jsx(x, { span: 8, children: /* @__PURE__ */ e.jsx(d, { children: /* @__PURE__ */ e.jsx(y, { title: "nameserver 存活", value: s.nameserverAlive ? "是" : "否" }) }) }),
        /* @__PURE__ */ e.jsx(x, { span: 8, children: /* @__PURE__ */ e.jsx(d, { children: /* @__PURE__ */ e.jsx(y, { title: "broker 存活", value: s.brokerAlive ? "是" : "否" }) }) }),
        /* @__PURE__ */ e.jsx(x, { span: 8, children: /* @__PURE__ */ e.jsx(d, { children: /* @__PURE__ */ e.jsx(
          y,
          {
            title: "bind 状态",
            value: s.bound ? "已 bind" : "未 bind",
            valueStyle: { color: s.bound ? "#3f8600" : "#cf1322" }
          }
        ) }) })
      ] }),
      /* @__PURE__ */ e.jsx(d, { title: "实例详情", children: /* @__PURE__ */ e.jsx(v, { column: 2, bordered: !0, size: "small", children: Object.entries(s).filter(([o]) => !["bindError"].includes(o)).map(([o, r]) => /* @__PURE__ */ e.jsx(v.Item, { label: o, children: typeof r == "object" ? JSON.stringify(r) : String(r) }, o)) }) })
    ] })
  ] });
}
const { Title: U } = p;
function J() {
  const [s, t] = c([]), [i, l] = c(!1), [n, a] = c(null), m = async () => {
    l(!0);
    try {
      const r = await z.topics(), h = Array.isArray(r) ? r : (r == null ? void 0 : r.topics) || [];
      t(h.map((u, k) => typeof u == "string" ? { key: k, name: u } : { key: k, ...u })), a(null);
    } catch (r) {
      a((r == null ? void 0 : r.message) || String(r));
    } finally {
      l(!1);
    }
  };
  b(() => {
    m();
  }, []);
  const o = Object.keys(s[0] || { name: "" }).map((r) => ({
    title: r,
    dataIndex: r,
    key: r,
    ellipsis: !0,
    render: (h) => typeof h == "object" ? JSON.stringify(h) : String(h ?? "—")
  }));
  return /* @__PURE__ */ e.jsxs("div", { children: [
    /* @__PURE__ */ e.jsx(f, { style: { marginBottom: 16 }, children: /* @__PURE__ */ e.jsx(U, { level: 4, style: { margin: 0 }, children: "Topic 清单" }) }),
    n && /* @__PURE__ */ e.jsx(C, { type: "error", showIcon: !0, style: { marginBottom: 16 }, message: "后端未连接", description: n }),
    /* @__PURE__ */ e.jsx(
      D,
      {
        rows: s,
        columns: o.length ? o : [{ title: "name", dataIndex: "name" }],
        rowKey: "key",
        loading: i && !s.length,
        searchKeys: "name",
        searchPlaceholder: "按 topic 名过滤",
        cardExtra: /* @__PURE__ */ e.jsx(S, { icon: /* @__PURE__ */ e.jsx(T, {}), onClick: m, loading: i, children: "刷新" })
      }
    )
  ] });
}
const { Title: V, Paragraph: F } = p;
function H() {
  const s = L(), [t, i] = c(null);
  b(() => {
    const n = localStorage.getItem("userInfo");
    if (n)
      try {
        i(JSON.parse(n));
      } catch {
        i({ name: n });
      }
  }, []);
  const l = $.filter((n) => n.key !== "/z-mq/home");
  return /* @__PURE__ */ e.jsxs("div", { children: [
    /* @__PURE__ */ e.jsx(d, { style: { marginBottom: 16, background: "linear-gradient(135deg, #7c3aed 0%, #6d28d9 100%)", border: "none" }, children: /* @__PURE__ */ e.jsxs(f, { direction: "vertical", size: 4, style: { color: "#fff" }, children: [
      /* @__PURE__ */ e.jsxs(V, { level: 3, style: { color: "#fff", margin: 0 }, children: [
        "欢迎",
        t != null && t.name ? `，${t.name}` : ""
      ] }),
      /* @__PURE__ */ e.jsx(F, { style: { color: "rgba(255,255,255,0.85)", margin: 0 }, children: "z-mq 消息队列 管理台" }),
      (t == null ? void 0 : t.role) && /* @__PURE__ */ e.jsx(K, { style: { marginTop: 8, background: "rgba(255,255,255,0.2)", color: "#fff", border: "none" }, children: t.role })
    ] }) }),
    /* @__PURE__ */ e.jsx(O, { gutter: [16, 16], children: l.map((n) => /* @__PURE__ */ e.jsx(x, { xs: 24, sm: 12, md: 12, lg: 8, children: /* @__PURE__ */ e.jsx(d, { hoverable: !0, onClick: () => s(n.key), style: { borderTop: "3px solid #7c3aed" }, children: /* @__PURE__ */ e.jsxs(f, { align: "start", size: 12, children: [
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
        n.desc && /* @__PURE__ */ e.jsx("div", { style: { fontSize: 12, color: "#94a3b8", marginTop: 2 }, children: n.desc })
      ] })
    ] }) }) }, n.key)) })
  ] });
}
function te({ mockAuth: s } = {}) {
  const t = s || {
    username: "",
    password: "",
    user: { name: "", role: "管理员" }
  }, [i, l] = c(!1), n = async (a) => {
    l(!0);
    try {
      if (!t.username || !t.password) {
        j.error("未配置 mock 账号（VITE_MOCK_USER / VITE_MOCK_PASSWORD）");
        return;
      }
      a.username === t.username && a.password === t.password ? (localStorage.setItem("token", "mock-" + t.username), localStorage.setItem("userInfo", JSON.stringify(t.user)), j.success("登录成功（mock 模式，待 CTC/SSO 接入）"), window.location.href = "/z-mq/home") : j.error("用户名或密码错误");
    } finally {
      l(!1);
    }
  };
  return /* @__PURE__ */ e.jsx("div", { style: {
    height: "100vh",
    display: "flex",
    justifyContent: "center",
    alignItems: "center",
    background: "linear-gradient(135deg, #eef2ff 0%, #f5f3ff 50%, #eff6ff 100%)"
  }, children: /* @__PURE__ */ e.jsxs(d, { style: { width: 400, boxShadow: "0 10px 40px rgba(124,58,237,0.12)" }, children: [
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
    /* @__PURE__ */ e.jsxs(g, { onFinish: n, size: "large", children: [
      /* @__PURE__ */ e.jsx(g.Item, { name: "username", rules: [{ required: !0, message: "请输入用户名" }], children: /* @__PURE__ */ e.jsx(I, { prefix: /* @__PURE__ */ e.jsx(A, {}), placeholder: "用户名" }) }),
      /* @__PURE__ */ e.jsx(g.Item, { name: "password", rules: [{ required: !0, message: "请输入密码" }], children: /* @__PURE__ */ e.jsx(I.Password, { prefix: /* @__PURE__ */ e.jsx(E, {}), placeholder: "密码" }) }),
      /* @__PURE__ */ e.jsx(g.Item, { style: { marginBottom: 8 }, children: /* @__PURE__ */ e.jsx(
        S,
        {
          type: "primary",
          htmlType: "submit",
          loading: i,
          block: !0,
          style: { background: "linear-gradient(135deg, #7c3aed 0%, #6d28d9 100%)", border: "none" },
          children: "登 录"
        }
      ) }),
      /* @__PURE__ */ e.jsx("div", { style: { textAlign: "center", color: "#94a3b8", fontSize: 12 }, children: "Mock 模式：账号由宿主注入（VITE_MOCK_USER / VITE_MOCK_PASSWORD）" })
    ] })
  ] }) });
}
const $ = [
  { key: "/z-mq/home", label: "首页", icon: /* @__PURE__ */ e.jsx(R, {}) },
  { key: "/z-mq/instance", label: "实例与端口", icon: /* @__PURE__ */ e.jsx(B, {}) },
  { key: "/z-mq/topics", label: "Topic 清单", icon: /* @__PURE__ */ e.jsx(_, {}) },
  { key: "/z-mq/docs", label: "组件文档", icon: /* @__PURE__ */ e.jsx(M, {}) }
], ne = [
  { path: "/z-mq/home", Component: H },
  { path: "/z-mq/instance", Component: N },
  { path: "/z-mq/topics", Component: J },
  { path: "/z-mq/docs", Component: w },
  { path: "/z-mq/:rest*", Component: q }
];
export {
  w as ComponentDocPage,
  H as HomePage,
  N as Instance,
  te as LoginPage,
  J as Topics,
  $ as menuItems,
  ne as routes
};
