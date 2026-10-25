import { j as e, m as T, C as q, M as A } from "./ComponentDoc-DnVYPAdH.js";
import { ReloadOutlined as C, UserOutlined as E, LockOutlined as R, HomeOutlined as B, DashboardOutlined as _, AppstoreOutlined as M, ReadOutlined as P } from "@ant-design/icons";
import { useState as a, useEffect as S } from "react";
import { Typography as u, Space as p, Button as k, Alert as O, Spin as K, Row as w, Col as f, Card as c, Statistic as j, Descriptions as I, Tag as D, Form as x, Input as z, message as b } from "antd";
import { ListTable as L } from "@yuku123/z-frontend-common/ui";
import { useNavigate as W } from "react-router-dom";
const { Title: N } = u;
function U() {
  const [s, r] = a(null), [i, l] = a(!1), [n, h] = a(null), d = async () => {
    l(!0);
    try {
      r(await T.instance()), h(null);
    } catch (o) {
      h((o == null ? void 0 : o.message) || String(o)), r(null);
    } finally {
      l(!1);
    }
  };
  return S(() => {
    d();
    const o = setInterval(d, 15e3);
    return () => clearInterval(o);
  }, []), /* @__PURE__ */ e.jsxs("div", { children: [
    /* @__PURE__ */ e.jsxs(p, { style: { marginBottom: 16 }, children: [
      /* @__PURE__ */ e.jsx(N, { level: 4, style: { margin: 0 }, children: "实例与端口" }),
      /* @__PURE__ */ e.jsx(k, { icon: /* @__PURE__ */ e.jsx(C, {}), onClick: d, loading: i, children: "刷新" }),
      /* @__PURE__ */ e.jsx(u.Text, { type: "secondary", children: "15s 自动刷新" })
    ] }),
    n && /* @__PURE__ */ e.jsx(O, { type: "error", showIcon: !0, style: { marginBottom: 16 }, message: "后端未连接", description: n }),
    i && !s && /* @__PURE__ */ e.jsx(K, {}),
    s && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
      /* @__PURE__ */ e.jsxs(w, { gutter: 16, style: { marginBottom: 16 }, children: [
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
      /* @__PURE__ */ e.jsx(c, { title: "实例详情", children: /* @__PURE__ */ e.jsx(I, { column: 2, bordered: !0, size: "small", children: Object.entries(s).filter(([o]) => !["bindError"].includes(o)).map(([o, t]) => /* @__PURE__ */ e.jsx(I.Item, { label: o, children: typeof t == "object" ? JSON.stringify(t) : String(t) }, o)) }) })
    ] })
  ] });
}
const { Title: J } = u;
function V() {
  const [s, r] = a([]), [i, l] = a(!1), [n, h] = a(null), d = async () => {
    l(!0);
    try {
      const t = await T.topics(), g = Array.isArray(t) ? t : (t == null ? void 0 : t.topics) || [];
      r(g.map((y, v) => typeof y == "string" ? { key: v, name: y } : { key: v, ...y })), h(null);
    } catch (t) {
      h((t == null ? void 0 : t.message) || String(t));
    } finally {
      l(!1);
    }
  };
  S(() => {
    d();
  }, []);
  const o = Object.keys(s[0] || { name: "" }).map((t) => ({
    title: t,
    dataIndex: t,
    key: t,
    ellipsis: !0,
    render: (g) => typeof g == "object" ? JSON.stringify(g) : String(g ?? "—")
  }));
  return /* @__PURE__ */ e.jsxs("div", { children: [
    /* @__PURE__ */ e.jsx(p, { style: { marginBottom: 16 }, children: /* @__PURE__ */ e.jsx(J, { level: 4, style: { margin: 0 }, children: "Topic 清单" }) }),
    n && /* @__PURE__ */ e.jsx(O, { type: "error", showIcon: !0, style: { marginBottom: 16 }, message: "后端未连接", description: n }),
    /* @__PURE__ */ e.jsx(
      L,
      {
        rows: s,
        columns: o.length ? o : [{ title: "name", dataIndex: "name" }],
        rowKey: "key",
        loading: i && !s.length,
        searchKeys: "name",
        searchPlaceholder: "按 topic 名过滤",
        cardExtra: /* @__PURE__ */ e.jsx(k, { icon: /* @__PURE__ */ e.jsx(C, {}), onClick: d, loading: i, children: "刷新" })
      }
    )
  ] });
}
const { Title: F, Paragraph: H } = u;
function $() {
  const s = W(), [r, i] = a(null);
  S(() => {
    const n = localStorage.getItem("userInfo");
    if (n)
      try {
        i(JSON.parse(n));
      } catch {
        i({ name: n });
      }
  }, []);
  const l = G.filter((n) => n.key !== "/z-mq/home");
  return /* @__PURE__ */ e.jsxs("div", { children: [
    /* @__PURE__ */ e.jsx(c, { style: { marginBottom: 16, background: "linear-gradient(135deg, #7c3aed 0%, #6d28d9 100%)", border: "none" }, children: /* @__PURE__ */ e.jsxs(p, { direction: "vertical", size: 4, style: { color: "#fff" }, children: [
      /* @__PURE__ */ e.jsxs(F, { level: 3, style: { color: "#fff", margin: 0 }, children: [
        "欢迎",
        r != null && r.name ? `，${r.name}` : ""
      ] }),
      /* @__PURE__ */ e.jsx(H, { style: { color: "rgba(255,255,255,0.85)", margin: 0 }, children: "z-mq 消息队列 管理台" }),
      (r == null ? void 0 : r.role) && /* @__PURE__ */ e.jsx(D, { style: { marginTop: 8, background: "rgba(255,255,255,0.2)", color: "#fff", border: "none" }, children: r.role })
    ] }) }),
    /* @__PURE__ */ e.jsx(w, { gutter: [16, 16], children: l.map((n) => /* @__PURE__ */ e.jsx(f, { xs: 24, sm: 12, md: 12, lg: 8, children: /* @__PURE__ */ e.jsx(c, { hoverable: !0, onClick: () => s(n.key), style: { borderTop: "3px solid #7c3aed" }, children: /* @__PURE__ */ e.jsxs(p, { align: "start", size: 12, children: [
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
const m = {
  username: "",
  password: "",
  user: { name: "", role: "管理员" }
};
function ne() {
  const [s, r] = a(!1), i = async (l) => {
    r(!0);
    try {
      if (!m.username || !m.password) {
        b.error("未配置 mock 账号（VITE_MOCK_USER / VITE_MOCK_PASSWORD）");
        return;
      }
      l.username === m.username && l.password === m.password ? (localStorage.setItem("token", "mock-" + m.username), localStorage.setItem("userInfo", JSON.stringify(m.user)), b.success("登录成功（mock 模式，待 CTC/SSO 接入）"), window.location.href = "/z-mq/home") : b.error("用户名或密码错误");
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
    /* @__PURE__ */ e.jsxs(x, { onFinish: i, size: "large", children: [
      /* @__PURE__ */ e.jsx(x.Item, { name: "username", rules: [{ required: !0, message: "请输入用户名" }], children: /* @__PURE__ */ e.jsx(z, { prefix: /* @__PURE__ */ e.jsx(E, {}), placeholder: "用户名" }) }),
      /* @__PURE__ */ e.jsx(x.Item, { name: "password", rules: [{ required: !0, message: "请输入密码" }], children: /* @__PURE__ */ e.jsx(z.Password, { prefix: /* @__PURE__ */ e.jsx(R, {}), placeholder: "密码" }) }),
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
      /* @__PURE__ */ e.jsx("div", { style: { textAlign: "center", color: "#94a3b8", fontSize: 12 }, children: "Mock 模式：账号由宿主注入（VITE_MOCK_USER / VITE_MOCK_PASSWORD）" })
    ] })
  ] }) });
}
const G = [
  { key: "/z-mq/home", label: "首页", icon: /* @__PURE__ */ e.jsx(B, {}) },
  { key: "/z-mq/instance", label: "实例与端口", icon: /* @__PURE__ */ e.jsx(_, {}) },
  { key: "/z-mq/topics", label: "Topic 清单", icon: /* @__PURE__ */ e.jsx(M, {}) },
  { key: "/z-mq/docs", label: "组件文档", icon: /* @__PURE__ */ e.jsx(P, {}) }
], re = [
  { path: "/z-mq/home", Component: $ },
  { path: "/z-mq/instance", Component: U },
  { path: "/z-mq/topics", Component: V },
  { path: "/z-mq/docs", Component: q },
  { path: "/z-mq/:rest*", Component: A }
];
export {
  q as ComponentDocPage,
  $ as HomePage,
  U as Instance,
  ne as LoginPage,
  V as Topics,
  G as menuItems,
  re as routes
};
