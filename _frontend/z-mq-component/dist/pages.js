import { j as e, m as k, M as q } from "./MqApp-cgKWjDQZ.js";
import { ReloadOutlined as v, HomeOutlined as I, DashboardOutlined as C, AppstoreOutlined as O } from "@ant-design/icons";
import { useState as a, useEffect as u } from "react";
import { Typography as x, Space as g, Button as S, Alert as z, Spin as w, Row as T, Col as h, Card as d, Statistic as j, Descriptions as b, Table as A, Tag as B } from "antd";
import { useNavigate as P } from "react-router-dom";
const { Title: E, Paragraph: $ } = x;
function N() {
  const [s, i] = a(null), [o, l] = a(!1), [n, m] = a(null), c = async () => {
    l(!0);
    try {
      i(await k.instance()), m(null);
    } catch (r) {
      m((r == null ? void 0 : r.message) || String(r)), i(null);
    } finally {
      l(!1);
    }
  };
  return u(() => {
    c();
    const r = setInterval(c, 15e3);
    return () => clearInterval(r);
  }, []), /* @__PURE__ */ e.jsxs("div", { children: [
    /* @__PURE__ */ e.jsxs(g, { style: { marginBottom: 16 }, children: [
      /* @__PURE__ */ e.jsx(E, { level: 4, style: { margin: 0 }, children: "实例与端口" }),
      /* @__PURE__ */ e.jsx(S, { icon: /* @__PURE__ */ e.jsx(v, {}), onClick: c, loading: o, children: "刷新" }),
      /* @__PURE__ */ e.jsx(x.Text, { type: "secondary", children: "15s 自动刷新" })
    ] }),
    /* @__PURE__ */ e.jsx($, { type: "secondary", children: "/mq/__instance 自省：自省接口通不通 / 内嵌 broker bind 没上 / 端口属主是谁 / bind 失败原文，四件事拆开答。" }),
    n && /* @__PURE__ */ e.jsx(z, { type: "error", showIcon: !0, style: { marginBottom: 16 }, message: "后端未连接", description: n }),
    o && !s && /* @__PURE__ */ e.jsx(w, {}),
    s && /* @__PURE__ */ e.jsxs(e.Fragment, { children: [
      /* @__PURE__ */ e.jsxs(T, { gutter: 16, style: { marginBottom: 16 }, children: [
        /* @__PURE__ */ e.jsx(h, { span: 8, children: /* @__PURE__ */ e.jsx(d, { children: /* @__PURE__ */ e.jsx(j, { title: "nameserver 存活", value: s.nameserverAlive ? "是" : "否" }) }) }),
        /* @__PURE__ */ e.jsx(h, { span: 8, children: /* @__PURE__ */ e.jsx(d, { children: /* @__PURE__ */ e.jsx(j, { title: "broker 存活", value: s.brokerAlive ? "是" : "否" }) }) }),
        /* @__PURE__ */ e.jsx(h, { span: 8, children: /* @__PURE__ */ e.jsx(d, { children: /* @__PURE__ */ e.jsx(
          j,
          {
            title: "bind 状态",
            value: s.bound ? "已 bind" : "未 bind",
            valueStyle: { color: s.bound ? "#3f8600" : "#cf1322" }
          }
        ) }) })
      ] }),
      /* @__PURE__ */ e.jsx(d, { title: "实例详情", children: /* @__PURE__ */ e.jsx(b, { column: 2, bordered: !0, size: "small", children: Object.entries(s).filter(([r]) => !["bindError"].includes(r)).map(([r, t]) => /* @__PURE__ */ e.jsx(b.Item, { label: r, children: typeof t == "object" ? JSON.stringify(t) : String(t) }, r)) }) })
    ] })
  ] });
}
const { Title: R, Paragraph: D } = x;
function J() {
  const [s, i] = a([]), [o, l] = a(!1), [n, m] = a(null), c = async () => {
    l(!0);
    try {
      const t = await k.topics(), p = Array.isArray(t) ? t : (t == null ? void 0 : t.topics) || [];
      i(p.map((y, f) => typeof y == "string" ? { key: f, name: y } : { key: f, ...y })), m(null);
    } catch (t) {
      m((t == null ? void 0 : t.message) || String(t));
    } finally {
      l(!1);
    }
  };
  u(() => {
    c();
  }, []);
  const r = Object.keys(s[0] || { name: "" }).map((t) => ({
    title: t,
    dataIndex: t,
    key: t,
    ellipsis: !0,
    render: (p) => typeof p == "object" ? JSON.stringify(p) : String(p ?? "—")
  }));
  return /* @__PURE__ */ e.jsxs("div", { children: [
    /* @__PURE__ */ e.jsxs(g, { style: { marginBottom: 16 }, children: [
      /* @__PURE__ */ e.jsx(R, { level: 4, style: { margin: 0 }, children: "Topic 清单" }),
      /* @__PURE__ */ e.jsx(S, { icon: /* @__PURE__ */ e.jsx(v, {}), onClick: c, loading: o, children: "刷新" })
    ] }),
    /* @__PURE__ */ e.jsx(D, { type: "secondary", children: "集群当前所有 topic（/mq/topics，从 broker 侧拉取）。" }),
    n && /* @__PURE__ */ e.jsx(z, { type: "error", showIcon: !0, style: { marginBottom: 16 }, message: "后端未连接", description: n }),
    /* @__PURE__ */ e.jsx(
      A,
      {
        rowKey: "key",
        dataSource: s,
        columns: r.length ? r : [{ title: "name", dataIndex: "name" }],
        loading: o && !s.length,
        size: "small",
        pagination: { pageSize: 20 }
      }
    )
  ] });
}
const { Title: H, Paragraph: L } = x;
function M() {
  const s = P(), [i, o] = a(null);
  u(() => {
    const n = localStorage.getItem("userInfo");
    if (n)
      try {
        o(JSON.parse(n));
      } catch {
        o({ name: n });
      }
  }, []);
  const l = W.filter((n) => n.key !== "/z-mq/home");
  return /* @__PURE__ */ e.jsxs("div", { children: [
    /* @__PURE__ */ e.jsx(d, { style: { marginBottom: 16, background: "linear-gradient(135deg, #7c3aed 0%, #6d28d9 100%)", border: "none" }, children: /* @__PURE__ */ e.jsxs(g, { direction: "vertical", size: 4, style: { color: "#fff" }, children: [
      /* @__PURE__ */ e.jsxs(H, { level: 3, style: { color: "#fff", margin: 0 }, children: [
        "欢迎",
        i != null && i.name ? `，${i.name}` : ""
      ] }),
      /* @__PURE__ */ e.jsx(L, { style: { color: "rgba(255,255,255,0.85)", margin: 0 }, children: "z-mq 消息队列 管理台" }),
      (i == null ? void 0 : i.role) && /* @__PURE__ */ e.jsx(B, { style: { marginTop: 8, background: "rgba(255,255,255,0.2)", color: "#fff", border: "none" }, children: i.role })
    ] }) }),
    /* @__PURE__ */ e.jsx(T, { gutter: [16, 16], children: l.map((n) => /* @__PURE__ */ e.jsx(h, { xs: 24, sm: 12, md: 12, lg: 8, children: /* @__PURE__ */ e.jsx(d, { hoverable: !0, onClick: () => s(n.key), style: { borderTop: "3px solid #7c3aed" }, children: /* @__PURE__ */ e.jsxs(g, { align: "start", size: 12, children: [
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
const W = [
  { key: "/z-mq/home", label: "首页", icon: /* @__PURE__ */ e.jsx(I, {}) },
  { key: "/z-mq/instance", label: "实例与端口", icon: /* @__PURE__ */ e.jsx(C, {}) },
  { key: "/z-mq/topics", label: "Topic 清单", icon: /* @__PURE__ */ e.jsx(O, {}) }
], Q = [
  { path: "/z-mq/home", Component: M },
  { path: "/z-mq/instance", Component: N },
  { path: "/z-mq/topics", Component: J },
  { path: "/z-mq/:rest*", Component: q }
];
export {
  N as Instance,
  J as Topics,
  W as menuItems,
  Q as routes
};
