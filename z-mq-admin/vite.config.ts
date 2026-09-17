import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import path from 'path'

// Z-MQ Console — Vite 配置
// 默认端口 8081, 代理 /api 到 z-mq-broker-admin (默认 9090)
export default defineConfig({
  plugins: [vue()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, 'src')
    }
  },
  server: {
    port: 8081,
    host: '0.0.0.0',
    open: false,
    proxy: {
      '/api': {
        target: process.env.VITE_ZMQ_ADMIN_URL || 'http://localhost:9090',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, '')
      }
    }
  },
  build: {
    outDir: 'dist',
    sourcemap: false,
    chunkSizeWarningLimit: 1500,
    rollupOptions: {
      output: {
        manualChunks: {
          vue: ['vue', 'vue-router', 'pinia'],
          echarts: ['echarts', 'vue-echarts'],
          element: ['element-plus', '@element-plus/icons-vue']
        }
      }
    }
  }
})