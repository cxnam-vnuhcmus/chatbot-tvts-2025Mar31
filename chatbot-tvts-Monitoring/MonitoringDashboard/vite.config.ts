import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@images': '/src/assets/images',
      '@icons': '/src/assets/icons',
      '@containers': '/src/containers',
      '@components': '/src/components',
      '@services': '/src/services',
      '@model': '/src/models',
      '@shared': '/src/shared',
    }
  }
})
