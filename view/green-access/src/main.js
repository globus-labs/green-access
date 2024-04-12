import { createApp } from 'vue'
import 'bootstrap-icons/font/bootstrap-icons.css'
import './style.css'
import App from './App.vue'

const app = createApp(App)
app.config.globalProperties.$user = 'user_0'
app.mount('#app')
