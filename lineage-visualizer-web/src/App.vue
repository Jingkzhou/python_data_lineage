<script setup lang="ts">
import { RouterView } from 'vue-router'
</script>

<template>
  <div class="app-shell">
    <header class="app-shell__header">
      <div>
        <p class="app-shell__eyebrow">python data lineage</p>
        <h1>Lineage Visualizer</h1>
      </div>
    </header>
    <main class="app-shell__main">
      <RouterView />
    </main>
  </div>
</template>

<style scoped>
:global(body) {
  margin: 0;
  font-family: 'Inter', 'SF Pro Display', system-ui, -apple-system, BlinkMacSystemFont, sans-serif;
  background-color: #f4f6fb;
  color: #0f172a;
}

:global(#app) {
  min-height: 100vh;
}

.app-shell {
  min-height: 100vh;
  padding: 2rem clamp(1rem, 5vw, 3.5rem);
  box-sizing: border-box;
}

.app-shell__header {
  margin-bottom: 1.5rem;
}

.app-shell__eyebrow {
  text-transform: uppercase;
  letter-spacing: 0.3em;
  font-size: 0.75rem;
  color: #475569;
  margin-bottom: 0.3rem;
}

.app-shell h1 {
  margin: 0;
  font-size: clamp(1.8rem, 2.5vw, 2.6rem);
}

.app-shell__main {
  min-height: calc(100vh - 5rem);
}
</style>
