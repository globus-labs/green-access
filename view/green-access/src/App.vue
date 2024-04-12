<script>
import Tasks from './components/Tasks.vue'

export default {
  name: "totalStats",
  components: {
    Tasks: Tasks
  },
  data() {
    return {
      credits: {},
      credits_rem: 0,
      credits_used: 0,
      energy: 0,
      j_completed: 0,
      j_running: 0,
    };
  },
  created() {
    this.fetchStats();
    this.timerStats = setInterval(this.fetchStats, 600000);
  },
  methods: {
    async fetchStats() {
      const res = await fetch("http://127.0.0.1:8000/allocation/?username=" + this.$user);
      const data = await res.json();
      this.credits = data;
      this.credits_rem = data["credits_remaining"].toFixed(2)
      this.credits_used = data["credits_consumed"].toFixed(2)
      this.energy = data["energy_consumed"].toFixed(2)
      this.j_completed = data["jobs_completed"].toFixed(2)
      this.j_running = data["jobs_running"].toFixed(2)
    },
    cancelAutoUpdate() {
      clearInterval(this.timer);
    },
  },
  beforeDestroy() {
    this.cancelAutoUpdate();
  },
};
</script>

<template>
  <div class="row">
    <div class="col d-flex justify-content-center">
      <div class="card summary-main mt-3">
        <div class="card-body">
          <h5>Usage Summary</h5>
            <div class="d-flex flex-row justify-content-center">
              <div id="summary-credits" class="card mx-1 summary h-100">
                <div class="card-header">Carbon Allocation</div>
                <div class="card-body">
                    <div>Used: {{ credits_used }}</div>
                    <div>Remaining: {{ credits_rem }}</div>
                </div>
              </div>
              <div id="summary-energy" class="card mx-1 summary energy">
                <div class="card-header">Energy</div>
                <div class="card-body">
                    <div>Consumed (kWh): {{ credits_used }}</div>
                </div>
              </div>
              <div id="summary-jobs" class="card mx-1 summary h-100">
                <div class="card-header">Jobs</div>
                <div class="card-body">
                    <div>Running: {{ j_running }}</div>
                    <div> Completed: {{ j_completed }}</div>
                </div>
              </div>
            </div>
        </div>
      </div>
    </div>
  </div>
  <Tasks />
</template>

<style scoped>
.energy{
  height: 100;
}
.summary-main{
  width: 42em;
  border: none;
  /* border-width: medium; */
  /* border-color: black; */
}
.summary{
  width: 16em;
  border-color: green;
}
/* #summary-credits{
  border-color: rgb(248, 70, 180);
}
#summary-energy{
  border-color: rgb(64, 104, 4);
}
#summary-jobs{
  border-color: rgb(3, 136, 251);
} */
</style>
