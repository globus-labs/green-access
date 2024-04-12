<script>
import { ref } from 'vue'
export default {
  name: "funcPredictions",
  data() {
    return {
      funcPred: [],
      timerStats: "",
      timerPred: "",
      n_functions: 0,
    };
  },
  created() {
    this.fetchFunctionStats();
    this.timerPred = setInterval(this.fetchFunctionStats, 600000)
  },
  methods: {
    async fetchFunctionStats(){
      const res = await fetch("http://0.0.0.0:8000/functions?username=" + this.$user);
      const all_functions = await res.json();
      this.n_functions = all_functions.length

      const formattedFunctions = [];
      const formattedpredictions = [];
      const functions = all_functions.slice(0,10)

      Object.entries(functions).forEach(([key, value]) => {
        formattedFunctions.push(value);
    });

      //this.funcPred = formattedFunctions.values()
      for (var i = 0; i < formattedFunctions.length; ++i){
        const pred = await fetch("http://127.0.0.1:8000/predict/?function_id=" + formattedFunctions[i].function_id);
        const predictions = await pred.json();

        const value_dict = {}
        value_dict["name"] = formattedFunctions[i].name
        value_dict["function_id"] = formattedFunctions[i].function_id
        const pred_list = []

        Object.entries(predictions.predictions).forEach(([key,value]) => {
            const pred_entry = {}
            pred_entry["endpoint_name"] = value.endpoint_name
            pred_entry["endpoint_id"] = value.endpoint_id
            pred_entry["runtime"] = value.runtime
            pred_entry["energy"] = value.energy
            pred_entry["credit"] = value.credit
            pred_list.push(pred_entry)
        });

        pred_list.sort((a,b) => a.energy - b.energy);
        value_dict["predictions"] = pred_list;
        formattedpredictions.push(value_dict);
      }
      this.funcPred = formattedpredictions


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
    <div class="row px-5">
        <div class="py-3">Detailed Summary</div>
        <div class="col">
            <div id="accordion" role="tablist" aria-multiselectable="true">
                <div class="card" v-for="(pred, index) in funcPred">
                    <h5 class="card-header  float-right" role="tab" id="headingOne">
                        <a data-bs-toggle="collapse" data-parent="#accordion" :href="'#collapse' + index" aria-expanded="true" :aria-controls="'collapse' + index" class="d-block">
                            <span class="text-start func-name text-green">{{ pred.name }} : {{ pred.function_id }}</span>
                             <span class="bi bi-chevron-double-down funccard-symbol text-end"></span>
                        </a>
                    </h5>

                    <div :id="'collapse' + index " class="collapse" role="tabpanel" aria-labelledby="headingOne">
                        <div class="card-body">
                            <table class="table">
                              <thead>
                                <tr>
                                  <th scope="col">Efficiency Ranking</th>
                                  <th scope="col">Site</th>
                                  <th scope="col">Endpoint UUID</th>
                                  <th scope="col">Predicted energy (Joules)</th>
                                  <th scope="col">Predicted runtime (s)</th>
                                  <th scope="col">Predicted allocation usage</th>
                                </tr>
                              </thead>
                              <tbody>
                                <tr v-for="(val, index) in pred.predictions">
                                  <th scope="row">{{ index + 1 }}</th>
                                  <td>{{ val.endpoint_name }}</td>
                                  <td>{{ val.endpoint_id }}</td>
                                  <td>{{ val.energy.toFixed(2) }}</td>
                                  <td>{{ val.runtime.toFixed(2) }}</td>
                                  <td>{{ val.credit.toFixed(2) }}</td>
                                </tr>
   
                              </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>


        </div>
    </div>
 </template>
<style scoped>
.func-name .funccard-symbol{
  display: inline;
}
.func-name{
  float: left
}
.funccard-symbol{
  float: right
}
.text-green{
  color:rgb(1, 75, 1);
  font-weight: normal;
}
.func-heading{
  /* background-color: rgb(253, 253, 246); */
  border-color: rgb(1, 46, 1);
}
a {
  color: inherit; /* blue colors for links too */
  text-decoration: inherit; /* no underline */
}

.card-header .fa {
  transition: .3s transform ease-in-out;
}
.card-header .collapsed .fa {
  transform: rotate(90deg);
}


</style>