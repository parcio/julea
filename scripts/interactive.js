function(el) {
  N=%.0f
  el.on('plotly_legendclick', function(d) { 
    // store original configuration
    if(!el.mystore) {
      el.mystore = {}
      el.mystore.autorange = true
      el.mystore.labels = JSON.parse(JSON.stringify(d.layout.xaxis2.categoryarray))
      el.mystore.labels0 = JSON.parse(JSON.stringify(d.layout.xaxis.categoryarray))
    }
    // get plotting node
    node = d.node;
    while(!node.id.startsWith('htmlwidget-')) {
      node = node.parentElement;
    }

    // check if hiding or showing thing
    dir = d.node.style.opacity > 0.5 ? -1 : 1
    layout = {}
    console.log('click', d,layout)

    // click at time/operation plot
    if (d.curveNumber < N) { 
      // modify x-axis range
      range = d.layout.xaxis2.range
      range[1] += dir
      layout["xaxis2.range"] = range,

      // modify x-axis label
      label = el.mystore.labels[d.curveNumber]
      categoryarray = d.layout.xaxis2.categoryarray
      if (dir < 1) { // removeLabel
        categoryarray.splice(categoryarray.indexOf(label), 1)
      } else { // add label
        i = 0
        while(i < categoryarray.length && el.mystore.labels.indexOf(categoryarray[i]) < d.curveNumber) { i += 1}
        categoryarray.splice(i, 0, label)
      }
      layout["xaxis2.categoryarray"] = categoryarray

      if (el.mystore.autorange) { // adapt y axis for plot
        // time/backendPlot
        max = 0
        categoryarray.forEach(category => {
          for (j = N; j < d.data.length; j += 1) {
            idx = d.data[j].x.indexOf(category)
            y = d.data[j].y[idx] + d.data[j].base[idx]
            max = Math.max(max, y)
          }
        })
        layout["yaxis2.range"] = [-max*0.01, max*1.01]

        // time/operationPlot
        max = 0
        for (j = 0; j < N; j += 1) {
          if (categoryarray.indexOf(d.data[j].name) == -1) { continue; }
          idx = 0
          d.layout.xaxis.categoryarray.forEach(category => {
            while(el.mystore.labels0[idx] != category) { idx += 1 }
            y = d.data[j].y[idx]
            max = Math.max(max, y)
          })
        }
        layout["yaxis.range"] = [-max*0.01, max*1.01]
      }

      Plotly.update(node, {"visible": dir > 0 ? true : "legendonly"}, layout, [d.curveNumber])

    } else { // click in time/backend plot
      // modify x-axis range
      range = d.layout.xaxis.range
      range[1] += dir
      layout["xaxis.range"] =  range,
      
      // modify x-axis labels
      label = el.mystore.labels0[d.curveNumber - N]
      console.log("label", label)
      categoryarray = d.layout.xaxis.categoryarray
      if (dir < 1) {
        categoryarray.splice(categoryarray.indexOf(label), 1)
      } else {
        i = 0 
        while(i < categoryarray.length && el.mystore.labels0.indexOf(categoryarray[i]) < d.curveNumber - N) { i+= 1}
        categoryarray.splice(i, 0, label)
      }
      layout["xaxis.categoryarray"] =  categoryarray
      console.log(categoryarray)

      basis = []
      mod = []
      // move base of bar, to avoid gaps in stacked barPlot
      for(i = d.curveNumber; i < d.data.length; i += 1) {
        mod.push(i)
        basis.push(d.data[i].base.map(
          (x, j) => x + dir * d.data[d.curveNumber].y[j]))
      }

      if (el.mystore.autorange) { // adapt y-axis forPlot
        max = 0
        for(i = 0; i < d.data.length; i += 1) {
          visible = d.fullData[i].visible != "legendonly" && i != d.curveNumber
          for(j = 0; j < d.layout.xaxis2.categoryarray.length; j += 1) {
            if(visible && d.layout.xaxis2.categoryarray.indexOf(d.data[i].x[j]) != -1) {
              if (i >= d.curveNumber) {
                max = Math.max(max, basis[i-d.curveNumber][j] + d.data[i].y[j])
              } else {
                max = Math.max(max, d.data[i].base[j] + d.data[i].y[j])
              }
            }
          }
        }
        layout["yaxis2.range"] = [-max*0.01, max*1.01]
        max = 0
        for(i = 0; i < categoryarray.length; i += 1) {
          for (j = 0; j < N; j += 1) {
            if (d.layout.xaxis2.categoryarray.indexOf(d.data[j].name) == -1) { continue; }
            idx = d.data[j].x.indexOf(categoryarray[i])
            y = d.data[j].y[idx]
            max = Math.max(max, y)
          }
        }
        layout["yaxis.range"] = [-max*0.01, max*1.01]
      }
      Plotly.update(node, {base: basis}, layout, mod)
      Plotly.restyle(node, {visible: dir > 0 ? true : "legendonly"}, [d.curveNumber])
    }
    return false;
  });
}
