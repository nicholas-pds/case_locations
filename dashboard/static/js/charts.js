// Workload stacked bar chart
let workloadChart = null;

// Register datalabels plugin globally (if available)
if (typeof ChartDataLabels !== 'undefined') {
  Chart.register(ChartDataLabels);
}

function initWorkloadChart(data) {
  const ctx = document.getElementById('workloadChart');
  if (!ctx) return;

  // Set pace grid columns to match chart day count
  const paceGrid = document.querySelector('.pace-grid');
  if (paceGrid) paceGrid.style.setProperty('--pace-cols', data.labels.length);

  workloadChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: data.labels,
      datasets: [
        {
          label: 'In Production',
          data: data.in_production,
          backgroundColor: '#E74C3C',
          borderRadius: 2,
        },
        {
          label: 'Invoiced',
          data: data.invoiced,
          backgroundColor: '#2196F3',
          borderRadius: 2,
        },
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: 'top',
          labels: {
            usePointStyle: true,
            padding: 20,
            font: { family: 'Inter', size: 12, weight: '500' }
          }
        },
        tooltip: {
          backgroundColor: '#1A1A1A',
          titleFont: { family: 'Inter', size: 13 },
          bodyFont: { family: 'Inter', size: 12 },
          padding: 12,
          cornerRadius: 6,
        },
        datalabels: {
          color: '#FFFFFF',
          font: {
            family: 'Inter',
            size: 12,
            weight: '700',
          },
          anchor: 'center',
          align: 'center',
          display: function(context) {
            return context.dataset.data[context.dataIndex] > 0;
          },
          formatter: function(value) {
            return value;
          },
        }
      },
      scales: {
        x: {
          stacked: true,
          grid: { display: false },
          ticks: { font: { family: 'Inter', size: 11 } }
        },
        y: {
          stacked: true,
          beginAtZero: true,
          grid: { color: '#F0F0F0' },
          ticks: { font: { family: 'Inter', size: 11 } }
        }
      }
    }
  });
}

function updateWorkloadChart(data) {
  if (!workloadChart) return;

  workloadChart.data.labels = data.labels;
  workloadChart.data.datasets[0].data = data.in_production;
  workloadChart.data.datasets[1].data = data.invoiced;
  workloadChart.update('none'); // no animation on update

  // Update pace grid columns on refresh
  const paceGrid = document.querySelector('.pace-grid');
  if (paceGrid) paceGrid.style.setProperty('--pace-cols', data.labels.length);
}
