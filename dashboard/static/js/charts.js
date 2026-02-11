// Workload stacked bar chart
let workloadChart = null;

function initWorkloadChart(data) {
  const ctx = document.getElementById('workloadChart');
  if (!ctx) return;

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
}
