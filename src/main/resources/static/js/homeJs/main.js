'use strict';


function chartBox(e) {
    var myChart = new Chart(ctx, {
        type: 'line',
        data: {
            //시간
            labels: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul'],
            datasets: [{
                label: '거래량',
                //데이터
                data: [65, 59, 80, 81, 56, 55, 40],
                backgroundColor: 'rgba(255, 99, 132, 0.2)',
                borderColor: 'rgba(255, 99, 132, 1)',
                stack: 'combined',
                type: 'bar'
            },
                {
                    label: '가격',
                    //데이터
                    data: [45, 79, 60, 71, 86, 25, 90],
                    backgroundColor: 'rgba(54, 162, 235, 0.2)',
                    borderColor: 'rgba(54, 162, 235, 1)',
                    stack: 'combined'
                }]
        },
        options: {
            scales: {
                // yAxes: [{
                //     ticks: {
                //         beginAtZero: true
                //     }
                // }]
            }
        }
    });
}