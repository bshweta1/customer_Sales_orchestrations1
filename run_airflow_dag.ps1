# ------------------------------------------
# PowerShell Script: Run Airflow DAG & Stream Logs
# ------------------------------------------

# DAG name
$dag_name = "customer_sales_etl_prod"

# Generate a unique run ID
$run_id = "manual_" + (Get-Date -Format "yyyyMMddHHmmss")

Write-Host "Triggering DAG $dag_name with run ID $run_id..." -ForegroundColor Cyan
airflow dags trigger $dag_name --conf '{}' --run-id $run_id

# Give Airflow a few seconds to register the DAG run
Start-Sleep -Seconds 5

# Get all tasks in the DAG
$tasks_output = airflow tasks list $dag_name | Select-Object -Skip 2
$tasks = $tasks_output | ForEach-Object { ($_ -split '\s+')[0] }

Write-Host "`nFound tasks: $($tasks -join ', ')" -ForegroundColor Green

# Function to wait for task to complete
function Wait-TaskComplete($dag, $task, $run_id) {
    while ($true) {
        $state = airflow tasks state $dag $task $run_id
        if ($state -eq "success" -or $state -eq "failed") {
            return $state
        }
        Start-Sleep -Seconds 5
    }
}

# Stream logs for all tasks sequentially
foreach ($task in $tasks) {
    Write-Host "`n=== TASK: $task ===" -ForegroundColor Yellow
    Write-Host "Waiting for task to complete..."
    
    $state = Wait-TaskComplete $dag_name $task $run_id
    Write-Host "Task $task finished with state: $state" -ForegroundColor Green

    Write-Host "Streaming logs..."
    airflow tasks logs $dag_name $task $run_id
}

Write-Host "`nAll tasks finished. DAG run completed." -ForegroundColor Cyan
