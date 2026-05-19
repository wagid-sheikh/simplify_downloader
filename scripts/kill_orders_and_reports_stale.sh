PIDS="$(
  ps -axo pid=,ppid=,pgid=,stat=,etime=,command= \
  | egrep 'cron_run_orders_and_reports|orders_sync_run_profiler|run_local_reports_daily_sales|run_local_reports_mtd_same_day_fulfillment|run_local_reports_pending_deliveries|dashboard_downloader|crm_downloader|daily_sales_report|pending_deliveries|mtd_same_day_fulfillment' \
  | grep -v grep \
  | awk '{print $1}'
)"

echo "Target to Kill PIDs:"
echo "$PIDS"

if [ -n "$PIDS" ]; then
  kill -TERM $PIDS 2>/dev/null || true
  sleep 5

  for pid in $PIDS; do
    kill -0 "$pid" 2>/dev/null && kill -KILL "$pid" 2>/dev/null || true
  done
fi

rm -rf ./tmp/cron_heavy_pipelines.lock
rm -rf ./tmp/cron_run_orders_and_reports.lock
find ./tmp -maxdepth 1 -name 'cron_step_attempt.*.log' -type f -delete 2>/dev/null || true

echo "Done. Remaining matching cron/report processes:"
ps -axo pid=,ppid=,pgid=,stat=,etime=,command= \
| egrep 'cron_run_orders_and_reports|orders_sync_run_profiler|run_local_reports_daily_sales|run_local_reports_mtd_same_day_fulfillment|run_local_reports_pending_deliveries|dashboard_downloader|crm_downloader|daily_sales_report|pending_deliveries|mtd_same_day_fulfillment' \
| grep -v grep || true