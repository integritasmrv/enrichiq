"""Simple Flask dashboard for pipeline monitoring."""

import os
import json
from datetime import datetime
from flask import Flask, render_template_string, jsonify, request
from monitoring.db_utils import MonitoringDB, DBConfig


app = Flask(__name__)

@app.template_filter('fmt')
def fmt_filter(value):
    if value is None:
        return '0'
    if isinstance(value, float):
        return f'{value:,.2f}'
    return f'{int(value):,}'

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Pipeline Monitor</title>
    <meta http-equiv="refresh" content="30">
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        h1 { color: #333; }
        h2 { color: #555; margin-top: 30px; }
        .status-planned { background: #fee; }
        .status-running { background: #efe; }
        .status-completed { background: #cfc; }
        .status-failed { background: #fcc; }
        .status-stopped { background: #ccc; }
        .status-paused { background: #ffec8b; }
        .status-skipped { background: #e5e5e5; }
        table { border-collapse: collapse; width: 100%; background: white; margin-bottom: 20px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background: #4CAF50; color: white; }
        tr:hover { background: #f5f5f5; }
        .progress-bar { background: #4CAF50; height: 20px; border-radius: 4px; }
        .progress-bg { background: #ddd; height: 20px; border-radius: 4px; width: 100px; }
        .btn { padding: 5px 10px; margin: 2px; cursor: pointer; }
        .error { color: red; }
        .metric-inserted { color: #2e7d32; font-weight: bold; }
        .metric-updated { color: #1565c0; font-weight: bold; }
        .metric-failed { color: #c62828; }
        .metric-skipped { color: #757575; }
    </style>
</head>
<body>
    <h1>Pipeline Monitor</h1>
    <p>Last updated: {{ last_update }}</p>
    <h2>Pipeline Runs</h2>
    <table>
        <tr>
            <th>Pipeline</th>
            <th>Version</th>
            <th>Source</th>
            <th>Type</th>
            <th>Status</th>
            <th>Progress</th>
            <th>Items</th>
            <th>Server Load</th>
            <th>Started</th>
            <th>Duration</th>
            <th>Actions</th>
        </tr>
        {% for run in runs %}
        <tr class="status-{{ run.status }}">
            <td><a href="/run/{{ run.run_id }}">{{ run.pipeline_name }}</a></td>
            <td>{{ run.pipeline_version }}</td>
            <td>{{ run.source_type }}</td>
            <td>{{ run.run_type }}</td>
            <td>{{ run.status }}</td>
            <td>
                <div class="progress-bg">
                    <div class="progress-bar" style="width: {{ run.progress_percent }}%"></div>
                </div>
                {{ run.progress_percent }}%
            </td>
            <td>{{ run.processed_items|default(0) }} / {{ run.total_items|default(0) }}</td>
            <td>{{ run.server_load_percent|default(0) }}%</td>
            <td>{{ run.started_at|default('-') }}</td>
            <td>{{ run.duration_minutes|default('-') }} min</td>
            <td>
                {% if run.status == 'running' %}
                <form method="POST" style="display:inline;">
                    <button type="submit" name="command" value="stop" class="btn">Stop</button>
                    <button type="submit" name="command" value="pause" class="btn">Pause</button>
                </form>
                {% elif run.status == 'paused' %}
                <form method="POST" style="display:inline;">
                    <button type="submit" name="command" value="resume" class="btn">Resume</button>
                    <button type="submit" name="command" value="stop" class="btn">Stop</button>
                </form>
                {% endif %}
            </td>
        </tr>
        {% endfor %}
    </table>
    <h2>Pipeline Metrics</h2>
    <table>
        <tr>
            <th>Extract Version</th>
            <th>Table</th>
            <th>Operation</th>
            <th>Total Rows</th>
            <th>Inserted</th>
            <th>Updated</th>
            <th>Status</th>
            <th>Recorded At</th>
        </tr>
        {% for metric in metrics %}
        <tr>
            <td>{{ metric.extract_version }}</td>
            <td>{{ metric.table_name }}</td>
            <td>{{ metric.operation }}</td>
            <td>{{ metric.rows_count|default(0)|int|fmt }}</td>
            <td class="metric-inserted">{{ metric.rows_inserted|default(0)|int|fmt }}</td>
            <td class="metric-updated">{{ metric.rows_updated|default(0)|int|fmt }}</td>
            <td class="metric-{{ metric.status }}">{{ metric.status }}</td>
            <td>{{ metric.recorded_at }}</td>
        </tr>
        {% endfor %}
    </table>
    <h2>Summary</h2>
    <ul>
        <li>Planned: {{ status_counts.planned|default(0) }}</li>
        <li>Running: {{ status_counts.running|default(0) }}</li>
        <li>Completed: {{ status_counts.completed|default(0) }}</li>
        <li>Failed: {{ status_counts.failed|default(0) }}</li>
        <li>Stopped: {{ status_counts.stopped|default(0) }}</li>
    </ul>
</body>
</html>
'''


@app.route('/')
def index():
    db = MonitoringDB()
    runs = db.get_all_runs_summary()
    metrics = db.get_all_metrics_summary()
    db.close()

    status_counts = {}
    for run in runs:
        status = run.get('status', 'unknown')
        status_counts[status] = status_counts.get(status, 0) + 1

    return render_template_string(
        HTML_TEMPLATE,
        runs=runs,
        metrics=metrics,
        status_counts=status_counts,
        last_update=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    )


@app.route('/run/<run_id>')
def run_detail(run_id):
    db = MonitoringDB()
    details = db.get_run_details(run_id)
    db.close()
    run = details.get('run', {})
    items = details.get('items', [])
    return render_template_string(RUN_DETAIL_TEMPLATE, run=run, items=items)

RUN_DETAIL_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head><title>Run Detail</title>
<style>
    body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
    h1 { color: #333; } h2 { color: #555; margin-top: 20px; }
    table { border-collapse: collapse; width: 100%; background: white; margin-bottom: 20px; }
    th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
    th { background: #4CAF50; color: white; } tr:hover { background: #f5f5f5; }
    a { text-decoration: none; color: #1565c0; }
    .info { background: white; padding: 15px; border: 1px solid #ddd; margin-bottom: 20px; }
    .info p { margin: 5px 0; }
</style></head>
<body>
    <h1>{{ run.pipeline_name }}</h1>
    <div class="info">
        <p><b>Run ID:</b> {{ run.run_id }}</p>
        <p><b>Status:</b> {{ run.status }}</p>
        <p><b>Version:</b> {{ run.pipeline_version }}</p>
        <p><b>Type:</b> {{ run.run_type }} / {{ run.source_type }}</p>
        <p><b>Started:</b> {{ run.started_at|default('-') }}</p>
        <p><b>Finished:</b> {{ run.finished_at|default('-') }}</p>
        <p><b>Error:</b> {{ run.error_message|default('') }}</p>
    </div>
    <h2>Files / Items ({{ items|length }})</h2>
    <table>
        <tr>
            <th>Type</th><th>Source Path</th><th>Table</th>
            <th>Status</th><th>Progress</th><th>Items</th>
            <th>Started</th><th>Finished</th>
        </tr>
        {% for item in items %}
        <tr>
            <td>{{ item.item_type }}</td>
            <td>{{ item.source_path|default('') }}</td>
            <td>{{ item.table_name|default('') }}</td>
            <td>{{ item.status }}</td>
            <td>{{ item.progress_percent|default(0) }}%</td>
            <td>{{ item.processed_items|default(0) }} / {{ item.total_items|default(0) }}</td>
            <td>{{ item.started_at|default('-') }}</td>
            <td>{{ item.finished_at|default('-') }}</td>
        </tr>
        {% endfor %}
    </table>
    <p><a href="/">&laquo; Back to Dashboard</a></p>
</body>
</html>
'''

@app.route('/api/runs')
def api_runs():
    db = MonitoringDB()
    runs = db.get_all_runs_summary()
    db.close()
    return jsonify(runs)


@app.route('/api/runs/<run_id>')
def api_run_detail(run_id):
    db = MonitoringDB()
    details = db.get_run_details(run_id)
    db.close()
    return jsonify(details)


@app.route('/api/command/<run_id>', methods=['POST'])
def api_command(run_id):
    command = request.form.get('command')
    if command not in ['pause', 'resume', 'stop']:
        return jsonify({'error': 'Invalid command'}), 400
    
    db = MonitoringDB()
    with db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_run_commands (run_id, command)
                VALUES (%s, %s)
            """, (run_id, command))
            conn.commit()
    return jsonify({'success': True, 'command': command})


@app.errorhandler(500)
def internal_error(e):
    import traceback
    return f'<pre>{traceback.format_exc()}</pre>', 500


if __name__ == '__main__':
    port = int(os.getenv('MONITOR_PORT', '8050'))
    app.run(host='0.0.0.0', port=port, debug=True)