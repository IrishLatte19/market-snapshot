# Show available commands without starting API requests.
default:
    @just --list

# Start quietly, then show only pipeline logs. Ctrl+C stops the stack.
run:
    #!/bin/sh
    set -eu
    mkdir -p data logs
    printf '%s\n' 'Preparing the pipeline...'
    # All three application services use this image; build it once, not in parallel.
    if build_output=$(docker compose --progress quiet --profile worker --profile monitoring build --quiet worker 2>&1); then
        :
    else
        printf '%s\n' "$build_output" >&2
        exit 1
    fi
    just links
    exec docker compose --progress quiet --profile worker --profile monitoring up --no-build --quiet-pull --attach worker --attach downstream

# Follow recent collector/processor logs; Ctrl+C only leaves this viewer.
logs:
    @docker compose --profile worker --profile monitoring logs --follow --tail 20 worker downstream

# Include infrastructure logs when debugging setup or service failures.
logs-all:
    @docker compose --profile worker --profile monitoring logs --follow --tail 100

# Stop services without deleting containers, volumes or exported files.
stop:
    @docker compose --profile worker --profile monitoring stop

# Show running services and the completed database initializer.
status:
    @docker compose --profile worker --profile monitoring ps -a

# Print the dashboard links, log legend and output paths.
links:
    @printf '%s\n' \
        'Grafana: http://localhost:3000/d/market-etl' \
        'Prometheus target health: http://localhost:9090/targets' \
        'Logs: worker = collector (fetch, validate, save); downstream = processor (normalize, export).' \
        'Outputs: data/raw/ and data/processed/ | Log file: logs/etl.log' \
        'Allow 30-60 seconds for dashboard samples. Use your saved Grafana login.' \
        'Ctrl+C stops the stack without deleting data. Setup diagnostics: just logs-all.'
