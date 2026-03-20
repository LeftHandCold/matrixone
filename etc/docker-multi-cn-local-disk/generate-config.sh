#!/bin/bash
# Generate configuration files from templates with environment variable substitution

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load config.env if it exists
if [ -f "config.env" ]; then
    echo "Loading configuration from config.env..."
    set -a
    source config.env
    set +a
fi

# Function to get config value with service-specific override
# Usage: get_config SERVICE PREFIX DEFAULT_VALUE
# Example: get_config "CN1" "CN1_" "info" -> checks CN1_LOG_LEVEL, then LOG_LEVEL, then "info"
get_config() {
    local service=$1
    local prefix=$2
    local default=$3
    local var_name=$4
    
    # Check service-specific config first (e.g., CN1_LOG_LEVEL)
    local service_var="${prefix}${var_name}"
    if [ -n "${!service_var:-}" ]; then
        echo "${!service_var}"
        return
    fi
    
    # Fall back to common config (e.g., LOG_LEVEL)
    if [ -n "${!var_name:-}" ]; then
        echo "${!var_name}"
        return
    fi
    
    # Use default
    echo "$default"
}

# Default values (used when no override)
DEFAULT_LOG_LEVEL="info"
DEFAULT_LOG_FORMAT="console"
DEFAULT_LOG_MAX_SIZE="512"
DEFAULT_MEMORY_CACHE="512MB"
DEFAULT_DISK_CACHE="8GB"
DEFAULT_CACHE_DIR="/mo-data/file-service-cache"
DEFAULT_DISABLE_TRACE="true"
DEFAULT_DISABLE_METRIC="false"  # Always enable metrics for dev-up

# Storage mode: "disk" (default) or "minio" (for OBS compatibility testing)
STORAGE_MODE="${STORAGE_MODE:-disk}"

# MinIO / OBS proxy settings (only used when STORAGE_MODE=minio)
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://obs-proxy:9000}"
MINIO_BUCKET="${MINIO_BUCKET:-mo-test}"
MINIO_KEY_PREFIX="${MINIO_KEY_PREFIX:-server/data}"
MINIO_KEY_ID="${MINIO_KEY_ID:-minio}"
MINIO_KEY_SECRET="${MINIO_KEY_SECRET:-minio123}"
# OBS compatibility config (applied to TN only)
OBS_PARALLEL_MODE="${OBS_PARALLEL_MODE:-}"
OBS_GC_DELETE_BATCH="${OBS_GC_DELETE_BATCH:-}"
OBS_GC_DELETE_WORKERS="${OBS_GC_DELETE_WORKERS:-}"
OBS_GC_CACHE_SIZE="${OBS_GC_CACHE_SIZE:-}"

# Function to generate fileservice TOML section
# Args: cache_memory, cache_disk, cache_dir, [parallel_mode]
generate_fileservice() {
    local cache_memory=$1
    local cache_disk=$2
    local cache_dir=$3
    local parallel_mode=${4:-}

    if [ "$STORAGE_MODE" = "minio" ]; then
        cat << FSEOF
[[fileservice]]
name = "LOCAL"
backend = "DISK"

[[fileservice]]
name = "SHARED"
backend = "MINIO"

[fileservice.s3]
bucket = "$MINIO_BUCKET"
endpoint = "$MINIO_ENDPOINT"
key-prefix = "$MINIO_KEY_PREFIX"
key-id = "$MINIO_KEY_ID"
key-secret = "$MINIO_KEY_SECRET"
FSEOF
        if [ -n "$parallel_mode" ]; then
            echo "parallel-mode = \"$parallel_mode\""
        fi
        cat << FSEOF

[fileservice.cache]
memory-capacity = "$cache_memory"
disk-capacity = "$cache_disk"
disk-path = "$cache_dir"

[[fileservice]]
name = "ETL"
backend = "MINIO"

[fileservice.s3]
bucket = "$MINIO_BUCKET"
endpoint = "$MINIO_ENDPOINT"
key-prefix = "server/etl"
key-id = "$MINIO_KEY_ID"
key-secret = "$MINIO_KEY_SECRET"
FSEOF
        if [ -n "$parallel_mode" ]; then
            echo "parallel-mode = \"$parallel_mode\""
        fi
    else
        cat << FSEOF
[[fileservice]]
name = "LOCAL"
backend = "DISK"

[[fileservice]]
name = "SHARED"
backend = "DISK"
data-dir = "/mo-data/shared"

[fileservice.cache]
memory-capacity = "$cache_memory"
disk-capacity = "$cache_disk"
disk-path = "$cache_dir"

[[fileservice]]
name = "ETL"
backend = "DISK-ETL"
FSEOF
    fi
}

# Function to generate TN GC config section (OBS compatibility)
generate_tn_gc_config() {
    local has_gc_config=false
    local gc_lines=""

    if [ -n "$OBS_GC_CACHE_SIZE" ]; then
        gc_lines="${gc_lines}cache-size = ${OBS_GC_CACHE_SIZE}\n"
        has_gc_config=true
    fi
    if [ -n "$OBS_GC_DELETE_BATCH" ]; then
        gc_lines="${gc_lines}gc-delete-batch-size = ${OBS_GC_DELETE_BATCH}\n"
        has_gc_config=true
    fi
    if [ -n "$OBS_GC_DELETE_WORKERS" ]; then
        gc_lines="${gc_lines}gc-delete-worker-num = ${OBS_GC_DELETE_WORKERS}\n"
        has_gc_config=true
    fi

    if [ "$has_gc_config" = true ]; then
        echo ""
        echo "[dn.GCCfg]"
        printf "$gc_lines"
    fi
}

echo "Configuration Summary:"
echo "======================"
echo "Storage Mode:    $STORAGE_MODE"
if [ "$STORAGE_MODE" = "minio" ]; then
    echo "  MinIO Endpoint:  $MINIO_ENDPOINT"
    echo "  MinIO Bucket:    $MINIO_BUCKET"
    if [ -n "$OBS_PARALLEL_MODE" ]; then echo "  parallel-mode:   $OBS_PARALLEL_MODE"; fi
    if [ -n "$OBS_GC_DELETE_BATCH" ]; then echo "  gc-delete-batch: $OBS_GC_DELETE_BATCH"; fi
    if [ -n "$OBS_GC_DELETE_WORKERS" ]; then echo "  gc-delete-workers: $OBS_GC_DELETE_WORKERS"; fi
    if [ -n "$OBS_GC_CACHE_SIZE" ]; then echo "  gc-cache-size:   $OBS_GC_CACHE_SIZE"; fi
fi
echo "Common (applies to all services unless overridden):"
echo "  Log Level:       ${LOG_LEVEL:-$DEFAULT_LOG_LEVEL}"
echo "  Log Format:      ${LOG_FORMAT:-$DEFAULT_LOG_FORMAT}"
echo "  Log Max Size:    ${LOG_MAX_SIZE:-$DEFAULT_LOG_MAX_SIZE} MB"
echo "  Memory Cache:    ${MEMORY_CACHE:-$DEFAULT_MEMORY_CACHE}"
echo "  Disk Cache:      ${DISK_CACHE:-$DEFAULT_DISK_CACHE}"
echo "  Disable Trace:   ${DISABLE_TRACE:-$DEFAULT_DISABLE_TRACE}"
echo "  Disable Metric:  ${DISABLE_METRIC:-$DEFAULT_DISABLE_METRIC}"
echo ""

# Check for service-specific overrides
has_overrides=false
for service in CN1 CN2 PROXY LOG TN; do
    prefix="${service}_"
    log_level=$(get_config "$service" "$prefix" "$DEFAULT_LOG_LEVEL" "LOG_LEVEL")
    log_format=$(get_config "$service" "$prefix" "$DEFAULT_LOG_FORMAT" "LOG_FORMAT")
    memory_cache=$(get_config "$service" "$prefix" "$DEFAULT_MEMORY_CACHE" "MEMORY_CACHE")
    disk_cache=$(get_config "$service" "$prefix" "$DEFAULT_DISK_CACHE" "DISK_CACHE")
    disable_trace=$(get_config "$service" "$prefix" "$DEFAULT_DISABLE_TRACE" "DISABLE_TRACE")
    disable_metric=$(get_config "$service" "$prefix" "$DEFAULT_DISABLE_METRIC" "DISABLE_METRIC")
    
    common_log_level="${LOG_LEVEL:-$DEFAULT_LOG_LEVEL}"
    common_memory_cache="${MEMORY_CACHE:-$DEFAULT_MEMORY_CACHE}"
    common_disk_cache="${DISK_CACHE:-$DEFAULT_DISK_CACHE}"
    common_disable_trace="${DISABLE_TRACE:-$DEFAULT_DISABLE_TRACE}"
    common_disable_metric="${DISABLE_METRIC:-$DEFAULT_DISABLE_METRIC}"
    
    if [ "$log_level" != "$common_log_level" ] || \
       [ "$log_format" != "${LOG_FORMAT:-$DEFAULT_LOG_FORMAT}" ] || \
       [ "$memory_cache" != "$common_memory_cache" ] || \
       [ "$disk_cache" != "$common_disk_cache" ] || \
       [ "$disable_trace" != "$common_disable_trace" ] || \
       [ "$disable_metric" != "$common_disable_metric" ]; then
        if [ "$has_overrides" = false ]; then
            echo "Service-Specific Overrides:"
            has_overrides=true
        fi
        echo "  $service:"
        [ "$log_level" != "$common_log_level" ] && echo "    Log Level: $log_level"
        [ "$log_format" != "${LOG_FORMAT:-$DEFAULT_LOG_FORMAT}" ] && echo "    Log Format: $log_format"
        [ "$memory_cache" != "$common_memory_cache" ] && echo "    Memory Cache: $memory_cache"
        [ "$disk_cache" != "$common_disk_cache" ] && echo "    Disk Cache: $disk_cache"
        [ "$disable_trace" != "$common_disable_trace" ] && echo "    Disable Trace: $disable_trace"
        [ "$disable_metric" != "$common_disable_metric" ] && echo "    Disable Metric: $disable_metric"
    fi
done

if [ "$has_overrides" = false ]; then
    echo "No service-specific overrides (all services use common config)"
fi
echo ""

# Get CN1-specific configs (with fallback to common/default)
CN1_LOG_LEVEL=$(get_config "CN1" "CN1_" "$DEFAULT_LOG_LEVEL" "LOG_LEVEL")
CN1_LOG_FORMAT=$(get_config "CN1" "CN1_" "$DEFAULT_LOG_FORMAT" "LOG_FORMAT")
CN1_LOG_MAX_SIZE=$(get_config "CN1" "CN1_" "$DEFAULT_LOG_MAX_SIZE" "LOG_MAX_SIZE")
CN1_MEMORY_CACHE=$(get_config "CN1" "CN1_" "$DEFAULT_MEMORY_CACHE" "MEMORY_CACHE")
CN1_DISK_CACHE=$(get_config "CN1" "CN1_" "$DEFAULT_DISK_CACHE" "DISK_CACHE")
CN1_CACHE_DIR=$(get_config "CN1" "CN1_" "$DEFAULT_CACHE_DIR" "CACHE_DIR")
CN1_DISABLE_TRACE=$(get_config "CN1" "CN1_" "$DEFAULT_DISABLE_TRACE" "DISABLE_TRACE")
CN1_DISABLE_METRIC=$(get_config "CN1" "CN1_" "$DEFAULT_DISABLE_METRIC" "DISABLE_METRIC")

# Generate CN1 config
cat > cn1.toml << EOF
service-type = "CN"
data-dir = "/mo-data"

[log]
level = "$CN1_LOG_LEVEL"
format = "$CN1_LOG_FORMAT"
max-size = $CN1_LOG_MAX_SIZE
filename = "/logs/cn1.log"

[hakeeper-client]
service-addresses = [
  "mo-log:32001",
]

$(generate_fileservice "$CN1_MEMORY_CACHE" "$CN1_DISK_CACHE" "$CN1_CACHE_DIR" "$OBS_PARALLEL_MODE")

[observability]
disableTrace = $CN1_DISABLE_TRACE
disableMetric = $CN1_DISABLE_METRIC
EOF

# Add metrics config if enabled
if [ "$CN1_DISABLE_METRIC" = "false" ]; then
    cat >> cn1.toml << EOF
enable-metric-to-prom = true
status-port = 7001
EOF
fi

cat >> cn1.toml << EOF

[cn]
uuid = "dd1dccb4-4d3c-41f8-b482-5251dc7a41bf"
port-base = 18100
listen-address = "0.0.0.0:18101"
service-address = "mo-cn1:18101"
service-host = "mo-cn1"
sql-address = "mo-cn1:16001"

[cn.Engine]
type = "distributed-tae"

[cn.frontend]
port = 16001
host = "0.0.0.0"
proxy-enabled = true
EOF

# Get CN2-specific configs
CN2_LOG_LEVEL=$(get_config "CN2" "CN2_" "$DEFAULT_LOG_LEVEL" "LOG_LEVEL")
CN2_LOG_FORMAT=$(get_config "CN2" "CN2_" "$DEFAULT_LOG_FORMAT" "LOG_FORMAT")
CN2_LOG_MAX_SIZE=$(get_config "CN2" "CN2_" "$DEFAULT_LOG_MAX_SIZE" "LOG_MAX_SIZE")
CN2_MEMORY_CACHE=$(get_config "CN2" "CN2_" "$DEFAULT_MEMORY_CACHE" "MEMORY_CACHE")
CN2_DISK_CACHE=$(get_config "CN2" "CN2_" "$DEFAULT_DISK_CACHE" "DISK_CACHE")
CN2_CACHE_DIR=$(get_config "CN2" "CN2_" "$DEFAULT_CACHE_DIR" "CACHE_DIR")
CN2_DISABLE_TRACE=$(get_config "CN2" "CN2_" "$DEFAULT_DISABLE_TRACE" "DISABLE_TRACE")
CN2_DISABLE_METRIC=$(get_config "CN2" "CN2_" "$DEFAULT_DISABLE_METRIC" "DISABLE_METRIC")

# Generate CN2 config
cat > cn2.toml << EOF
service-type = "CN"
data-dir = "/mo-data"

[log]
level = "$CN2_LOG_LEVEL"
format = "$CN2_LOG_FORMAT"
max-size = $CN2_LOG_MAX_SIZE
filename = "/logs/cn2.log"

[hakeeper-client]
service-addresses = [
  "mo-log:32001",
]

$(generate_fileservice "$CN2_MEMORY_CACHE" "$CN2_DISK_CACHE" "$CN2_CACHE_DIR" "$OBS_PARALLEL_MODE")

[observability]
disableTrace = $CN2_DISABLE_TRACE
disableMetric = $CN2_DISABLE_METRIC
EOF

# Add metrics config if enabled
if [ "$CN2_DISABLE_METRIC" = "false" ]; then
    cat >> cn2.toml << EOF
enable-metric-to-prom = true
status-port = 7001
EOF
fi

cat >> cn2.toml << EOF

[cn]
uuid = "dd2dccb4-4d3c-41f8-b482-5251dc7a41be"
port-base = 18200
listen-address = "0.0.0.0:18201"
service-address = "mo-cn2:18201"
service-host = "mo-cn2"
sql-address = "mo-cn2:16002"

[cn.Engine]
type = "distributed-tae"

[cn.frontend]
port = 16002
host = "0.0.0.0"
proxy-enabled = true
EOF

# Get LOG service-specific configs
LOG_LOG_LEVEL=$(get_config "LOG" "LOG_" "$DEFAULT_LOG_LEVEL" "LOG_LEVEL")
LOG_LOG_FORMAT=$(get_config "LOG" "LOG_" "$DEFAULT_LOG_FORMAT" "LOG_FORMAT")
LOG_LOG_MAX_SIZE=$(get_config "LOG" "LOG_" "$DEFAULT_LOG_MAX_SIZE" "LOG_MAX_SIZE")
LOG_MEMORY_CACHE=$(get_config "LOG" "LOG_" "$DEFAULT_MEMORY_CACHE" "MEMORY_CACHE")
LOG_DISK_CACHE=$(get_config "LOG" "LOG_" "$DEFAULT_DISK_CACHE" "DISK_CACHE")
LOG_CACHE_DIR=$(get_config "LOG" "LOG_" "$DEFAULT_CACHE_DIR" "CACHE_DIR")
LOG_DISABLE_METRIC=$(get_config "LOG" "LOG_" "$DEFAULT_DISABLE_METRIC" "DISABLE_METRIC")

# Generate log service config
cat > log.toml << EOF
# service node type, [DN|CN|LOG]
service-type = "LOG"
data-dir = "/mo-data"

[log]
level = "$LOG_LOG_LEVEL"
format = "$LOG_LOG_FORMAT"
max-size = $LOG_LOG_MAX_SIZE
filename = "/logs/logservice.log"

$(STORAGE_MODE=disk generate_fileservice "$LOG_MEMORY_CACHE" "$LOG_DISK_CACHE" "$LOG_CACHE_DIR")

[observability]
EOF

# Add metrics config based on LOG_DISABLE_METRIC
if [ "$LOG_DISABLE_METRIC" = "false" ]; then
    cat >> log.toml << EOF
disableMetric = false
enable-metric-to-prom = true
status-port = 7001
EOF
else
    cat >> log.toml << EOF
disableMetric = true
status-port = 7001
EOF
fi

# Get TN-specific configs
TN_LOG_LEVEL=$(get_config "TN" "TN_" "$DEFAULT_LOG_LEVEL" "LOG_LEVEL")
TN_LOG_FORMAT=$(get_config "TN" "TN_" "$DEFAULT_LOG_FORMAT" "LOG_FORMAT")
TN_LOG_MAX_SIZE=$(get_config "TN" "TN_" "$DEFAULT_LOG_MAX_SIZE" "LOG_MAX_SIZE")
TN_MEMORY_CACHE=$(get_config "TN" "TN_" "$DEFAULT_MEMORY_CACHE" "MEMORY_CACHE")
TN_DISK_CACHE=$(get_config "TN" "TN_" "$DEFAULT_DISK_CACHE" "DISK_CACHE")
TN_CACHE_DIR=$(get_config "TN" "TN_" "$DEFAULT_CACHE_DIR" "CACHE_DIR")
TN_DISABLE_TRACE=$(get_config "TN" "TN_" "$DEFAULT_DISABLE_TRACE" "DISABLE_TRACE")
TN_DISABLE_METRIC=$(get_config "TN" "TN_" "$DEFAULT_DISABLE_METRIC" "DISABLE_METRIC")

# Generate TN config
cat > tn.toml << EOF
service-type = "DN"
data-dir = "/mo-data"

[log]
level = "$TN_LOG_LEVEL"
format = "$TN_LOG_FORMAT"
max-size = $TN_LOG_MAX_SIZE
filename = "/logs/tn.log"

[hakeeper-client]
service-addresses = [
  "mo-log:32001",
]

$(generate_fileservice "$TN_MEMORY_CACHE" "$TN_DISK_CACHE" "$TN_CACHE_DIR" "$OBS_PARALLEL_MODE")

[observability]
disableTrace = $TN_DISABLE_TRACE
disableMetric = $TN_DISABLE_METRIC
EOF

# Add metrics config if enabled
if [ "$TN_DISABLE_METRIC" = "false" ]; then
    cat >> tn.toml << EOF
enable-metric-to-prom = true
status-port = 7001
EOF
fi

cat >> tn.toml << EOF

[dn]
uuid = "dd4dccb4-4d3c-41f8-b482-5251dc7a41bd"
port-base = 19100
service-address = "mo-tn:19100"
service-host = "mo-tn"

[dn.Txn.Storage]
backend = "TAE"
log-backend = "logservice"

[dn.Ckp]
flush-interval = "60s"
min-count = 100
scan-interval = "5s"
incremental-interval = "60s"
global-interval = "100000s"
EOF

# Add OBS compatibility GC config to TN (only in minio mode)
if [ "$STORAGE_MODE" = "minio" ]; then
    generate_tn_gc_config >> tn.toml
fi

# Get PROXY-specific configs
PROXY_LOG_LEVEL=$(get_config "PROXY" "PROXY_" "$DEFAULT_LOG_LEVEL" "LOG_LEVEL")
PROXY_LOG_FORMAT=$(get_config "PROXY" "PROXY_" "$DEFAULT_LOG_FORMAT" "LOG_FORMAT")
PROXY_LOG_MAX_SIZE=$(get_config "PROXY" "PROXY_" "$DEFAULT_LOG_MAX_SIZE" "LOG_MAX_SIZE")
PROXY_MEMORY_CACHE=$(get_config "PROXY" "PROXY_" "$DEFAULT_MEMORY_CACHE" "MEMORY_CACHE")
PROXY_DISK_CACHE=$(get_config "PROXY" "PROXY_" "$DEFAULT_DISK_CACHE" "DISK_CACHE")
PROXY_CACHE_DIR=$(get_config "PROXY" "PROXY_" "$DEFAULT_CACHE_DIR" "CACHE_DIR")
PROXY_DISABLE_TRACE=$(get_config "PROXY" "PROXY_" "$DEFAULT_DISABLE_TRACE" "DISABLE_TRACE")
PROXY_DISABLE_METRIC=$(get_config "PROXY" "PROXY_" "$DEFAULT_DISABLE_METRIC" "DISABLE_METRIC")

# Generate proxy config  
cat > proxy.toml << EOF
service-type = "PROXY"
data-dir = "/mo-data"

[log]
level = "$PROXY_LOG_LEVEL"
format = "$PROXY_LOG_FORMAT"
max-size = $PROXY_LOG_MAX_SIZE
filename = "/logs/proxy.log"

[hakeeper-client]
service-addresses = [
  "mo-log:32001",
]

$(generate_fileservice "$PROXY_MEMORY_CACHE" "$PROXY_DISK_CACHE" "$PROXY_CACHE_DIR" "$OBS_PARALLEL_MODE")

[observability]
disableTrace = $PROXY_DISABLE_TRACE
disableMetric = $PROXY_DISABLE_METRIC
EOF

# Add metrics config if enabled
if [ "$PROXY_DISABLE_METRIC" = "false" ]; then
    cat >> proxy.toml << EOF
enable-metric-to-prom = true
status-port = 7001
EOF
fi

cat >> proxy.toml << EOF

[proxy]
uuid = "rd1dccb4-4d3c-41f8-b482-5251dc7a41bf"
listen-address = "0.0.0.0:6009"

[proxy.cluster]
refresh-interval = "5s"
EOF

echo "✓ Configuration files generated successfully"
echo ""
echo "Generated files:"
echo "  - cn1.toml"
echo "  - cn2.toml"
echo "  - log.toml"
echo "  - tn.toml"
echo "  - proxy.toml"

