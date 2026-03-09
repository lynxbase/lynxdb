#!/usr/bin/env bash
# fault_inject.sh — Network fault injection helpers for chaos testing.
# Requires NET_ADMIN capability and iptables/tc installed.
#
# Usage:
#   ./fault_inject.sh partition_node <container_name>
#   ./fault_inject.sh heal_node <container_name>
#   ./fault_inject.sh add_latency <container_name> <delay_ms>
#   ./fault_inject.sh kill_node <container_name>
#   ./fault_inject.sh restart_node <container_name>

set -euo pipefail

COMPOSE_FILE="$(dirname "$0")/docker-compose.yml"

partition_node() {
    local container="$1"
    echo "Partitioning $container from all peers..."
    docker exec "$container" iptables -A INPUT -p tcp --dport 9400 -j DROP
    docker exec "$container" iptables -A OUTPUT -p tcp --dport 9400 -j DROP
    echo "Done: $container is now isolated."
}

heal_node() {
    local container="$1"
    echo "Healing network for $container..."
    docker exec "$container" iptables -F INPUT 2>/dev/null || true
    docker exec "$container" iptables -F OUTPUT 2>/dev/null || true
    # Remove tc qdisc if present.
    docker exec "$container" tc qdisc del dev eth0 root 2>/dev/null || true
    echo "Done: $container network restored."
}

add_latency() {
    local container="$1"
    local delay_ms="$2"
    echo "Adding ${delay_ms}ms latency to $container..."
    docker exec "$container" tc qdisc add dev eth0 root netem delay "${delay_ms}ms"
    echo "Done: $container has ${delay_ms}ms latency."
}

kill_node() {
    local container="$1"
    echo "Killing $container..."
    docker kill "$container"
    echo "Done: $container killed."
}

restart_node() {
    local container="$1"
    echo "Restarting $container..."
    docker start "$container"
    echo "Done: $container restarted."
}

case "${1:-help}" in
    partition_node) partition_node "$2" ;;
    heal_node)      heal_node "$2" ;;
    add_latency)    add_latency "$2" "$3" ;;
    kill_node)      kill_node "$2" ;;
    restart_node)   restart_node "$2" ;;
    *)
        echo "Usage: $0 {partition_node|heal_node|add_latency|kill_node|restart_node} <args...>"
        exit 1
        ;;
esac
