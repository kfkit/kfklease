#!/usr/bin/env bash
# The chart's tests: that it lints, what it renders by default, what each
# value changes and what it refuses to render. `make chart-test` runs this,
# and so does CI. Helm when installed, the alpine/helm image otherwise.
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=../lib.sh
source ../lib.sh

# The values no release renders without, each written once: a check that
# needs one of them missing leaves its group out.
site=(--set site=s)
target=(--set scaledObject.targetRef.name=w)
rest=(--set brokers=k:9092 --set topic=t)

# render <helm template arguments...> is the one helm template command line.
render() {
  helm_run template t "$CHART" "$@"
}

# t <values...> renders a release with the required values and <values> on
# top; dep and so render its Deployment and its ScaledObject alone.
t() {
  render "${site[@]}" "${rest[@]}" "${target[@]}" "$@"
}
dep() {
  t "$@" -s templates/deployment.yaml
}
so() {
  t "$@" -s templates/scaledobject.yaml
}

out=""    # the render the checks look at
checks=0  # how many passed

passed() {
  checks=$(( checks + 1 ))
}

# flunk <message...> ends the run, with the render the check looked at.
flunk() {
  [[ -z $out ]] || echo "$out" >&2
  fail "$@" || exit
}

# has <text>: a line of the render contains <text>.
has() {
  grep -qF -- "$1" <<<"$out" || flunk "missing: $1"
  passed
}

# has_line <line>: the render has this very line, indentation included.
has_line() {
  grep -qxF -- "$1" <<<"$out" || flunk "missing line: $1"
  passed
}

# lacks <regex> <what is wrong when it is there>
lacks() {
  ! grep -qE -- "$1" <<<"$out" || flunk "$2"
  passed
}

# counts <n> <regex> <what is wrong otherwise>: <n> lines match.
counts() {
  [[ $(grep -cE -- "$2" <<<"$out") == "$1" ]] || flunk "$3"
  passed
}

# val <name> <value>: the environment variable <name> is <value>.
val() {
  grep -qF "value: \"$2\"" <<<"$(grep -A1 "name: $1\$" <<<"$out")" || flunk "$1 is not $2"
  passed
}

# refuses <render command...>: helm must fail, and say why.
refuses() {
  local err
  out=""
  if err=$("$@" 2>&1 >/dev/null); then flunk "rendered: $*"; fi
  [[ $err == *Error:* ]] || flunk "not an error of helm's: ${err}"
  err=${err##*Error: }
  echo "   refused: ${err%%$'\n'*}"
  passed
}

lints_and_renders() {
  helm_run lint "$CHART" "${site[@]}" "${rest[@]}" "${target[@]}"
  passed
  t >/dev/null
  passed
}

n_replicas_of_any_kind() {
  out=$(so --set scaledObject.targetRef.apiVersion=kafka.strimzi.io/v1beta2 \
    --set scaledObject.targetRef.kind=KafkaConnect --set scaledObject.targetRef.name=c \
    --set scaledObject.replicas=3)
  local want
  for want in 'apiVersion: kafka.strimzi.io/v1beta2' 'kind: KafkaConnect' 'idleReplicaCount: 0' \
              'minReplicaCount: 3' 'maxReplicaCount: 3'; do
    has "$want"
  done
  # The old value fails loudly.
  refuses render "${site[@]}" "${rest[@]}" --set scaledObject.target=w
  refuses t --set scaledObject.replicas=0
}

fails_closed() {
  out=$(so)
  grep -qx '    replicas: 0' <<<"$(grep -A2 '^  fallback:' <<<"$out")" || flunk "no fallback to 0 replicas"
  passed
  out=$(so --set scaledObject.fallback.enabled=false)
  lacks 'fallback:' "fallback rendered although disabled"
}

health_gate() {
  out=$(dep)
  lacks KFKLEASE_HEALTH_ "health gate rendered without a URL"
  out=$(dep --set healthGate.url=http://c:8083/health --set healthGate.failAfter=2m)
  local name
  for name in URL INTERVAL TIMEOUT FAIL_AFTER COOLDOWN; do
    has "name: KFKLEASE_HEALTH_$name"
  done
  val KFKLEASE_HEALTH_FAIL_AFTER 2m
}

topic_settings() {
  out=$(dep)
  val KFKLEASE_REPLICATION_FACTOR 3
  val KFKLEASE_MIN_INSYNC_REPLICAS 0
  val KFKLEASE_FLUSH_EVERY_RECORD true
  out=$(dep --set replicationFactor=1 --set minInsyncReplicas=1 --set flushEveryRecord=false)
  val KFKLEASE_REPLICATION_FACTOR 1
  val KFKLEASE_MIN_INSYNC_REPLICAS 1
  val KFKLEASE_FLUSH_EVERY_RECORD false
}

pod_settings() {
  local bad want selector
  out=$(dep)
  for bad in imagePullSecrets priorityClassName securityContext; do
    lacks "$bad" "$bad rendered by default"
  done
  out=$(dep --set 'imagePullSecrets[0].name=regcred' --set priorityClassName=critical \
    --set podLabels.team=data --set podSecurityContext.runAsNonRoot=true \
    --set securityContext.readOnlyRootFilesystem=true)
  selector=$(sed -n '/^  selector:/,/^  template:/p' <<<"$out")
  ! grep -q 'team: data' <<<"$selector" || flunk "podLabels leaked into the selector"
  passed
  for want in '      imagePullSecrets:' '        - name: regcred' '      priorityClassName: critical' \
              '        team: data' '      securityContext:' '        runAsNonRoot: true' \
              '          securityContext:' '            readOnlyRootFilesystem: true'; do
    has_line "$want"
  done
}

site_and_takeover_delay() {
  local bad
  out=$(t --set site=eu-west)
  val KFKLEASE_SITE eu-west
  # shellcheck disable=SC2016 # Kubernetes expands $(POD_NAME), not the shell
  val KFKLEASE_HOLDER 'eu-west/$(POD_NAME)'
  # 5 s polling + 60 s cooldown + 30 s margin.
  val KFKLEASE_TAKEOVER_DELAY 95s
  has_line '  cooldownPeriod: 60'
  out=$(t --set site=eu --set takeoverDelay=120)
  val KFKLEASE_TAKEOVER_DELAY 120s
  # The slowest ScaledObject of the release sets the bound.
  out=$(t --set site=eu --set 'extraLeases[0].topic=b' --set 'extraLeases[0].scaledObject.targetRef.name=wb' \
    --set 'extraLeases[0].scaledObject.cooldownPeriod=100')
  val KFKLEASE_TAKEOVER_DELAY 135s
  refuses render "${rest[@]}" "${target[@]}"
  for bad in 'site=eu,holderPrefix=eu' 'site=eu,takeoverDelay=94' 'site=eu,takeoverDelay=-1'; do
    refuses t --set "$bad"
  done
  out=$(t --set holderPrefix=eu --set site=eu 2>&1) || true
  has 'replaced by site'
}

two_scaler_replicas() {
  local want
  out=$(t)
  for want in '  replicas: 2' '    type: RollingUpdate' '      maxSurge: 1' '      maxUnavailable: 0' \
              '              service: ready' 'kind: PodDisruptionBudget' '  minAvailable: 1' \
              '        podAntiAffinity:' '                topologyKey: kubernetes.io/hostname'; do
    has_line "$want"
  done
  lacks 'Recreate' "still Recreate"
  out=$(t --set replicaCount=1 \
    --set 'affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].key=k' \
    --set 'affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].operator=Exists')
  has_line '  replicas: 1'
  lacks 'kind: PodDisruptionBudget' "PodDisruptionBudget with one replica blocks every drain"
  lacks 'podAntiAffinity' "default anti-affinity next to the given affinity"
  has 'nodeAffinity:'
  out=$(t --set podDisruptionBudget.enabled=false)
  lacks 'kind: PodDisruptionBudget' "PodDisruptionBudget rendered although disabled"
  refuses t --set replicaCount=0
}

standby() {
  out=$(dep)
  val KFKLEASE_STANDBY false
  out=$(dep --set standby=true)
  val KFKLEASE_STANDBY true
}

handover_access() {
  local want off a=(--set topic=a --set scaledObject.targetRef.name=wa)
  out=$(t "${a[@]}" --set 'extraLeases[0].topic=b' --set 'extraLeases[0].scaledObject.targetRef.name=wb' \
    --set 'extraLeases[0].scaledObject.targetRef.kind=KafkaConnect' \
    --set 'extraLeases[0].scaledObject.targetRef.apiVersion=kafka.strimzi.io/v1beta2' \
    --set 'extraLeases[1].topic=c' --set 'extraLeases[1].scaledObject.enabled=false')
  for want in 'kind: ServiceAccount' 'kind: Role' 'kind: RoleBinding' '      serviceAccountName: t' \
              '      - wa' '      - wb' '    verbs: [get, patch]' '  - apiGroups: ["apps"]' \
              '    resources: [deployments/scale]' '  - apiGroups: ["kafka.strimzi.io"]' \
              '    resources: [kafkaconnects/scale]' '    resources: [pods]' '    verbs: [list]' \
              '              value: "wa,wb,"'; do
    has_line "$want"
  done
  lacks 'ClusterRole|kind: ClusterRoleBinding' "cluster-wide access"
  for off in 'handover.enabled=false' 'scaledObject.enabled=false'; do
    out=$(t "${a[@]}" --set "$off")
    lacks 'kind: Role|serviceAccountName|KFKLEASE_SCALED_OBJECT' "handover rendered with $off"
  done
}

several_leases() {
  local want bad a=(--set topic=a --set scaledObject.targetRef.name=wa --set scaledObject.cooldownPeriod=7)
  out=$(t "${a[@]}" --set 'extraLeases[0].topic=b' --set 'extraLeases[0].scaledObject.targetRef.name=wb' \
    --set 'extraLeases[0].scaledObject.targetRef.kind=KafkaConnect' \
    --set 'extraLeases[0].scaledObject.targetRef.apiVersion=kafka.strimzi.io/v1beta2' \
    --set 'extraLeases[0].scaledObject.replicas=3' --set 'extraLeases[0].healthGate.url=http://b:8083/' \
    --set 'extraLeases[1].topic=c' --set 'extraLeases[1].scaledObject.targetRef.name=wc')
  counts 3 '^kind: ScaledObject' "want 3 ScaledObjects"
  counts 3 '^  cooldownPeriod: 7' "top-level scaledObject not inherited"
  for want in 'value: "a,b,c"' 'value: ",http://b:8083/,"' 'topic: "b"' 'kind: KafkaConnect' 'minReplicaCount: 3'; do
    has "$want"
  done
  for bad in 'extraLeases[0].topic=a,extraLeases[0].scaledObject.targetRef.name=x' \
             'extraLeases[0].topic=b,extraLeases[0].scaledObject.targetRef.name=wa' \
             'extraLeases[0].topic=b' 'extraLeases[0].scaledObject.targetRef.name=x' 'topic=a\,b'; do
    refuses t "${a[@]}" --set "$bad"
  done
}

# check <function> <what it checks> runs one group of checks.
check() {
  echo "== $2"
  "$1"
}

check lints_and_renders "The chart lints and renders"
check n_replicas_of_any_kind "N replicas of any scalable kind; the old value fails loudly"
check fails_closed "Fail closed by default, a fallback to zero replicas"
check health_gate "Health gate off by default, wired when a URL is set"
check topic_settings "Topic settings for a lease log, three replicas by default"
check pod_settings "Pod settings render where Kubernetes reads them"
check site_and_takeover_delay "The lease belongs to a site, with a takeover delay that covers KEDA's cooldown"
check two_scaler_replicas "Two scaler replicas per site, rolled one at a time, kept by a PodDisruptionBudget"
check standby "Standby mode off by default"
check handover_access "Handover access, namespaced and scoped to the release's own objects"
check several_leases "Several leases in one release, one ScaledObject each"
echo "chart-test: ${checks} checks passed"
