#!/bin/bash

read -r -d '' SPEC_TEMPLATE <<EOF
name: SVCNAME
pods:
  node:
    count: 2
    tasks:
      task:
        goal: RUNNING
        cmd: "echo this is SVCNAME >> output && sleep 1000"
        cpus: 0.1
        memory: 252
EOF

runCurl() {
    echo "$1 $2 ..." 1>&2
    TOKEN="Authorization: token=$(dcos config show core.dcos_acs_token)"
    URL=$(dcos config show core.dcos_url)/service/queues${2}
    if [ -n "$3" ]; then
        # "@-" = read from stdin
        (set -o xtrace; echo "$3" | time curl -F 'file=@-' -F 'type=yaml' -X $1 -H "$TOKEN" $URL)
    else
        (set -o xtrace; time curl -X $1 -H "$TOKEN" $URL)
    fi
    echo
}

syntax() {
    echo "Commands:"
    echo "  list"
    echo "  <svcname> add"
    echo "  <svcname> remove"
    echo ""
    echo "  <svcname> plans"
    echo "  <svcname> deployplan"
    echo "  <svcname> recoveryplan"
    echo "  <svcname> pods"
    echo "  <svcname> restart"
    echo "  <svcname> replace"
    exit 1
}

if [ -z "$1" ]; then
    syntax
fi

svcname=$1
cmd=$2

# special case: list command doesn't need svcname
if [ "$svcname" == "list" ]; then
    runCurl GET /v1/queue
    exit
fi

# all other commands require svcname:
case $cmd in
    add)
        SPEC=$(echo "$SPEC_TEMPLATE" | sed s/SVCNAME/${svcname}/g)
        runCurl POST /v1/queue "$SPEC"
        ;;
    remove)
        runCurl DELETE /v1/queue/${svcname}
        ;;

    plans)
        runCurl GET /v1/run/${svcname}/plans
        ;;
    deployplan)
        runCurl GET /v1/run/${svcname}/plans/deploy
        ;;
    recoveryplan)
        runCurl GET /v1/run/${svcname}/plans/recovery
        ;;

    pods)
        runCurl GET /v1/run/${svcname}/pod
        ;;
    restart)
        runCurl POST /v1/run/${svcname}/pod/node-0/restart
        ;;
    replace)
        runCurl POST /v1/run/${svcname}/pod/node-0/replace
        ;;
    *)
        syntax
        ;;
esac

