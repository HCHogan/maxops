{ pkgs, self }:
pkgs.testers.runNixOSTest {
  name = "maxops-read-only";
  nodes.machine =
    { pkgs, ... }:
    let
      deploymentBaseline = pkgs.runCommand "maxops-deployment-baseline" { } ''
        mkdir -p $out/bin
        cat > $out/bin/switch-to-configuration <<EOF
        #!${pkgs.runtimeShell}
        ${pkgs.coreutils}/bin/ln -sfn "$out" /run/maxops-fixture-current
        ${pkgs.coreutils}/bin/printf 'baseline\n' > /run/maxops-fixture-mode
        EOF
        chmod +x $out/bin/switch-to-configuration
      '';
      deploymentExternal = pkgs.runCommand "maxops-deployment-external" { } ''
        mkdir -p $out/bin
        cat > $out/bin/switch-to-configuration <<EOF
        #!${pkgs.runtimeShell}
        ${pkgs.coreutils}/bin/ln -sfn "$out" /run/maxops-fixture-current
        ${pkgs.coreutils}/bin/printf 'external\n' > /run/maxops-fixture-mode
        EOF
        chmod +x $out/bin/switch-to-configuration
      '';
      deploymentFlake = pkgs.writeText "maxops-fixture-flake.nix" ''
        {
          outputs = { self }:
            let
              mode = builtins.readFile ./config.txt;
            in {
              packages.x86_64-linux.default = derivation {
                name = "maxops-fixture-system";
                system = "x86_64-linux";
                builder = ./busybox;
                args = [ "sh" "-c" '''
                  "$builder" mkdir -p $out/bin
                  "$builder" cp "$builder" $out/bin/busybox
                  "$builder" cat > $out/bin/switch-to-configuration <<EOF
                  #!$out/bin/busybox sh
                  $out/bin/busybox ln -sfn "$out" /run/maxops-fixture-current
                  $out/bin/busybox printf '%s' "''${mode}" > /run/maxops-fixture-mode
                  if [ "''${mode}" = bad ]; then exit 1; fi
                  EOF
                  "$builder" chmod +x $out/bin/switch-to-configuration
                ''' ];
              };
            };
        }
      '';
      deploymentLock = pkgs.writeText "maxops-fixture-flake.lock" ''
        {
          "nodes": {
            "root": {
              "inputs": {}
            }
          },
          "root": "root",
          "version": 7
        }
      '';
    in
    {
      imports = [ self.nixosModules.default ];
      networking.hostName = "fixture";
      # The classic initrd avoids case-colliding terminfo directories when this
      # cross-platform VM test is built from a macOS Nix store.
      boot.initrd.systemd.enable = false;
      environment.systemPackages = [
        self.packages.${pkgs.stdenv.hostPlatform.system}.default
        pkgs.curl
        pkgs.git
        pkgs.jq
        pkgs.python3
      ];
      systemd.tmpfiles.rules = [
        "f /run/agent-token 0400 root root - agent-test-token-aaaaaaaaaaaaaaaaaaaaaa"
        "f /run/client-token 0400 root root - client-test-token-bbbbbbbbbbbbbbbbbbbbb"
        "f /run/execution-token 0400 root root - execution-test-token-cccccccccccccccccc"
        "f /run/manager-token 0400 root root - manager-test-token-dddddddddddddddddddd"
        "f /run/manager2-token 0400 root root - manager2-test-token-eeeeeeeeeeeeeeeeeeeee"
        "f /run/alert-token 0400 root root - alert-test-token-ffffffffffffffffffffff"
        "f /run/job-credential 0400 root root - fixture-credential-value"
        "d /var/lib/maxops-fixture-deploy 0755 root root - -"
        "L+ /var/lib/maxops-fixture-deploy/profile - - - - ${deploymentBaseline}"
        "L+ /run/maxops-fixture-current - - - - ${deploymentBaseline}"
        "L+ /run/maxops-fixture-external - - - - ${deploymentExternal}"
      ];
      services.maxops-executor = {
        enable = true;
        manageableUnits = [
          "maxops-managed.service"
          "maxops-no-reload.service"
        ];
        credentialSources.fixture = "/run/job-credential";
        profiles.diagnostic = {
          environment.PATH = pkgs.lib.makeBinPath [
            pkgs.coreutils
            pkgs.systemd
            pkgs.iproute2
            pkgs.procps
            pkgs.gnugrep
            pkgs.jq
            pkgs.tailscale
          ];
          timeoutSeconds = 600;
          outputLimitBytes = 64;
          tasksMax = 32;
          memoryMaxBytes = 268435456;
          allowedCredentials = [ "fixture" ];
        };
        profiles.activation = {
          user = "root";
          privileged = true;
          timeoutSeconds = 300;
          outputLimitBytes = 65536;
        };
        profiles.deployment = {
          timeoutSeconds = 600;
          outputLimitBytes = 65536;
          tasksMax = 32;
          memoryMaxBytes = 268435456;
        };
        repositories.fixture = {
          url = "/var/lib/maxops-executor/fixture-remote.git";
          publishRefs = [ "refs/heads/main" ];
          checks = {
            content = [
              "${pkgs.bash}/bin/bash"
              "-c"
              ''test "$(cat config.txt)" = changed-again && test -z "''${CREDENTIALS_DIRECTORY+x}"''
            ];
            frozen = [
              "${pkgs.bash}/bin/bash"
              "-c"
              ''first=$(cat config.txt); sleep 6; test "$first" = changed && test "$(cat config.txt)" = changed && test -z "''${CREDENTIALS_DIRECTORY+x}"''
            ];
          };
        };
        deploymentProfiles.fixture-system = {
          repository = "fixture";
          targetHost = "fixture";
          flakeAttribute = "packages.x86_64-linux.default";
          buildProfile = "deployment";
          activateProfile = "activation";
          verifyProfile = "deployment";
          profilePath = "/var/lib/maxops-fixture-deploy/profile";
          runningLink = "/run/maxops-fixture-current";
          verifyCommands = [
            [
              "${pkgs.bash}/bin/bash"
              "-c"
              ''test "$(${pkgs.coreutils}/bin/readlink /run/maxops-fixture-current)" = "$(${pkgs.coreutils}/bin/readlink -f /var/lib/maxops-fixture-deploy/profile)" && ${pkgs.gnugrep}/bin/grep -qx published /run/maxops-fixture-mode''
            ]
          ];
        };
      };
      services.maxops-agent = {
        enable = true;
        tokenFile = "/run/agent-token";
        readableUnits = [
          "maxops-fixture.service"
          "maxops-managed.service"
          "maxops-no-reload.service"
        ];
        manageableUnits = [
          "maxops-managed.service"
          "maxops-no-reload.service"
        ];
        allowLogs = true;
        execution = {
          enable = true;
          tokenFile = "/run/execution-token";
        };
      };
      services.maxops-hub = {
        enable = true;
        repositories = [
          {
            name = "fixture";
            executorHost = "fixture";
          }
        ];
        deployments = [
          {
            name = "fixture-system";
            repository = "fixture";
            builderHost = "fixture";
            targetHost = "fixture";
            flakeAttribute = "packages.x86_64-linux.default";
          }
        ];
        hosts = [
          {
            name = "fixture";
            agentUrl = "http://127.0.0.1:9720";
            tokenFile = "/run/agent-token";
            executionTokenFile = "/run/execution-token";
            readableUnits = [
              "maxops-fixture.service"
              "maxops-managed.service"
              "maxops-no-reload.service"
            ];
            manageableUnits = [
              "maxops-managed.service"
              "maxops-no-reload.service"
            ];
            diagnosticProfile = "diagnostic";
            diagnosticProbes.identity = [
              "${pkgs.coreutils}/bin/id"
              "-u"
            ];
          }
        ];
        clients = [
          {
            name = "test";
            tokenFile = "/run/client-token";
            hosts = [ "fixture" ];
            capabilities = [
              "fleet:read"
              "units:read"
              "host:read"
              "logs:read"
              "events:read"
              "self:read"
            ];
          }
          {
            name = "manager";
            tokenFile = "/run/manager-token";
            hosts = [ "fixture" ];
            access = "manage";
            capabilities = [
              "exec:run"
              "units:manage"
              "jobs:read"
              "jobs:cancel"
              "workspace:read"
              "workspace:write"
              "workspace:publish"
              "deploy:manage"
              "changes:read"
              "diagnostics:collect"
              "remediations:manage"
              "events:read"
              "self:read"
            ];
            repositories = [ "fixture" ];
            deployments = [ "fixture-system" ];
          }
          {
            name = "manager2";
            tokenFile = "/run/manager2-token";
            hosts = [ "fixture" ];
            access = "manage";
            capabilities = [
              "units:manage"
              "jobs:read"
              "jobs:cancel"
            ];
          }
        ];
        alertIngress = {
          enable = true;
          tokenFile = "/run/alert-token";
          sinkUrl = "http://127.0.0.1:9730/legacy";
        };
        eventSinks = [
          {
            id = "fixture-automation";
            url = "http://127.0.0.1:9730/events";
            hosts = [ "fixture" ];
            retrySeconds = 1;
          }
        ];
        remediationPolicy = {
          maxAttemptsPerEpisode = 1;
          cooldownSeconds = 0;
        };
      };
      systemd.services.maxops-executor.preStart = ''
        if ! ${pkgs.git}/bin/git --git-dir=/var/lib/maxops-executor/fixture-remote.git rev-parse --verify refs/heads/main >/dev/null 2>&1; then
          rm -rf /var/lib/maxops-executor/fixture-remote.git /var/lib/maxops-executor/fixture-seed
          ${pkgs.git}/bin/git init --bare --initial-branch=main /var/lib/maxops-executor/fixture-remote.git
          ${pkgs.git}/bin/git init --initial-branch=main /var/lib/maxops-executor/fixture-seed
          printf 'initial\n' > /var/lib/maxops-executor/fixture-seed/config.txt
          cp ${pkgs.pkgsStatic.busybox}/bin/busybox /var/lib/maxops-executor/fixture-seed/busybox
          chmod +x /var/lib/maxops-executor/fixture-seed/busybox
          cp ${deploymentFlake} /var/lib/maxops-executor/fixture-seed/flake.nix
          cp ${deploymentLock} /var/lib/maxops-executor/fixture-seed/flake.lock
          ln -s /etc/shadow /var/lib/maxops-executor/fixture-seed/escape
          ${pkgs.git}/bin/git -C /var/lib/maxops-executor/fixture-seed add config.txt busybox flake.nix flake.lock escape
          ${pkgs.git}/bin/git -C /var/lib/maxops-executor/fixture-seed \
            -c user.name=Fixture -c user.email=fixture@example.invalid commit -m initial
          ${pkgs.git}/bin/git -C /var/lib/maxops-executor/fixture-seed remote add origin /var/lib/maxops-executor/fixture-remote.git
          ${pkgs.git}/bin/git -C /var/lib/maxops-executor/fixture-seed push origin refs/heads/main
          rm -rf /var/lib/maxops-executor/fixture-seed
        fi
      '';
      systemd.services.maxops-fixture = {
        wantedBy = [ "multi-user.target" ];
        serviceConfig.Type = "oneshot";
        script = ''
          echo maxops-journal-fixture
          exit 1
        '';
      };
      systemd.services.maxops-managed = {
        wantedBy = [ "multi-user.target" ];
        script = ''
          echo "$INVOCATION_ID" >> /var/lib/maxops-managed-invocations
          trap 'echo reload >> /var/lib/maxops-managed-reloads' HUP
          while true; do sleep 1; done
        '';
        reload = ''
          if test -e /run/maxops-fail-reload; then
            exit 1
          fi
          kill -HUP "$MAINPID"
        '';
        preStop = ''
          sleep 4
        '';
      };
      systemd.services.maxops-no-reload = {
        wantedBy = [ "multi-user.target" ];
        script = ''
          while true; do sleep 1; done
        '';
      };
      systemd.services.maxops-webhook-fixture = {
        wantedBy = [ "multi-user.target" ];
        serviceConfig = {
          DynamicUser = true;
          StateDirectory = "maxops-webhook-fixture";
        };
        script = ''
          ${pkgs.python3}/bin/python3 - <<'PY'
          from http.server import BaseHTTPRequestHandler, HTTPServer
          from pathlib import Path

          class Handler(BaseHTTPRequestHandler):
              def do_POST(self):
                  body = self.rfile.read(int(self.headers.get("content-length", "0")))
                  target = Path("/var/lib/maxops-webhook-fixture/events.jsonl")
                  with target.open("ab") as stream:
                      stream.write(body + b"\n")
                  if self.path == "/events":
                      self.send_response(202)
                      self.send_header("content-type", "application/json")
                      self.end_headers()
                      self.wfile.write(b'{"stage":"accepted"}')
                  else:
                      self.send_response(200)
                      self.end_headers()

              def log_message(self, format, *args):
                  pass

          HTTPServer(("127.0.0.1", 9730), Handler).serve_forever()
          PY
        '';
      };
    };
  testScript = ''
    import json

    start_all()
    machine.wait_for_unit("maxops-agent.service")
    machine.wait_for_unit("maxops-executor.service")
    machine.wait_for_unit("maxops-hub.service")
    machine.wait_for_unit("maxops-webhook-fixture.service")
    machine.wait_for_open_port(9721)
    baseline = machine.succeed("readlink /run/maxops-fixture-current").strip()
    external_deployment = machine.succeed("readlink /run/maxops-fixture-external").strip()
    machine.succeed("curl -fsS http://127.0.0.1:9720/healthz")
    machine.fail("curl -fsS http://127.0.0.1:9720/v1/snapshot")
    ctl = "timeout 60 maxopsctl --token-file /run/client-token "
    machine.succeed(ctl + "host.facts --host fixture | jq -e '.facts.system_closure | startswith(\"/nix/store/\")'")
    machine.succeed(ctl + "units.failed | jq -e '.hosts[0].units[0].unit == \"maxops-fixture.service\"'")
    machine.succeed(ctl + "units.list --host fixture | jq -e '.units | length == 3'")
    machine.succeed(ctl + "units.status --host fixture --unit maxops-fixture.service | jq -e '.unit.details.exec_main_status == 1'")
    machine.succeed(ctl + "deploy.status | jq -e '.hosts[0].activated_at == null'")
    machine.succeed(ctl + "units.logs --host fixture --unit maxops-fixture.service | jq -e '[.entries[].message] | any(contains(\"maxops-journal-fixture\"))'")
    machine.fail(ctl + "units.logs --host fixture --unit sshd.service")
    machine.fail("runuser -u maxops-agent -- systemctl --no-ask-password restart maxops-fixture.service")

    manager = "timeout 60 maxopsctl --token-file /run/manager-token "
    machine.succeed("curl -fsS http://127.0.0.1:9721/readyz | jq -e '.ready == true and .components.storage == \"ready\"'")
    machine.fail("curl -fsS http://127.0.0.1:9721/metrics")
    machine.succeed("curl -fsS -H 'Authorization: Bearer manager-test-token-dddddddddddddddddddd' http://127.0.0.1:9721/metrics | grep -q '^maxops_jobs_nonterminal '")

    # Any HTTP client can turn an Alertmanager event into evidence and a
    # budgeted repair; no Max/chat process participates in this flow.
    machine.succeed("cat > /tmp/alert.json <<'EOF'\n{\"version\":\"4\",\"status\":\"firing\",\"alerts\":[{\"status\":\"firing\",\"fingerprint\":\"fixture-managed-down\",\"startsAt\":\"2026-09-06T00:00:00Z\",\"labels\":{\"instance\":\"fixture\",\"alertname\":\"ManagedServiceDown\"}}]}\nEOF")
    machine.succeed("curl -fsS -H 'Authorization: Bearer alert-test-token-ffffffffffffffffffffff' -H 'Content-Type: application/json' --data-binary @/tmp/alert.json http://127.0.0.1:9721/v1/alerts | jq -e '.events_persisted == 1'")
    alert_event = machine.succeed(manager + "events.list | jq -r '.events[] | select(.kind == \"alert_firing\") | .event_id'").strip()
    machine.succeed(f"cat > /tmp/diagnostic-request.json <<'EOF'\n{{\"host\":\"fixture\",\"event_id\":\"{alert_event}\",\"unit\":\"maxops-managed.service\",\"probes\":[\"identity\"]}}\nEOF")
    diagnostic_job = machine.succeed(manager + "diagnostics.collect --params-file /tmp/diagnostic-request.json --idempotency-key fixture-diagnostic | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {diagnostic_job} | jq -e '.handle.state == \"succeeded\" and .result.diagnostic.host == \"fixture\" and (.result.diagnostic.evidence | length) == 3'", timeout=30)
    machine.succeed(f"cat > /tmp/remediation-begin.json <<'EOF'\n{{\"event_id\":\"{alert_event}\",\"host\":\"fixture\"}}\nEOF")
    claim_job = machine.succeed(manager + "remediations.begin --params-file /tmp/remediation-begin.json --idempotency-key fixture-remediation | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {claim_job} | jq -e '.handle.state == \"succeeded\"'", timeout=20)
    remediation = machine.succeed(manager + f"jobs.status --job-id {claim_job} | jq -r .result.remediation.remediation_id").strip()
    repair_job = machine.succeed(manager + "units.restart --host fixture --unit maxops-managed.service --idempotency-key fixture-auto-repair | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {repair_job} | jq -e '.handle.state == \"succeeded\"'", timeout=30)
    machine.succeed(f"cat > /tmp/remediation-finish.json <<'EOF'\n{{\"remediation_id\":\"{remediation}\",\"expected_revision\":1,\"outcome\":\"succeeded\",\"related_job_id\":\"{repair_job}\",\"summary\":\"service restarted and target state verified\"}}\nEOF")
    machine.succeed(manager + "remediations.finish --params-file /tmp/remediation-finish.json | jq -e '.state == \"succeeded\"'")
    machine.wait_until_succeeds("test $(wc -l < /var/lib/maxops-webhook-fixture/events.jsonl) -ge 5", timeout=20)
    second_claim = machine.succeed(manager + "remediations.begin --params-file /tmp/remediation-begin.json --idempotency-key fixture-remediation-again | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {second_claim} | jq -e '.handle.state == \"failed\" and .result.error == \"remediation_budget_exhausted\"'", timeout=20)

    machine.succeed("echo '{\"host\":\"fixture\",\"profile\":\"diagnostic\",\"command\":{\"argv\":[\"${pkgs.bash}/bin/bash\",\"-c\",\"printf first; sleep 12; printf second\"]},\"timeout_seconds\":30}' > /tmp/long-job.json")
    probe = {"host":"fixture", "profile":"diagnostic", "command":{"script":"set -euo pipefail; command -v systemctl journalctl ip free grep jq tailscale >/dev/null; systemctl --version >/dev/null; ip -j address show | jq -e 'length > 0' >/dev/null; printf diagnostic-ready"}}
    machine.succeed("cat > /tmp/diagnostic-env.json <<'EOF'\n" + json.dumps(probe) + "\nEOF")
    probe_job = machine.succeed(manager + "exec.run --params-file /tmp/diagnostic-env.json --idempotency-key diagnostic-environment | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {probe_job} | jq -e '.handle.state == \"succeeded\" and .result.exit_code == 0 and .result.stdout_bytes == 16 and .result.stderr_bytes == 0'", timeout=30)

    machine.fail(ctl + "exec.run --params-file /tmp/long-job.json --idempotency-key observer-cannot-run")
    job = machine.succeed(manager + "exec.run --params-file /tmp/long-job.json --idempotency-key restart-survival | jq -r .job_id").strip()
    same_job = machine.succeed(manager + "exec.run --params-file /tmp/long-job.json --idempotency-key restart-survival | jq -r .job_id").strip()
    assert job == same_job
    machine.wait_until_succeeds(f"systemctl is-active maxops-job-{job}.service", timeout=20)
    machine.succeed(f"systemctl show maxops-job-{job}.service -p User --value | grep -x maxops-runner")
    machine.succeed(f"systemctl show maxops-job-{job}.service -p KillMode --value | grep -x control-group")
    machine.succeed(f"systemctl show maxops-job-{job}.service -p NoNewPrivileges --value | grep -x yes")
    machine.succeed(f"systemctl show maxops-job-{job}.service -p ProtectSystem --value | grep -x strict")
    machine.succeed("systemctl restart maxops-agent.service maxops-hub.service maxops-executor.service")
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {job} | jq -e '.handle.state == \"succeeded\"'", timeout=30)
    machine.succeed(manager + f"jobs.logs --job-id {job} | jq -r .stdout_base64 | base64 -d | grep -x firstsecond")

    machine.succeed("echo '{\"host\":\"fixture\",\"profile\":\"diagnostic\",\"command\":{\"argv\":[\"${pkgs.bash}/bin/bash\",\"-c\",\"sleep 30\"]},\"timeout_seconds\":30}' > /tmp/cancel-job.json")
    cancel_job = machine.succeed(manager + "exec.run --params-file /tmp/cancel-job.json --idempotency-key cancellation | jq -r .job_id").strip()
    machine.wait_until_succeeds(f"systemctl is-active maxops-job-{cancel_job}.service", timeout=20)
    revision = machine.succeed(manager + f"jobs.status --job-id {cancel_job} | jq -r .handle.revision").strip()
    machine.succeed(manager + f"jobs.cancel --job-id {cancel_job} --expected-revision {revision} --reason fixture-stop | jq -e '.handle.state == \"cancelled\"'")
    machine.fail(f"systemctl is-active maxops-job-{cancel_job}.service")

    machine.succeed("echo '{\"host\":\"fixture\",\"profile\":\"diagnostic\",\"command\":{\"argv\":[\"${pkgs.bash}/bin/bash\",\"-c\",\"sleep 30\"]},\"timeout_seconds\":2}' > /tmp/timeout-job.json")
    timeout_job = machine.succeed(manager + "exec.run --params-file /tmp/timeout-job.json --idempotency-key timeout | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {timeout_job} | jq -e '.handle.state == \"timed_out\"'", timeout=20)

    machine.succeed("echo '{\"host\":\"fixture\",\"profile\":\"diagnostic\",\"command\":{\"argv\":[\"${pkgs.bash}/bin/bash\",\"-c\",\"head -c 1024 /dev/zero\"]},\"timeout_seconds\":30}' > /tmp/output-job.json")
    output_job = machine.succeed(manager + "exec.run --params-file /tmp/output-job.json --idempotency-key output-limit | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {output_job} | jq -e '.handle.state == \"succeeded\"'", timeout=20)
    machine.succeed(manager + f"jobs.logs --job-id {output_job} | jq -e '.truncated == true and .encoding == \"base64\" and (.stdout_base64 | length) > 0'")

    machine.succeed("echo '{\"host\":\"fixture\",\"profile\":\"diagnostic\",\"command\":{\"argv\":[\"${pkgs.bash}/bin/bash\",\"-c\",\"cat $CREDENTIALS_DIRECTORY/fixture\"]},\"credential_refs\":[\"fixture\"],\"timeout_seconds\":30}' > /tmp/credential-job.json")
    credential_job = machine.succeed(manager + "exec.run --params-file /tmp/credential-job.json --idempotency-key credential | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {credential_job} | jq -e '.handle.state == \"succeeded\"'", timeout=20)
    machine.succeed(manager + f"jobs.logs --job-id {credential_job} | jq -r .stdout_base64 | base64 -d | grep -x fixture-credential-value")
    machine.succeed("jq '.credential_refs = [\"undeclared\"]' /tmp/credential-job.json > /tmp/forbidden-credential-job.json")
    forbidden_job = machine.succeed(manager + "exec.run --params-file /tmp/forbidden-credential-job.json --idempotency-key forbidden-credential | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {forbidden_job} | jq -e '.handle.state == \"failed\"'", timeout=20)

    # Service mutations use a distinct capability and exact manageable-unit list.
    machine.fail(ctl + "units.restart --host fixture --unit maxops-managed.service --idempotency-key observer-service-action")
    machine.fail(manager + "units.restart --host fixture --unit maxops-fixture.service --idempotency-key forbidden-service")
    invocation = machine.succeed(ctl + "units.status --host fixture --unit maxops-managed.service | jq -r .unit.details.invocation_id").strip()
    assert len(invocation) == 32
    machine.succeed(f"echo '{{\"host\":\"fixture\",\"unit\":\"maxops-managed.service\",\"expected_invocation_id\":\"{invocation}\"}}' > /tmp/restart-service.json")
    restart_job = machine.succeed(manager + "units.restart --params-file /tmp/restart-service.json --idempotency-key service-restart-recovery | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {restart_job} | jq -e '.handle.state == \"reconciling\"'", timeout=20)
    machine.succeed("systemctl restart maxops-executor.service")
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {restart_job} | jq -e '.handle.state == \"succeeded\"'", timeout=30)
    restarted_invocation = machine.succeed(ctl + "units.status --host fixture --unit maxops-managed.service | jq -r .unit.details.invocation_id").strip()
    assert restarted_invocation != invocation
    machine.succeed(manager + f"jobs.status --job-id {restart_job} | jq -e '.result.before.invocation_id == \"{invocation}\" and .result.after.invocation_id == \"{restarted_invocation}\"'")

    stop_job = machine.succeed(manager + "units.stop --host fixture --unit maxops-managed.service --idempotency-key service-stop | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {stop_job} | jq -e '.handle.state == \"succeeded\"'", timeout=20)
    machine.fail("systemctl is-active maxops-managed.service")
    start_job = machine.succeed(manager + "units.start --host fixture --unit maxops-managed.service --idempotency-key service-start | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {start_job} | jq -e '.handle.state == \"succeeded\"'", timeout=20)
    machine.succeed("systemctl is-active maxops-managed.service")

    reload_job = machine.succeed(manager + "units.reload --host fixture --unit maxops-managed.service --idempotency-key service-reload | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {reload_job} | jq -e '.handle.state == \"succeeded\"'", timeout=20)
    machine.wait_until_succeeds("grep -x reload /var/lib/maxops-managed-reloads", timeout=20)
    machine.succeed("touch /run/maxops-fail-reload")
    failed_reload = machine.succeed(manager + "units.reload --host fixture --unit maxops-managed.service --idempotency-key failed-reload | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {failed_reload} | jq -e '.handle.state == \"failed\" and .result.after.reload_result != \"success\"'", timeout=20)
    machine.succeed("rm /run/maxops-fail-reload")
    unsupported_reload = machine.succeed(manager + "units.reload --host fixture --unit maxops-no-reload.service --idempotency-key unsupported-reload | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {unsupported_reload} | jq -e '.handle.state == \"failed\" and .result.error == \"systemd_rejected_action\"'", timeout=20)

    # A manual restart is a valid external fleet write. It invalidates an old
    # InvocationID instead of being overwritten by the stale maxops request.
    stale_invocation = machine.succeed(ctl + "units.status --host fixture --unit maxops-managed.service | jq -r .unit.details.invocation_id").strip()
    machine.succeed("systemctl restart maxops-managed.service")
    external_invocation = machine.succeed(ctl + "units.status --host fixture --unit maxops-managed.service | jq -r .unit.details.invocation_id").strip()
    assert external_invocation != stale_invocation
    machine.succeed(f"echo '{{\"host\":\"fixture\",\"unit\":\"maxops-managed.service\",\"expected_invocation_id\":\"{stale_invocation}\"}}' > /tmp/stale-restart.json")
    stale_job = machine.succeed(manager + "units.restart --params-file /tmp/stale-restart.json --idempotency-key stale-service-restart | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {stale_job} | jq -e '.handle.state == \"failed\" and .result.error == \"stale_baseline\"'", timeout=20)
    machine.succeed("systemctl is-active maxops-managed.service")

    # Two independent principals may submit concurrently, but the executor's
    # persistent host-level manager lock makes the observed invocation chain serial.
    manager2 = "timeout 60 maxopsctl --token-file /run/manager2-token "
    first_restart = machine.succeed(manager + "units.restart --host fixture --unit maxops-managed.service --idempotency-key concurrent-manager-1 | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {first_restart} | jq -e '.handle.state == \"reconciling\"'", timeout=20)
    second_restart = machine.succeed(manager2 + "units.restart --host fixture --unit maxops-managed.service --idempotency-key concurrent-manager-2 | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {first_restart} | jq -e '.handle.state == \"succeeded\"'", timeout=30)
    machine.wait_until_succeeds(manager2 + f"jobs.status --job-id {second_restart} | jq -e '.handle.state == \"succeeded\"'", timeout=30)
    first_after = machine.succeed(manager + f"jobs.status --job-id {first_restart} | jq -r .result.after.invocation_id").strip()
    second_before = machine.succeed(manager2 + f"jobs.status --job-id {second_restart} | jq -r .result.before.invocation_id").strip()
    assert first_after == second_before
    machine.fail(manager2 + f"jobs.status --job-id {first_restart}")

    # Repository work happens in private immutable revisions, never in a human checkout.
    machine.succeed("git clone /var/lib/maxops-executor/fixture-remote.git /tmp/human")
    machine.succeed("printf dirty >> /tmp/human/config.txt")
    machine.fail(ctl + "workspace.create --repository fixture --idempotency-key observer-workspace")
    machine.fail(manager2 + "workspace.create --repository fixture --idempotency-key ungranted-workspace")
    machine.succeed(manager + "workspace.create --repository fixture --idempotency-key workspace-create --wait > /tmp/workspace-create.json")
    workspace = machine.succeed("jq -r .result.workspace.workspace_id /tmp/workspace-create.json").strip()
    base = machine.succeed("jq -r .result.workspace.base_commit /tmp/workspace-create.json").strip()
    machine.succeed(manager + f"workspace.status --repository fixture --workspace-id {workspace} | jq -e '.revision == 1 and .state == \"clean\"'")
    machine.succeed(manager + f"workspace.read --repository fixture --workspace-id {workspace} --expected-revision 1 --path config.txt | jq -e '.content == \"initial\\n\"'")
    machine.fail(manager + f"workspace.read --repository fixture --workspace-id {workspace} --expected-revision 1 --path ../etc/shadow 2> /tmp/traversal-error")
    machine.succeed("grep -F 422 /tmp/traversal-error")
    machine.fail(manager + f"workspace.read --repository fixture --workspace-id {workspace} --expected-revision 1 --path escape 2> /tmp/symlink-error")
    machine.succeed("grep -F 422 /tmp/symlink-error")
    machine.succeed(f"cat > /tmp/workspace-apply.json <<'EOF'\n{{\"repository\":\"fixture\",\"workspace_id\":\"{workspace}\",\"expected_revision\":1,\"edits\":[{{\"path\":\"config.txt\",\"content\":\"changed\\n\"}}]}}\nEOF")
    machine.succeed(manager + "workspace.apply --params-file /tmp/workspace-apply.json | jq -e '.revision == 2 and .state == \"dirty\"'")
    machine.fail(manager + "workspace.apply --params-file /tmp/workspace-apply.json 2> /tmp/revision-error")
    machine.succeed("grep -F 409 /tmp/revision-error")
    machine.succeed(manager + f"workspace.diff --repository fixture --workspace-id {workspace} --expected-revision 2 | jq -r .patch | grep -F '+changed'")

    frozen_job = machine.succeed(manager + f"workspace.check --repository fixture --workspace-id {workspace} --expected-revision 2 --check frozen --idempotency-key frozen-check | jq -r .job_id").strip()
    machine.wait_until_succeeds(f"systemctl is-active maxops-job-{frozen_job}.service", timeout=20)
    machine.succeed(f"cat > /tmp/workspace-apply-2.json <<'EOF'\n{{\"repository\":\"fixture\",\"workspace_id\":\"{workspace}\",\"expected_revision\":2,\"edits\":[{{\"path\":\"config.txt\",\"content\":\"changed-again\\n\"}}]}}\nEOF")
    machine.succeed(manager + "workspace.apply --params-file /tmp/workspace-apply-2.json | jq -e '.revision == 3'")
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {frozen_job} | jq -e '.handle.state == \"succeeded\"'", timeout=20)
    machine.succeed(manager + f"workspace.check --repository fixture --workspace-id {workspace} --expected-revision 3 --check content --idempotency-key content-check --wait | jq -e '.handle.state == \"succeeded\"'")
    machine.succeed(manager + f"workspace.commit --repository fixture --workspace-id {workspace} --expected-revision 3 --message 'maxops change' > /tmp/workspace-commit.json")
    commit = machine.succeed("jq -r .commit_hash /tmp/workspace-commit.json").strip()
    assert len(commit) == 40

    # Another writer advances the remote. The old baseline cannot be published over it.
    machine.succeed("git clone /var/lib/maxops-executor/fixture-remote.git /tmp/external")
    machine.succeed("printf external\\n > /tmp/external/external.txt")
    machine.succeed("git -C /tmp/external add external.txt && git -C /tmp/external -c user.name=External -c user.email=external@example.invalid commit -m external && git -C /tmp/external push origin main")
    external = machine.succeed("git --git-dir=/var/lib/maxops-executor/fixture-remote.git rev-parse refs/heads/main").strip()
    machine.succeed(f"cat > /tmp/workspace-publish-stale.json <<'EOF'\n{{\"repository\":\"fixture\",\"workspace_id\":\"{workspace}\",\"expected_revision\":4,\"reference\":\"refs/heads/main\",\"expected_remote_head\":\"{base}\"}}\nEOF")
    stale_publish = machine.succeed(manager + "workspace.publish --params-file /tmp/workspace-publish-stale.json --idempotency-key stale-publish | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {stale_publish} | jq -e '.handle.state == \"failed\" and .result.error == \"baseline_changed\" and .result.observed_remote_head == \"{external}\"'", timeout=20)
    machine.succeed("git --git-dir=/var/lib/maxops-executor/fixture-remote.git show refs/heads/main:config.txt | grep -x initial")

    # Re-observe the new remote head in a new workspace, then publish by fast-forward.
    machine.succeed(f"cat > /tmp/workspace-create-2-request.json <<'EOF'\n{{\"repository\":\"fixture\",\"expected_remote_head\":\"{external}\"}}\nEOF")
    machine.succeed(manager + "workspace.create --params-file /tmp/workspace-create-2-request.json --idempotency-key workspace-rebase --wait > /tmp/workspace-create-2.json")
    workspace2 = machine.succeed("jq -r .result.workspace.workspace_id /tmp/workspace-create-2.json").strip()
    machine.succeed(f"cat > /tmp/workspace-apply-3.json <<'EOF'\n{{\"repository\":\"fixture\",\"workspace_id\":\"{workspace2}\",\"expected_revision\":1,\"edits\":[{{\"path\":\"config.txt\",\"content\":\"published\\n\"}}]}}\nEOF")
    machine.succeed(manager + "workspace.apply --params-file /tmp/workspace-apply-3.json > /tmp/workspace-apply-3-result.json")
    machine.succeed(manager + f"workspace.commit --repository fixture --workspace-id {workspace2} --expected-revision 2 --message 'publish after external change' > /tmp/workspace-commit-2.json")
    commit2 = machine.succeed("jq -r .commit_hash /tmp/workspace-commit-2.json").strip()
    machine.succeed(f"cat > /tmp/workspace-publish.json <<'EOF'\n{{\"repository\":\"fixture\",\"workspace_id\":\"{workspace2}\",\"expected_revision\":3,\"reference\":\"refs/heads/main\",\"expected_remote_head\":\"{external}\"}}\nEOF")
    machine.succeed(manager + "workspace.publish --params-file /tmp/workspace-publish.json --idempotency-key publish-after-refresh --wait | jq -e '.handle.state == \"succeeded\" and .result.workspace.state == \"published\"'")
    machine.succeed(f"test \"$(git --git-dir=/var/lib/maxops-executor/fixture-remote.git rev-parse refs/heads/main)\" = {commit2}")
    machine.succeed("git --git-dir=/var/lib/maxops-executor/fixture-remote.git show refs/heads/main:config.txt | grep -x published")
    machine.fail("git -C /tmp/human diff --quiet")
    machine.succeed("grep -F dirty /tmp/human/config.txt")

    # Build and activate a real Nix store output from the immutable workspace.
    change = machine.succeed(manager + f"deploy.prepare --repository fixture --workspace-id {workspace2} --expected-revision 4 --target-host fixture --profile fixture-system --idempotency-key deploy-prepare | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"changes.status --change-id {change} | jq -e '.state == \"prepared\" and (.plan.drv_path | startswith(\"/nix/store/\")) and (.plan.lock_digest | length == 64)'", timeout=30)
    change_revision = machine.succeed(manager + f"changes.status --change-id {change} | jq -r .revision").strip()
    machine.succeed(manager + f"deploy.build --change-id {change} --expected-revision {change_revision} --idempotency-key deploy-build")
    machine.wait_until_succeeds(manager + f"changes.status --change-id {change} | jq -e '.state == \"ready\" and (.artifact.out_path | startswith(\"/nix/store/\"))'", timeout=60)
    change_revision = machine.succeed(manager + f"changes.status --change-id {change} | jq -r .revision").strip()
    # Service actions and deployment mutations share one target-local lock.
    # The build above remains independent, while activation waits for a slow restart.
    deployment_lock_restart = machine.succeed(manager + "units.restart --host fixture --unit maxops-managed.service --idempotency-key deployment-lock-restart | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {deployment_lock_restart} | jq -e '.handle.state == \"reconciling\"'", timeout=20)
    activation_job = machine.succeed(manager + f"deploy.activate --change-id {change} --expected-revision {change_revision} --idempotency-key deploy-activate | jq -r .job_id").strip()
    machine.succeed("sleep 1")
    machine.succeed(manager + f"jobs.status --job-id {activation_job} | jq -e '.handle.state == \"queued\" or .handle.state == \"dispatching\"'")
    machine.succeed(f"test \"$(readlink /run/maxops-fixture-current)\" = {baseline}")
    machine.wait_until_succeeds(manager + f"jobs.status --job-id {deployment_lock_restart} | jq -e '.handle.state == \"succeeded\"'", timeout=30)
    machine.wait_until_succeeds(manager + f"changes.status --change-id {change} | jq -e '.state == \"verifying\"'", timeout=30)
    change_revision = machine.succeed(manager + f"changes.status --change-id {change} | jq -r .revision").strip()
    machine.succeed(manager + f"deploy.verify --change-id {change} --expected-revision {change_revision} --idempotency-key deploy-verify")
    machine.wait_until_succeeds(manager + f"changes.status --change-id {change} | jq -e '.state == \"succeeded\"'", timeout=30)
    deployed = machine.succeed(manager + f"changes.status --change-id {change} | jq -r .artifact.out_path").strip()
    machine.succeed(f"test \"$(readlink /run/maxops-fixture-current)\" = {deployed}")
    machine.succeed("grep -qx published /run/maxops-fixture-mode")

    # A later human Git push makes the built plan stale; maxops leaves runtime alone.
    stale_source = machine.succeed(manager + f"deploy.prepare --repository fixture --workspace-id {workspace2} --expected-revision 4 --target-host fixture --profile fixture-system --idempotency-key deploy-stale-source-prepare | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"changes.status --change-id {stale_source} | jq -e '.state == \"prepared\"'", timeout=30)
    stale_source_revision = machine.succeed(manager + f"changes.status --change-id {stale_source} | jq -r .revision").strip()
    machine.succeed(manager + f"deploy.build --change-id {stale_source} --expected-revision {stale_source_revision} --idempotency-key deploy-stale-source-build")
    machine.wait_until_succeeds(manager + f"changes.status --change-id {stale_source} | jq -e '.state == \"ready\"'", timeout=60)
    machine.succeed("git -C /tmp/external pull --ff-only && printf later\\n > /tmp/external/later.txt && git -C /tmp/external add later.txt && git -C /tmp/external -c user.name=External -c user.email=external@example.invalid commit -m later && git -C /tmp/external push origin main")
    stale_source_revision = machine.succeed(manager + f"changes.status --change-id {stale_source} | jq -r .revision").strip()
    machine.fail(manager + f"deploy.activate --change-id {stale_source} --expected-revision {stale_source_revision} --idempotency-key deploy-stale-source-activate")
    machine.succeed(manager + f"changes.status --change-id {stale_source} | jq -e '.state == \"stale\"'")
    machine.succeed(f"test \"$(readlink /run/maxops-fixture-current)\" = {deployed}")

    # A manual rebuild/profile switch is equally valid and blocks an old plan.
    stale_runtime = machine.succeed(manager + f"deploy.prepare --repository fixture --workspace-id {workspace2} --expected-revision 4 --target-host fixture --profile fixture-system --idempotency-key deploy-stale-runtime-prepare | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"changes.status --change-id {stale_runtime} | jq -e '.state == \"prepared\"'", timeout=30)
    stale_runtime_revision = machine.succeed(manager + f"changes.status --change-id {stale_runtime} | jq -r .revision").strip()
    machine.succeed(manager + f"deploy.build --change-id {stale_runtime} --expected-revision {stale_runtime_revision} --idempotency-key deploy-stale-runtime-build")
    machine.wait_until_succeeds(manager + f"changes.status --change-id {stale_runtime} | jq -e '.state == \"ready\"'", timeout=60)
    machine.succeed(f"${pkgs.nix}/bin/nix-env -p /var/lib/maxops-fixture-deploy/profile --set {baseline}")
    machine.succeed(f"{baseline}/bin/switch-to-configuration switch")
    stale_runtime_revision = machine.succeed(manager + f"changes.status --change-id {stale_runtime} | jq -r .revision").strip()
    machine.fail(manager + f"deploy.activate --change-id {stale_runtime} --expected-revision {stale_runtime_revision} --idempotency-key deploy-stale-runtime-activate")
    machine.succeed(manager + f"changes.status --change-id {stale_runtime} | jq -e '.state == \"stale\"'")
    machine.succeed(f"test \"$(readlink /run/maxops-fixture-current)\" = {baseline}")

    # A failing activation may roll back only while its own output still owns the profile.
    machine.succeed(manager + "workspace.create --repository fixture --idempotency-key bad-workspace-create --wait > /tmp/bad-workspace.json")
    bad_workspace = machine.succeed("jq -r .result.workspace.workspace_id /tmp/bad-workspace.json").strip()
    machine.succeed(f"cat > /tmp/bad-apply.json <<'EOF'\n{{\"repository\":\"fixture\",\"workspace_id\":\"{bad_workspace}\",\"expected_revision\":1,\"edits\":[{{\"path\":\"config.txt\",\"content\":\"bad\\n\"}}]}}\nEOF")
    machine.succeed(manager + "workspace.apply --params-file /tmp/bad-apply.json")
    machine.succeed(manager + f"workspace.commit --repository fixture --workspace-id {bad_workspace} --expected-revision 2 --message 'bad deployment fixture'")
    bad_change = machine.succeed(manager + f"deploy.prepare --repository fixture --workspace-id {bad_workspace} --expected-revision 3 --target-host fixture --profile fixture-system --idempotency-key bad-deploy-prepare | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"changes.status --change-id {bad_change} | jq -e '.state == \"prepared\"'", timeout=30)
    bad_revision = machine.succeed(manager + f"changes.status --change-id {bad_change} | jq -r .revision").strip()
    machine.succeed(manager + f"deploy.build --change-id {bad_change} --expected-revision {bad_revision} --idempotency-key bad-deploy-build")
    machine.wait_until_succeeds(manager + f"changes.status --change-id {bad_change} | jq -e '.state == \"ready\"'", timeout=60)
    bad_revision = machine.succeed(manager + f"changes.status --change-id {bad_change} | jq -r .revision").strip()
    machine.succeed(manager + f"deploy.activate --change-id {bad_change} --expected-revision {bad_revision} --idempotency-key bad-deploy-activate")
    machine.wait_until_succeeds(manager + f"changes.status --change-id {bad_change} | jq -e '.state == \"rolled_back\"'", timeout=30)
    machine.succeed(f"test \"$(readlink /run/maxops-fixture-current)\" = {baseline}")
    machine.succeed("grep -qx baseline /run/maxops-fixture-mode")

    # A manual activation during maxops verification takes ownership and blocks rollback.
    machine.succeed(manager + "workspace.create --repository fixture --idempotency-key superseded-workspace-create --wait > /tmp/superseded-workspace.json")
    superseded_workspace = machine.succeed("jq -r .result.workspace.workspace_id /tmp/superseded-workspace.json").strip()
    machine.succeed(f"cat > /tmp/superseded-apply.json <<'EOF'\n{{\"repository\":\"fixture\",\"workspace_id\":\"{superseded_workspace}\",\"expected_revision\":1,\"edits\":[{{\"path\":\"config.txt\",\"content\":\"superseded\\n\"}}]}}\nEOF")
    machine.succeed(manager + "workspace.apply --params-file /tmp/superseded-apply.json")
    machine.succeed(manager + f"workspace.commit --repository fixture --workspace-id {superseded_workspace} --expected-revision 2 --message 'superseded deployment fixture'")
    superseded_change = machine.succeed(manager + f"deploy.prepare --repository fixture --workspace-id {superseded_workspace} --expected-revision 3 --target-host fixture --profile fixture-system --idempotency-key superseded-deploy-prepare | jq -r .job_id").strip()
    machine.wait_until_succeeds(manager + f"changes.status --change-id {superseded_change} | jq -e '.state == \"prepared\"'", timeout=30)
    superseded_revision = machine.succeed(manager + f"changes.status --change-id {superseded_change} | jq -r .revision").strip()
    machine.succeed(manager + f"deploy.build --change-id {superseded_change} --expected-revision {superseded_revision} --idempotency-key superseded-deploy-build")
    machine.wait_until_succeeds(manager + f"changes.status --change-id {superseded_change} | jq -e '.state == \"ready\"'", timeout=60)
    superseded_revision = machine.succeed(manager + f"changes.status --change-id {superseded_change} | jq -r .revision").strip()
    machine.succeed(manager + f"deploy.activate --change-id {superseded_change} --expected-revision {superseded_revision} --idempotency-key superseded-deploy-activate")
    machine.wait_until_succeeds("grep -qx superseded /run/maxops-fixture-mode", timeout=20)
    machine.succeed(f"${pkgs.nix}/bin/nix-env -p /var/lib/maxops-fixture-deploy/profile --set {external_deployment}")
    machine.succeed(f"{external_deployment}/bin/switch-to-configuration switch")
    machine.wait_until_succeeds(manager + f"changes.status --change-id {superseded_change} | jq -e '.state == \"superseded\"'", timeout=30)
    machine.succeed(f"test \"$(readlink /run/maxops-fixture-current)\" = {external_deployment}")
    machine.succeed("grep -qx external /run/maxops-fixture-mode")
  '';
}
