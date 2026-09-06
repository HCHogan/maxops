{ pkgs, self }:
pkgs.testers.runNixOSTest {
  name = "maxops-read-only";
  nodes.machine = { pkgs, ... }: {
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
    ];
    systemd.tmpfiles.rules = [
      "f /run/agent-token 0400 root root - agent-test-token-aaaaaaaaaaaaaaaaaaaaaa"
      "f /run/client-token 0400 root root - client-test-token-bbbbbbbbbbbbbbbbbbbbb"
      "f /run/execution-token 0400 root root - execution-test-token-cccccccccccccccccc"
      "f /run/manager-token 0400 root root - manager-test-token-dddddddddddddddddddd"
      "f /run/manager2-token 0400 root root - manager2-test-token-eeeeeeeeeeeeeeeeeeeee"
      "f /run/job-credential 0400 root root - fixture-credential-value"
    ];
    services.maxops-executor = {
      enable = true;
      manageableUnits = [
        "maxops-managed.service"
        "maxops-no-reload.service"
      ];
      credentialSources.fixture = "/run/job-credential";
      profiles.diagnostic = {
        timeoutSeconds = 30;
        outputLimitBytes = 64;
        tasksMax = 32;
        memoryMaxBytes = 268435456;
        allowedCredentials = [ "fixture" ];
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
          ];
          repositories = [ "fixture" ];
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
    };
    systemd.services.maxops-executor.preStart = ''
      if ! ${pkgs.git}/bin/git --git-dir=/var/lib/maxops-executor/fixture-remote.git rev-parse --verify refs/heads/main >/dev/null 2>&1; then
        rm -rf /var/lib/maxops-executor/fixture-remote.git /var/lib/maxops-executor/fixture-seed
        ${pkgs.git}/bin/git init --bare --initial-branch=main /var/lib/maxops-executor/fixture-remote.git
        ${pkgs.git}/bin/git init --initial-branch=main /var/lib/maxops-executor/fixture-seed
        printf 'initial\n' > /var/lib/maxops-executor/fixture-seed/config.txt
        ln -s /etc/shadow /var/lib/maxops-executor/fixture-seed/escape
        ${pkgs.git}/bin/git -C /var/lib/maxops-executor/fixture-seed add config.txt escape
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
  };
  testScript = ''
    start_all()
    machine.wait_for_unit("maxops-agent.service")
    machine.wait_for_unit("maxops-executor.service")
    machine.wait_for_unit("maxops-hub.service")
    machine.wait_for_open_port(9721)
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
    machine.succeed("echo '{\"host\":\"fixture\",\"profile\":\"diagnostic\",\"command\":{\"argv\":[\"${pkgs.bash}/bin/bash\",\"-c\",\"printf first; sleep 12; printf second\"]},\"timeout_seconds\":30}' > /tmp/long-job.json")
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
  '';
}
