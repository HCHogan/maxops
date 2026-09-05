{ pkgs, self }:
pkgs.testers.runNixOSTest {
  name = "maxops-read-only";
  nodes.machine = { pkgs, ... }: {
    imports = [ self.nixosModules.default ];
    networking.hostName = "fixture";
    environment.systemPackages = [
      self.packages.${pkgs.stdenv.hostPlatform.system}.default
      pkgs.curl
      pkgs.jq
    ];
    systemd.tmpfiles.rules = [
      "f /run/agent-token 0400 root root - agent-test-token-aaaaaaaaaaaaaaaaaaaaaa"
      "f /run/client-token 0400 root root - client-test-token-bbbbbbbbbbbbbbbbbbbbb"
    ];
    services.maxops-agent = {
      enable = true;
      tokenFile = "/run/agent-token";
      readableUnits = [ "maxops-fixture.service" ];
      allowLogs = true;
    };
    services.maxops-hub = {
      enable = true;
      hosts = [
        {
          name = "fixture";
          agentUrl = "http://127.0.0.1:9720";
          tokenFile = "/run/agent-token";
          readableUnits = [ "maxops-fixture.service" ];
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
      ];
    };
    systemd.services.maxops-fixture = {
      wantedBy = [ "multi-user.target" ];
      serviceConfig.Type = "oneshot";
      script = ''
        echo maxops-journal-fixture
        exit 1
      '';
    };
  };
  testScript = ''
    start_all()
    machine.wait_for_unit("maxops-agent.service")
    machine.wait_for_unit("maxops-hub.service")
    machine.wait_for_open_port(9721)
    machine.succeed("curl -fsS http://127.0.0.1:9720/healthz")
    machine.fail("curl -fsS http://127.0.0.1:9720/v1/snapshot")
    ctl = "maxopsctl --token-file /run/client-token "
    machine.succeed(ctl + "host.facts --host fixture | jq -e '.facts.system_closure | startswith(\"/nix/store/\")'")
    machine.succeed(ctl + "units.failed | jq -e '.hosts[0].units[0].unit == \"maxops-fixture.service\"'")
    machine.succeed(ctl + "units.logs --host fixture --unit maxops-fixture.service | jq -e '[.entries[].message] | any(contains(\"maxops-journal-fixture\"))'")
    machine.fail(ctl + "units.logs --host fixture --unit sshd.service")
    machine.fail("runuser -u maxops-agent -- systemctl --no-ask-password restart maxops-fixture.service")
  '';
}
