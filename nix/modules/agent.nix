self:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.maxops-agent;
  configFile = (pkgs.formats.json { }).generate "maxops-agent.json" {
    host = cfg.hostName;
    listen = "${cfg.listenAddress}:${toString cfg.port}";
    token_file = "/run/credentials/maxops-agent.service/token";
    execution_token_file =
      if cfg.execution.enable then "/run/credentials/maxops-agent.service/execution-token" else null;
    executor_socket = if cfg.execution.enable then cfg.execution.socketPath else null;
    readable_units = cfg.readableUnits;
    read_all_units = cfg.readAllUnits;
    manageable_units = cfg.manageableUnits;
    allow_logs = cfg.allowLogs;
    journalctl = "${pkgs.systemd}/bin/journalctl";
  };
in
{
  options.services.maxops-agent = {
    enable = lib.mkEnableOption "the read-only maxops Linux agent";
    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      description = "Package containing maxops-agent.";
    };
    hostName = lib.mkOption {
      type = lib.types.str;
      default = config.networking.hostName;
      description = "Inventory identity reported by this agent.";
    };
    listenAddress = lib.mkOption {
      type = lib.types.str;
      default = "127.0.0.1";
      description = "Explicit bind address; bracket IPv6 literals. Use a tailnet address for remote access.";
    };
    port = lib.mkOption {
      type = lib.types.port;
      default = 9720;
      description = "Agent HTTP port.";
    };
    tokenFile = lib.mkOption {
      type = lib.types.str;
      description = "Absolute runtime path to the agent token. Never put token contents in the Nix store.";
    };
    readAllUnits = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Observe every loaded systemd unit and read any exact unit's status/logs; grants no service mutations.";
    };
    readableUnits = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "Exact .service names exposed by status and optional log queries.";
    };
    manageableUnits = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "Exact readable services whose management jobs may be forwarded.";
    };
    allowLogs = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Allow bounded journal queries. Grants the process access to the system journal group.";
    };
    execution = {
      enable = lib.mkEnableOption "forwarding authenticated management jobs to the local executor";
      tokenFile = lib.mkOption {
        type = lib.types.str;
        default = "";
        description = "Dedicated runtime credential accepted only by the management endpoint.";
      };
      socketPath = lib.mkOption {
        type = lib.types.str;
        default = "/run/maxops-executor/control.sock";
        description = "Unix socket exposed by the local maxops executor.";
      };
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion =
          !(builtins.elem cfg.listenAddress [
            "0.0.0.0"
            "::"
            "[::]"
          ]);
        message = "maxops-agent requires an explicit bind address.";
      }
      {
        assertion = lib.hasPrefix "/" cfg.tokenFile && !(lib.hasPrefix builtins.storeDir cfg.tokenFile);
        message = "maxops-agent.tokenFile must be a runtime path outside the Nix store.";
      }
      {
        assertion =
          !cfg.execution.enable
          || (
            lib.hasPrefix "/" cfg.execution.tokenFile
            && !(lib.hasPrefix builtins.storeDir cfg.execution.tokenFile)
          );
        message = "maxops-agent.execution.tokenFile must be a runtime path outside the Nix store.";
      }
      {
        assertion =
          cfg.readAllUnits || lib.all (unit: builtins.elem unit cfg.readableUnits) cfg.manageableUnits;
        message = "maxops-agent manageableUnits must be a subset of readableUnits.";
      }
    ];
    users.groups.maxops-executor = lib.mkIf cfg.execution.enable { };
    systemd.services.maxops-agent = {
      description = "maxops host agent";
      wantedBy = [ "multi-user.target" ];
      after = [
        "network-online.target"
        "tailscaled.service"
      ]
      ++ lib.optional cfg.execution.enable "maxops-executor.service";
      wants = [ "network-online.target" ] ++ lib.optional cfg.execution.enable "maxops-executor.service";
      environment.RUST_LOG = "info";
      serviceConfig = {
        ExecStart = "${cfg.package}/bin/maxops-agent --config ${configFile}";
        DynamicUser = true;
        LoadCredential = [
          "token:${cfg.tokenFile}"
        ]
        ++ lib.optional cfg.execution.enable "execution-token:${cfg.execution.tokenFile}";
        SupplementaryGroups =
          lib.optional cfg.allowLogs "systemd-journal" ++ lib.optional cfg.execution.enable "maxops-executor";
        Restart = "on-failure";
        RestartSec = "10s";
        TimeoutStopSec = "15s";
        NoNewPrivileges = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
        PrivateDevices = true;
        ProtectKernelTunables = true;
        ProtectKernelModules = true;
        ProtectControlGroups = true;
        RestrictSUIDSGID = true;
        RestrictAddressFamilies = [
          "AF_UNIX"
          "AF_INET"
          "AF_INET6"
        ];
        CapabilityBoundingSet = "";
        LockPersonality = true;
        UMask = "0077";
      };
    };
  };
}
