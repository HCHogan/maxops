self:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.maxops-executor;
  profiles = lib.mapAttrs (_: profile: {
    inherit (profile) user environment privileged;
    interpreter = toString profile.interpreter;
    timeout_seconds = profile.timeoutSeconds;
    output_limit_bytes = profile.outputLimitBytes;
    working_roots = profile.workingRoots;
    allowed_credentials = profile.allowedCredentials;
    tasks_max = profile.tasksMax;
    memory_max_bytes = profile.memoryMaxBytes;
  }) cfg.profiles;
  configFile = (pkgs.formats.json { }).generate "maxops-executor.json" {
    host = cfg.hostName;
    socket_path = cfg.socketPath;
    state_file = "/var/lib/maxops-executor/state.db";
    spec_directory = "/var/lib/maxops-executor/specs";
    spool_root = "/var/lib/maxops-jobs";
    systemd_run = "${pkgs.systemd}/bin/systemd-run";
    systemctl = "${pkgs.systemd}/bin/systemctl";
    runner = "${cfg.package}/bin/maxops-job-runner";
    manageable_units = cfg.manageableUnits;
    credential_sources = cfg.credentialSources;
    inherit profiles;
  };
in
{
  options.services.maxops-executor = {
    enable = lib.mkEnableOption "the privileged local maxops job executor";
    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      description = "Package containing maxops-executor and maxops-job-runner.";
    };
    hostName = lib.mkOption {
      type = lib.types.str;
      default = config.networking.hostName;
      description = "Inventory identity accepted by this executor.";
    };
    socketPath = lib.mkOption {
      type = lib.types.str;
      default = "/run/maxops-executor/control.sock";
      readOnly = true;
      description = "Local Unix socket shared with maxops-agent.";
    };
    profiles = lib.mkOption {
      default = { diagnostic = { }; };
      description = "Server-defined command execution profiles.";
      type = lib.types.attrsOf (
        lib.types.submodule {
          options = {
            user = lib.mkOption {
              type = lib.types.str;
              default = "maxops-runner";
              description = "Existing local account used by commands in this profile.";
            };
            interpreter = lib.mkOption {
              type = lib.types.path;
              default = pkgs.runtimeShell;
              description = "Explicit interpreter used only for script command specifications.";
            };
            timeoutSeconds = lib.mkOption {
              type = lib.types.ints.positive;
              default = 300;
              description = "Maximum command runtime for the profile.";
            };
            outputLimitBytes = lib.mkOption {
              type = lib.types.ints.positive;
              default = 16 * 1024 * 1024;
              description = "Maximum stored bytes for each output stream.";
            };
            workingRoots = lib.mkOption {
              type = lib.types.listOf lib.types.str;
              default = [ ];
              description = "Canonical directory roots permitted as an explicit cwd.";
            };
            environment = lib.mkOption {
              type = lib.types.attrsOf lib.types.str;
              default = {
                PATH = lib.makeBinPath [ pkgs.coreutils ];
              };
              description = "Non-secret base environment for the command.";
            };
            allowedCredentials = lib.mkOption {
              type = lib.types.listOf lib.types.str;
              default = [ ];
              description = "Credential source names this profile may request.";
            };
            privileged = lib.mkOption {
              type = lib.types.bool;
              default = false;
              description = "Permit the profile to run without the diagnostic filesystem sandbox.";
            };
            tasksMax = lib.mkOption {
              type = lib.types.ints.positive;
              default = 256;
              description = "Maximum process count in one transient job cgroup.";
            };
            memoryMaxBytes = lib.mkOption {
              type = lib.types.nullOr lib.types.ints.positive;
              default = null;
              description = "Optional cgroup memory limit for one job.";
            };
          };
        }
      );
    };
    credentialSources = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = { };
      description = "Runtime secret paths keyed by API credential reference name.";
    };
    manageableUnits = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "Exact systemd service names the executor may mutate over D-Bus.";
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = cfg.profiles != { };
        message = "maxops-executor requires at least one execution profile.";
      }
      {
        assertion = lib.all (profile: profile.user != "root" || profile.privileged) (
          lib.attrValues cfg.profiles
        );
        message = "a root maxops execution profile must explicitly set privileged = true.";
      }
      {
        assertion = lib.all (
          path: lib.hasPrefix "/" path && !(lib.hasPrefix builtins.storeDir path)
        ) (lib.attrValues cfg.credentialSources);
        message = "maxops executor credential sources must be runtime paths outside the Nix store.";
      }
      {
        assertion = lib.all (
          profile: lib.all (name: builtins.hasAttr name cfg.credentialSources) profile.allowedCredentials
        ) (lib.attrValues cfg.profiles);
        message = "maxops executor profiles may only allow declared credential source names.";
      }
    ];

    users.groups.maxops-executor = { };
    users.groups.maxops-runner = { };
    users.users.maxops-runner = {
      isSystemUser = true;
      group = "maxops-runner";
    };

    systemd.tmpfiles.rules = [ "d /var/lib/maxops-jobs 0711 root root - -" ];
    systemd.services.maxops-executor = {
      description = "maxops durable local job executor";
      wantedBy = [ "multi-user.target" ];
      after = [ "dbus.service" ];
      environment.RUST_LOG = "info";
      serviceConfig = {
        ExecStart = "${cfg.package}/bin/maxops-executor --config ${configFile}";
        User = "root";
        Group = "maxops-executor";
        RuntimeDirectory = "maxops-executor";
        RuntimeDirectoryMode = "0770";
        StateDirectory = "maxops-executor";
        StateDirectoryMode = "0700";
        Restart = "on-failure";
        RestartSec = "2s";
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
        RestrictAddressFamilies = [ "AF_UNIX" ];
        CapabilityBoundingSet = "";
        LockPersonality = true;
        UMask = "0007";
      };
    };
  };
}
