self:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.maxops-executor;
  workspaceRoot = "/var/lib/maxops-workspaces";
  profiles = lib.mapAttrs (name: profile: {
    inherit (profile) user environment privileged;
    interpreter = toString profile.interpreter;
    timeout_seconds = profile.timeoutSeconds;
    output_limit_bytes = profile.outputLimitBytes;
    working_roots = profile.workingRoots ++ lib.optional (
      lib.any (repository: repository.checkProfile == name) (lib.attrValues cfg.repositories)
      || lib.any (deployment: deployment.buildProfile == name) (lib.attrValues cfg.deploymentProfiles)
    ) workspaceRoot;
    allowed_credentials = profile.allowedCredentials;
    tasks_max = profile.tasksMax;
    memory_max_bytes = profile.memoryMaxBytes;
  }) cfg.profiles;
  repositories = lib.mapAttrs (_: repository: {
    url = repository.url;
    default_ref = repository.defaultRef;
    publish_refs = repository.publishRefs;
    check_profile = repository.checkProfile;
    checks = repository.checks;
    author_name = repository.authorName;
    author_email = repository.authorEmail;
  }) cfg.repositories;
  deploymentProfiles = lib.mapAttrs (_: deployment: {
    repository = deployment.repository;
    target_host = deployment.targetHost;
    kind = deployment.kind;
    flake_attribute = deployment.flakeAttribute;
    build_profile = deployment.buildProfile;
    activate_profile = deployment.activateProfile;
    verify_profile = deployment.verifyProfile;
    profile_path = deployment.profilePath;
    running_link = deployment.runningLink;
    activation_program = deployment.activationProgram;
    activation_arguments = deployment.activationArguments;
    artifact_source = deployment.artifactSource;
    verify_commands = deployment.verifyCommands;
    verify_attempts = deployment.verifyAttempts;
    verify_interval_seconds = deployment.verifyIntervalSeconds;
    automatic_rollback = deployment.automaticRollback;
  }) cfg.deploymentProfiles;
  configFile = (pkgs.formats.json { }).generate "maxops-executor.json" {
    host = cfg.hostName;
    socket_path = cfg.socketPath;
    state_file = "/var/lib/maxops-executor/state.db";
    spec_directory = "/var/lib/maxops-executor/specs";
    spool_root = "/var/lib/maxops-jobs";
    systemd_run = "${pkgs.systemd}/bin/systemd-run";
    systemctl = "${pkgs.systemd}/bin/systemctl";
    runner = "${cfg.package}/bin/maxops-job-runner";
    deploy_runner = "${cfg.package}/bin/maxops-deploy-runner";
    git = "${pkgs.git}/bin/git";
    nix = "${pkgs.nix}/bin/nix";
    nix_env = "${pkgs.nix}/bin/nix-env";
    workspace_root = workspaceRoot;
    repository_root = "/var/lib/maxops-executor/repositories";
    manageable_units = cfg.manageableUnits;
    credential_sources = cfg.credentialSources;
    profiles = profiles;
    repositories = repositories;
    deployment_profiles = deploymentProfiles;
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
    repositories = lib.mkOption {
      default = { };
      description = "Configured Git repositories and server-owned checks.";
      type = lib.types.attrsOf (
        lib.types.submodule {
          options = {
            url = lib.mkOption {
              type = lib.types.str;
              description = "Trusted fetch and publish URL; API callers cannot override it.";
            };
            defaultRef = lib.mkOption {
              type = lib.types.str;
              default = "refs/heads/main";
              description = "Configured branch used as the workspace base.";
            };
            publishRefs = lib.mkOption {
              type = lib.types.listOf lib.types.str;
              default = [ ];
              description = "Exact branch refs that workspace commits may publish.";
            };
            checkProfile = lib.mkOption {
              type = lib.types.str;
              default = "diagnostic";
              description = "Unprivileged execution profile used for repository checks.";
            };
            checks = lib.mkOption {
              type = lib.types.attrsOf (lib.types.listOf lib.types.str);
              default = { };
              description = "Named structured argv checks available through workspace.check.";
            };
            authorName = lib.mkOption {
              type = lib.types.str;
              default = "maxops";
              description = "Server-controlled Git author and committer name.";
            };
            authorEmail = lib.mkOption {
              type = lib.types.str;
              default = "maxops@localhost";
              description = "Server-controlled Git author and committer email.";
            };
          };
        }
      );
    };
    deploymentProfiles = lib.mkOption {
      default = { };
      description = "Server-owned Nix build, activation, verification, and recovery policies.";
      type = lib.types.attrsOf (
        lib.types.submodule {
          options = {
            repository = lib.mkOption { type = lib.types.str; };
            targetHost = lib.mkOption { type = lib.types.str; };
            kind = lib.mkOption {
              type = lib.types.enum [ "system" "home" ];
              default = "system";
            };
            flakeAttribute = lib.mkOption { type = lib.types.str; };
            buildProfile = lib.mkOption {
              type = lib.types.str;
              default = "diagnostic";
            };
            activateProfile = lib.mkOption { type = lib.types.str; };
            verifyProfile = lib.mkOption {
              type = lib.types.str;
              default = "diagnostic";
            };
            profilePath = lib.mkOption {
              type = lib.types.str;
              default = "/nix/var/nix/profiles/system";
            };
            runningLink = lib.mkOption {
              type = lib.types.str;
              default = "/run/current-system";
            };
            activationProgram = lib.mkOption {
              type = lib.types.str;
              default = "bin/switch-to-configuration";
            };
            activationArguments = lib.mkOption {
              type = lib.types.listOf lib.types.str;
              default = [ "switch" ];
            };
            artifactSource = lib.mkOption {
              type = lib.types.nullOr lib.types.str;
              default = null;
              description = "Optional trusted Nix copy source when the target lacks the built output.";
            };
            verifyCommands = lib.mkOption {
              type = lib.types.listOf (lib.types.listOf lib.types.str);
              default = [ ];
              description = "Target-local structured argv acceptance checks.";
            };
            verifyAttempts = lib.mkOption {
              type = lib.types.ints.positive;
              default = 3;
            };
            verifyIntervalSeconds = lib.mkOption {
              type = lib.types.ints.positive;
              default = 2;
            };
            automaticRollback = lib.mkOption {
              type = lib.types.bool;
              default = true;
            };
          };
        }
      );
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
      {
        assertion = lib.all (
          repository: builtins.hasAttr repository.checkProfile cfg.profiles
        ) (lib.attrValues cfg.repositories);
        message = "maxops executor repository checks must reference a configured profile.";
      }
      {
        assertion = lib.all (deployment:
          builtins.hasAttr deployment.buildProfile cfg.profiles
          && builtins.hasAttr deployment.activateProfile cfg.profiles
          && builtins.hasAttr deployment.verifyProfile cfg.profiles
        ) (lib.attrValues cfg.deploymentProfiles);
        message = "maxops executor deployment profiles must reference configured execution profiles.";
      }
      {
        assertion = lib.all (deployment:
          deployment.kind != "system"
          || cfg.profiles.${deployment.activateProfile}.privileged
        ) (lib.attrValues cfg.deploymentProfiles);
        message = "system deployment activation profiles must explicitly be privileged.";
      }
    ];

    users.groups.maxops-executor = { };
    users.groups.maxops-runner = { };
    users.groups.maxops-workspace = { };
    users.users.maxops-runner = {
      isSystemUser = true;
      group = "maxops-runner";
      extraGroups = [ "maxops-workspace" ];
    };

    systemd.tmpfiles.rules = [
      "d /var/lib/maxops-jobs 0711 root root - -"
      "d ${workspaceRoot} 2770 root maxops-workspace - -"
    ];
    systemd.services.maxops-executor = {
      description = "maxops durable local job executor";
      wantedBy = [ "multi-user.target" ];
      after = [ "dbus.service" ];
      environment.RUST_LOG = "info";
      serviceConfig = {
        ExecStart = "${cfg.package}/bin/maxops-executor --config ${configFile}";
        User = "root";
        Group = "maxops-executor";
        SupplementaryGroups = [ "maxops-workspace" ];
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
        RestrictAddressFamilies = [ "AF_UNIX" ] ++ lib.optionals (cfg.repositories != { }) [
          "AF_INET"
          "AF_INET6"
        ];
        ReadWritePaths = [ workspaceRoot ];
        CapabilityBoundingSet = "";
        LockPersonality = true;
        UMask = "0007";
      };
    };
  };
}
