/**
 *   cargo build --release
 *   pm2 start ecosystem.config.cjs
 *   pm2 logs polybot
 */
const path = require("path");
const ROOT = __dirname;

module.exports = {
  apps: [
    {
      name: "polybot",
      cwd: ROOT,
      script: path.join(ROOT, "target", "release", "polybot"),
      interpreter: "none",
      instances: 1,
      exec_mode: "fork",
      autorestart: false,
      watch: false,
      max_restarts: 50,
      min_uptime: "5s",
      restart_delay: 200,
      kill_timeout: 4000,
      out_file: path.join(ROOT, "logs", "pm2-out.log"),
      error_file: path.join(ROOT, "logs", "pm2-error.log"),
      merge_logs: true,
      time: false,
      env_file: path.join(ROOT, ".env"),
      env: {
        RUST_LOG: "info,polybot=debug",
        FORCE_COLOR: "1",
      },
    },
  ],
};
