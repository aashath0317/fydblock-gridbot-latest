module.exports = {
  apps: [{
    name: "gridbot",
    script: "./server.py",
    interpreter: "/root/venv/bin/python",
    env: {
      NODE_ENV: "production",
      PYTHONPATH: "."
    }
  }]
}
