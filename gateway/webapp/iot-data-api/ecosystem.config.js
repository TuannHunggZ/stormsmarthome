module.exports = {
  apps : [{
    name: "iot-data-api",
    script: 'index.js',
    watch: '.',
    watch_delay: 1000,
    ignore_watch : ["node_modules", "public/"]
  }],

  deploy : {
    production : {
      user : 'SSH_USERNAME',
      host : 'SSH_HOSTMACHINE',
      ref  : 'origin/master',
      repo : 'GIT_REPOSITORY',
      path : 'DESTINATION_PATH',
      'pre-deploy-local': '',
      'post-deploy' : 'npm install && pm2 reload ecosystem.config.js --env production',
      'pre-setup': ''
    }
  }
};
