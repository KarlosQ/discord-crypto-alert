require('dotenv').config()
const Discord = require('discord.js')
const fs = require('fs')
const config = require('./src/config')
const Server = require('./src/server')

//Load available exchanges
if (!config.exchanges || !config.exchanges.length) {
  config.exchanges = []

  fs.readdirSync('./src/exchanges/').forEach((file) => {
    ;/\.js$/.test(file) && config.exchanges.push(file.replace(/\.js$/, ''))
  })
}

const exchanges = []

for (let name of config.exchanges) {
  const exchange = new (require('./src/exchanges/' + name))(config)

  config.exchanges[config.exchanges.indexOf(name)] = exchange.id

  exchanges.push(exchange)
}

// DISCORD
const client = new Discord.Client()

client.on('ready', () => {
  console.log(`Logged in as ${client.user.tag}!`)
  // Start server
  const server = new Server(config, exchanges, client)
})

//make sure this line is the last line
client.login(process.env.CLIENT_TOKEN) //login bot using token

// Backup server on SIGINT
process.on('SIGINT', function () {
  console.log('\nSIGINT')

  /*   Promise.all([server.backupTrades(true)])
    .then((data) => { */
  console.log('[init] goodbye')

  process.exit()
  /*     })
    .catch((err) => {
      console.log(`[server/exit] Something went wrong when executing SIGINT script${err && err.message ? '\n\t' + err.message : ''}`)

      process.exit()
    }) */
})
