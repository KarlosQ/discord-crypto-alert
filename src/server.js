const EventEmitter = require('events')
const { getIp, getHms, parsePairsFromWsRequest, groupTrades, ago } = require('./helper')

function getIcon(side) {
  return side === 'sell' ? '🔻' : '⬆️'
}

function translateSide(side) {
  return side === 'sell' ? 'Venta' : 'Compra'
}

function isValidNumber(n) {
  return n && Number(n) && Number(n) % 1 === 0
}

function formatAmountToMillons(amount) {
  return `${Math.round(amount / 10000) / 100} millones`
}

function msToSeconds(ms) {
  return `${Math.round(ms / 1000)} segundos`
}

function secondsToMs(seconds) {
  return Math.round(seconds * 1000)
}

function amountToMillons(amount) {
  return Math.round(amount * 1000000)
}

function commandWithNumber(msg) {
  const text = msg.content.split(' ')[1]
  if (isValidNumber(text)) {
    const amount = parseInt(text, 10)
    if (amount < 1) {
      msg.reply('Cantidad insignificante, almenos 1')
    } else {
      return amount
    }
  } else {
    msg.reply('Valor no válido. Usar solo números')
  }
}

class Server extends EventEmitter {
  constructor(options, exchanges, discord) {
    super()

    this.accumulatedTime = 10000
    this.accumulatedLimit = 15000000
    this.bigDealLimit = 2000000
    this.channel = discord.channels.cache.find((channel) => channel.name === 'bot')
    this.accumulatedBuyAmount = 0
    this.accumulatedSellAmount = 0
    this.timmerAccumulated
    this.options = options
    this.exchanges = exchanges || []
    this.indexedProducts = {}
    this.storages = null

    /**
     * Raw trades ready to be persisted into storage next save
     * @type Trade[]
     */
    this.chunk = []

    /**
     * Keep track of all active connections (exchange + symbol)
     * @type {{[exchangeAndPair: string]: {exchange: string, pair: string, apiId: string, hit: number, ping: number}}}
     */
    this.connections = {}

    /**
     * delayedForBroadcast trades ready to be broadcasted next interval (see _broadcastDelayedTradesInterval)
     * @type Trade[]
     */
    this.delayedForBroadcast = []

    /**
     * active trades aggregations
     * @type {{[aggrIdentifier: string]: Trade}}
     */
    this.aggregating = {}

    /**
     * already aggregated trades ready to be broadcasted (see _broadcastAggregatedTradesInterval)
     * @type Trade[]
     */
    this.aggregated = []

    this.BANNED_IPS = []

    if (this.options.collect) {
      /*       console.log(
        `\n[server] collect is enabled`,
        this.options.broadcast && this.options.broadcastAggr
          ? '\n\twill aggregate every trades that came on same ms (impact only broadcast)'
          : '',
        this.options.broadcast && this.options.broadcastDebounce
          ? `\n\twill broadcast trades every ${this.options.broadcastDebounce}ms`
          : this.options.broadcast
          ? `will broadcast trades instantly`
          : ''
      )
      console.log(`\tconnect to -> ${this.exchanges.map((a) => a.id).join(', ')}`) */

      this.handleExchangesEvents()
      this.connectExchanges()

      // profile exchanges connections (keep alive)
      this._activityMonitoringInterval = setInterval(this.monitorExchangesActivity.bind(this, +new Date()), this.options.monitorInterval)
    }

    //DISCORD
    discord.on('message', (msg) => {
      if (msg.content === '!ping') {
        msg.reply('Pong!')
      }

      if (msg.content === '!help') {
        msg.reply(`

          Comandos:
          - !acumulado {millones}
          - !tiempo-acumulado {segundos}
          - !transaccion {millones}

          Ejemplo para un millon:
          !acumulado 1

          Ejemplo para 50 segundos
          !tiempo-acumulado 50

          No se admiten decimales
        `)
      }

      if (msg.content.startsWith('!acumulado')) {
        const amount = commandWithNumber(msg)
        if (amount) {
          this.accumulatedLimit = amountToMillons(amount)
          msg.reply(`Acumulado cambiado a ${amount} millones`)
        }
      }

      if (msg.content.startsWith('!tiempo-acumulado')) {
        const amount = commandWithNumber(msg)
        if (amount) {
          this.accumulatedTime = secondsToMs(amount)
          msg.reply(`tiempo acumulado cambiado a ${amount} segundos`)
        }
      }

      if (msg.content.startsWith('!transaccion')) {
        const amount = commandWithNumber(msg)
        if (amount) {
          this.bigDealLimit = amountToMillons(amount)
          msg.reply(`transaccion importante cambiada a ${amount} millones`)
        }
      }
    })

    this.channel.send(`
      🤖 Crypto tracking bot iniciado. Valores por defecto:
      tiempo-acumulado: ${msToSeconds(this.accumulatedTime)}
      acumulado: ${formatAmountToMillons(this.accumulatedLimit)}
      transaccion: ${formatAmountToMillons(this.bigDealLimit)}
  `)
  }

  isBigDeal(amount) {
    if (amount > this.bigDealLimit) return true

    return false
  }

  isImportantAccumulated(amount) {
    if (amount > this.accumulatedLimit) return true

    return false
  }

  handleExchangesEvents() {
    this.exchanges.forEach((exchange) => {
      if (this.options.broadcast && this.options.broadcastAggr) {
        exchange.on('trades', this.dispatchAggregateTrade.bind(this, exchange.id))
      } else {
        exchange.on('trades', this.dispatchRawTrades.bind(this, exchange.id))
      }

      exchange.on('liquidations', this.dispatchRawTrades.bind(this, exchange.id))

      exchange.on('index', (pairs) => {
        for (let pair of pairs) {
          if (this.indexedProducts[pair]) {
            this.indexedProducts[pair].count++

            if (this.indexedProducts[pair].exchanges.indexOf(exchange.id) === -1) {
              this.indexedProducts[pair].exchanges.push(exchange.id)
            }
          } else {
            this.indexedProducts[pair] = {
              value: pair,
              count: 1,
              exchanges: [exchange.id],
            }
          }
        }

        // this.dumpSymbolsByExchanges()
      })

      exchange.on('disconnected', (pair, apiId) => {
        const id = exchange.id + ':' + pair

        if (!this.connections[id]) {
          console.error(`[server] couldn't delete connection ${id} because the connections[${id}] does not exists`)
          return
        }

        // console.log(`[server] deleted connection ${id}`)

        delete this.connections[id]

        // this.dumpConnections()
      })

      exchange.on('connected', (pair, apiId) => {
        const id = exchange.id + ':' + pair

        if (this.connections[id]) {
          console.error(`[server] couldn't register connection ${id} because the connections[${id}] does not exists`)
          return
        }

        //   console.log(`[server] registered connection ${id}`)

        const now = +new Date()

        this.connections[id] = {
          apiId,
          exchange: exchange.id,
          pair: pair,
          hit: 0,
          start: now,
          timestamp: now,
        }

        // this.dumpConnections()
      })
    })
  }

  dumpConnections(pingThreshold) {
    if (typeof this._dumpConnectionsTimeout !== 'undefined') {
      clearTimeout(this._dumpConnectionsTimeout)
      delete this._dumpConnectionsTimeout
    }

    const now = +new Date()

    this._dumpConnectionsTimeout = setTimeout(() => {
      const structPairs = {}
      const channels = {}

      for (let id in this.connections) {
        const connection = this.connections[id]

        if (pingThreshold && now - connection.timestamp < pingThreshold) {
          continue
        }

        if (!channels[connection.apiId]) {
          channels[connection.apiId] = 0
        }

        channels[connection.apiId]++

        structPairs[connection.exchange + ':' + connection.pair] = {
          apiId: connection.apiId,
          exchange: connection.exchange,
          pair: connection.pair,
          hit: connection.hit,
          avg: parseInt(((1000 * 60) / (now - connection.start)) * connection.hit) || 0,
          ping: connection.hit ? ago(connection.timestamp) : 'never',
        }
      }

      for (const market in structPairs) {
        const row = structPairs[market]
        const count = channels[row.apiId]

        row.thrs = getHms(Math.max(this.options.reconnectionThreshold / (0.5 + row.avg / count / 100), 1000 * 10), false, false)
        delete row.avg
        delete row.apiId
      }

      console.table(structPairs)
    }, 5000)

    return Promise.resolve()
  }

  connectExchanges() {
    if (!this.exchanges.length || !this.options.pairs.length) {
      return
    }

    this.chunk = []

    const exchangesProductsResolver = Promise.all(
      this.exchanges.map((exchange) => {
        const exchangePairs = this.options.pairs.filter(
          (pair) => pair.indexOf(':') === -1 || new RegExp('^' + exchange.id + ':').test(pair)
        )

        if (!exchangePairs.length) {
          return Promise.resolve()
        }

        return exchange.getProductsAndConnect(exchangePairs)
      })
    )

    /*     exchangesProductsResolver.then(() => {
      this.dumpConnections()
    })
 */
    if (this.options.broadcast && this.options.broadcastDebounce && !this.options.broadcastAggr) {
      this._broadcastDelayedTradesInterval = setInterval(() => {
        if (!this.delayedForBroadcast.length) {
          return
        }

        this.delayedForBroadcast = []
      }, this.options.broadcastDebounce || 1000)
    }
  }

  /**
   * Trigger subscribe to pairs on all activated exchanges
   * @param {string[]} pairs
   * @returns {Promise<any>} promises of connections
   * @memberof Server
   */
  async connectPairs(pairs) {
    //    console.log(`[server] connect to ${pairs.join(',')}`)

    const promises = []

    for (let exchange of this.exchanges) {
      for (let pair of pairs) {
        promises.push(
          exchange.link(pair).catch((err) => {
            //   console.debug(`[server/connectPairs/${exchange.id}] ${err}`)

            if (err instanceof Error) {
              console.error(err)
            }
          })
        )
      }
    }

    await Promise.all(promises)
  }

  /**
   * Trigger unsubscribe to pairs on all activated exchanges
   * @param {string[]} pairs
   * @returns {Promise<void>} promises of disconnection
   * @memberof Server
   */
  async disconnectPairs(pairs) {
    // console.log(`[server] disconnect from ${pairs.join(',')}`)

    for (let exchange of this.exchanges) {
      for (let pair of pairs) {
        try {
          await exchange.unlink(pair)
        } catch (err) {
          // console.debug(`[server/disconnectPairs/${exchange.id}] ${err}`)

          if (err instanceof Error) {
            console.error(err)
          }
        }
      }
    }
  }

  monitorExchangesActivity(startTime) {
    const now = +new Date()

    const sources = []
    const averages = {}
    const activity = {}
    const pairs = {}

    for (let id in this.connections) {
      const connection = this.connections[id]

      if (!activity[connection.apiId]) {
        activity[connection.apiId] = []
        pairs[connection.apiId] = []
        averages[connection.apiId] = ((1000 * 60) / (now - connection.start)) * connection.hit
      }

      activity[connection.apiId].push(now - connection.timestamp)
      pairs[connection.apiId].push(connection.pair)
    }

    for (let source in activity) {
      const minPing = activity[source].length ? Math.min.apply(null, activity[source]) : 0
      const threshold = Math.max(this.options.reconnectionThreshold / (0.5 + averages[source] / activity[source].length / 100), 1000 * 10)

      // console.log(`${averages[source] / activity[source].length}/min avg, ${minPing}/${threshold}ms : ${pairs[source].join(', ')}`)

      if (minPing > threshold) {
        // one of the feed did not received any data since 1m or more
        // => reconnect api (and all the feed binded to it)

        console.warn(
          `[warning] api ${source} reached reconnection threshold ${getHms(minPing)} > ${getHms(threshold)} (${
            averages[source] / activity[source].length
          } pings/min avg, min ${minPing})\n\t-> reconnect ${pairs[source].join(', ')}`
        )

        sources.push(source)
      }
    }

    for (let exchange of this.exchanges) {
      for (let api of exchange.apis) {
        if (sources.indexOf(api.id) !== -1) {
          exchange.reconnectApi(api)

          sources.splice(sources.indexOf(api.id), 1)
        }
      }
    }

    /*     const dumpConnections =
      (Math.floor((now - (startTime + this.options.monitorInterval)) / this.options.monitorInterval) * this.options.monitorInterval) %
        (this.options.monitorInterval * 60) ===
      0

    if (dumpConnections) {
      this.dumpConnections()
    } */
  }

  sendToDiscordClient(trade) {
    const amount = trade.price * trade.size

    if (this.isBigDeal(amount)) {
      this.channel.send(`
      ${getIcon(trade.side)} ${translateSide(trade.side)} significativa ${getIcon(trade.side)}
      exchange: ${trade.exchange}
      par: ${trade.pair}
      cantidad: ${formatAmountToMillons(amount)}
    `)
    }

    if (trade.side === 'sell') {
      this.accumulatedSellAmount += amount
    } else {
      this.accumulatedBuyAmount += amount
    }

    if (!this.timmerAccumulated) {
      this.timmerAccumulated = setInterval(() => {
        if (this.isImportantAccumulated(this.accumulatedSellAmount)) {
          this.channel.send(`

            🔻 Venta acumulada significativa en los ultimos ${msToSeconds(this.accumulatedTime)} 🔻
            par: BTC/USD
            cantidad: ${formatAmountToMillons(this.accumulatedSellAmount)}
          `)
        }

        if (this.isImportantAccumulated(this.accumulatedBuyAmount)) {
          this.channel.send(`

            ⬆️ Compra acumulada significativa en ultimos ${msToSeconds(this.accumulatedTime)} ⬆️
            par: BTC/USD
            cantidad: ${formatAmountToMillons(this.accumulatedBuyAmount)}
          `)
        }

        this.accumulatedBuyAmount = 0
        this.accumulatedSellAmount = 0
      }, this.accumulatedTime)
    }
  }

  dispatchRawTrades(exchange, { source, data }) {
    const now = +new Date()

    for (let i = 0; i < data.length; i++) {
      const trade = data[i]
      const identifier = exchange + ':' + trade.pair

      this.sendToDiscordClient(trade)

      if (!this.connections[identifier]) {
        // console.warn(`[${exchange}/dispatchRawTrades] connection ${identifier} doesn't exists but tried to dispatch a trade for it`)
        continue
      }

      // ping connection
      this.connections[identifier].hit++
      this.connections[identifier].timestamp = now

      // save trade
      if (this.storages) {
        this.chunk.push(trade)
      }
    }

    if (this.options.broadcast) {
      if (this.options.broadcastAggr && !this.options.broadcastDebounce) {
      } else {
        Array.prototype.push.apply(this.delayedForBroadcast, data)
      }
    }
  }

  dispatchAggregateTrade(exchange, { source, data }) {
    const now = +new Date()
    const length = data.length

    for (let i = 0; i < length; i++) {
      const trade = data[i]
      const identifier = exchange + ':' + trade.pair

      this.sendToDiscordClient(trade)

      // ping connection
      this.connections[identifier].hit++
      this.connections[identifier].timestamp = now

      // save trade
      if (this.storages) {
        this.chunk.push(trade)
      }

      if (!this.connections[identifier]) {
        console.error(`UNKNOWN TRADE SOURCE`, trade)
        //  console.info('This trade will be ignored.')
        continue
      }

      if (this.aggregating[identifier]) {
        const queuedTrade = this.aggregating[identifier]

        if (queuedTrade.timestamp === trade.timestamp && queuedTrade.side === trade.side) {
          queuedTrade.size += trade.size
          queuedTrade.price += trade.price * trade.size
          continue
        } else {
          queuedTrade.price /= queuedTrade.size
          this.aggregated.push(queuedTrade)
        }
      }

      this.aggregating[identifier] = Object.assign({}, trade)
      this.aggregating[identifier].timeout = now + 50
      this.aggregating[identifier].price *= this.aggregating[identifier].size
    }
  }

  broadcastAggregatedTrades() {
    const now = +new Date()

    const onGoingAggregation = Object.keys(this.aggregating)

    for (let i = 0; i < onGoingAggregation.length; i++) {
      const trade = this.aggregating[onGoingAggregation[i]]
      if (now > trade.timeout) {
        trade.price /= trade.size
        this.aggregated.push(trade)

        delete this.aggregating[onGoingAggregation[i]]
      }
    }

    if (this.aggregated.length) {
      this.aggregated.splice(0, this.aggregated.length)
    }
  }
}

module.exports = Server
