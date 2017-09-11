const {
  Pool,
} = require('pg');
const {
  URL,
} = require('url');
const _ = require('lodash');
const EventEmitter = require('events');

const Table = require('./lib/table');
const Transaction = require('./lib/transaction');

const poolSym = Symbol('pool');

class PG extends EventEmitter {
  constructor(url) {
    super();
    const urlInfo = new URL(url);
    const options = {
      host: urlInfo.hostname,
      port: urlInfo.port,
    };
    if (urlInfo.username) {
      options.user = urlInfo.username;
      options.password = urlInfo.password;
    }
    options.database = urlInfo.pathname.substring(1);
    const pool = new Pool(options);
    const events = [
      'acquire',
      'remove',
      'error',
      'connect',
    ];
    _.forEach(events, (event) => {
      pool.on(event, (...args) => this.emit(event, ...args));
    });
    this[poolSym] = pool;
  }
  get pool() {
    return this[poolSym];
  }
  /**
   * postgres query function
   *
   * @param {any} args
   * @returns {Promise}
   * @memberof PG
   */
  query(...args) {
    return this.pool.query(...args);
  }
  /**
   * 获取Table实例
   *
   * @param {any} name
   * @returns
   * @memberof PG
   */
  getTable(name) {
    return new Table(this, name);
  }
  /**
   * 获取用于transaction
   *
   * @memberof PG
   */
  async transaction() {
    const client = await this.pool.connect();
    return new Transaction(client);
  }
}

module.exports = PG;
