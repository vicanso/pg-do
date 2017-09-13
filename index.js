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
const schemaSym = Symbol('schema');
const constraintSym = Symbol('constraint');

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
    const max = Number.parseInt(urlInfo.searchParams.get('max'), 10);
    if (!_.isNaN(max)) {
      options.max = max;
    }
    const idleTimeout = Number.parseInt(urlInfo.searchParams.get('idleTimeout'), 10);
    if (!_.isNaN(idleTimeout)) {
      options.idleTimeoutMillis = idleTimeout;
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
    this[schemaSym] = {};
    this[constraintSym] = {};
  }
  get pool() {
    return this[poolSym];
  }
  /**
   * 增加table的schema
   *
   * @param {any} table
   * @param {any} schema
   * @param {any} constraint
   * @return {PG}
   * @memberof PG
   */
  addSchema(table, schema, constraint) {
    if (!table || !schema) {
      throw new Error('table and schema can not be null');
    }
    this[schemaSym][table] = schema;
    if (constraint) {
      this[constraintSym][table] = constraint;
    }
    return this;
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
    return new Table(this, name, this[schemaSym][name], this[constraintSym][name]);
  }
  /**
   * 获取用于transaction
   *
   * @memberof PG
   */
  async transaction() {
    const client = await this.pool.connect();
    return new Transaction(client, this[schemaSym]);
  }
}

module.exports = PG;
