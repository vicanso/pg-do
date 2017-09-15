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
const hookSym = Symbol('hook');

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
    this[hookSym] = {};
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
    const table = new Table(this, name, this[schemaSym][name]);
    const constraint = this[constraintSym][name];
    if (constraint) {
      table.constraint = constraint;
    }
    const hook = this[hookSym];
    if (hook) {
      table.hook = hook;
    }
    return table;
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
  /**
   * 增加对不同的op的hook函数
   *
   * @param {any} op - insert, update等
   * @param {any} fn - hook函数
   * @returns
   * @memberof PG
   */
  hook(op, fn) {
    const hooks = this[hookSym];
    if (!hooks[op]) {
      hooks[op] = [];
    }
    hooks[op].push(fn);
    return this;
  }
}

module.exports = PG;
