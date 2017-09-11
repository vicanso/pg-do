const Table = require('./table');

const clientSym = Symbol('client');

class Transaction {
  constructor(client) {
    if (!client) {
      throw new Error('client can not be null');
    }
    this[clientSym] = client;
  }
  get client() {
    return this[clientSym];
  }
  begin() {
    return this.client.query('BEGIN');
  }
  commit() {
    return this.client.query('COMMIT');
  }
  rollback() {
    return this.client.query('ROLLBACK');
  }
  release() {
    return this.client.release();
  }
  /**
   * 获取Table实例
   *
   * @param {any} name
   * @returns
   * @memberof PG
   */
  getTable(name) {
    return new Table(this.client, name);
  }
}

module.exports = Transaction;
