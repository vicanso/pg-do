const Table = require('./table');

const clientSym = Symbol('client');
const schemaSym = Symbol('schema');

class Transaction {
  constructor(client, schema) {
    if (!client || !schema) {
      throw new Error('client and schema can not be null');
    }
    this[clientSym] = client;
    this[schemaSym] = schema;
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
    return new Table(this.client, name, this[schemaSym][name]);
  }
}

module.exports = Transaction;
