const _ = require('lodash');

const SQL = require('./sql');

const clientSym = Symbol('client');
const tableSym = Symbol('table');
const schemaSym = Symbol('schema');
const constraintSym = Symbol('constraint');
const hookSym = Symbol('hook');

/**
 * 根据is函数从args中获取所需参数，如果没有符合的，则返回defaultValue
 *
 * @param {any} args
 * @param {any} is
 * @param {any} defaultValue
 * @returns
 */
function getParam(args, is, defaultValue) {
  let result;
  args.forEach((v) => {
    if (!_.isUndefined(result)) {
      return;
    }
    if (is(v)) {
      result = v;
    }
  });
  if (_.isUndefined(result)) {
    result = defaultValue;
  }
  return result;
}

class Table {
  constructor(client, table, schema) {
    if (!client || !table) {
      throw new Error('client and table can not be null');
    }
    this[clientSym] = client;
    this[tableSym] = table;
    this[schemaSym] = schema;
    this[hookSym] = null;
    this[constraintSym] = null;
  }
  get client() {
    return this[clientSym];
  }
  get table() {
    return this[tableSym];
  }
  get schema() {
    return this[schemaSym];
  }
  get sql() {
    const {
      table,
      client,
      schema,
    } = this;
    if (!table || !client || !schema) {
      throw new Error('table client and schema can not be null');
    }
    const sql = new SQL(table, schema, client);
    const constraints = this[constraintSym];
    if (constraints && constraints.length) {
      sql.addConstraint(...constraints);
    }
    return sql;
  }
  set constraint(v) {
    this[constraintSym] = v;
  }
  set hook(v) {
    this[hookSym] = v;
  }
  runHook(op, sql) {
    const hook = this[hookSym];
    if (!hook) {
      return;
    }
    _.forEach(hook[op], fn => fn(sql, this.table));
  }
  /**
   * create table
   *
   * @param {Object} schema The schema of table
   * @returns {Promise}
   * @memberof Table
   */
  create() {
    const sql = this.sql;
    sql.mode = 'create';
    return sql;
  }
  /**
   * insert data to table
   *
   * @param {Object|Array} data The data to insert
   * @param {String} fields The fields to return
   * @returns {Promise}
   * @memberof Table
   */
  insert(data, fields) {
    const insertOne = !_.isArray(data);
    const inserts = insertOne ? [data] : data;

    const sql = this.sql;
    sql.mode = 'insert';
    sql.insert(...inserts);
    if (fields) {
      sql.addField(...fields.split(' '));
    }
    const originalThen = sql.then.bind(sql);
    sql.then = (resolve, reject) => {
      this.runHook('insert', sql);
      return originalThen((result) => {
        if (sql.rawMode) {
          return resolve(result);
        }
        const items = _.map(_.get(result, 'rows'), item => _.extend({}, item));
        if (insertOne) {
          return resolve(items[0] || null);
        }
        return resolve(items);
      }).catch(reject);
    };
    return sql;
  }
  /**
   * 查询一条记录
   *
   * @param {any} conditions
   * @param {any} args
   * @returns
   * @memberof Table
   */
  findOne(conditions, ...args) {
    const sql = this.find(conditions, ...args);
    sql.limit(1);
    const originalThen = sql.then.bind(sql);
    sql.then = (resolve, reject) => {
      this.runHook('findOne', sql);
      return originalThen((result) => {
        if (sql.rawMode) {
          return resolve(result);
        }
        const item = result[0] || null;
        return resolve(item);
      }).catch(reject);
    };
    return sql;
  }
  /**
   * 查询符合条件的所有记录
   *
   * @param {any} conditions
   * @param {any} args
   * @returns
   * @memberof Table
   */
  find(conditions, ...args) {
    const sql = this.sql;
    sql.where(conditions);
    const fields = getParam(args, _.isString);
    if (fields) {
      sql.addField(...fields.split(' '));
    }
    const originalThen = sql.then.bind(sql);
    sql.then = (resolve, reject) => {
      this.runHook('find', sql);
      return originalThen((result) => {
        if (sql.rawMode) {
          return resolve(result);
        }
        const rows = _.get(result, 'rows');
        if (!rows || !rows.length) {
          return resolve([]);
        }
        return resolve(_.map(rows, item => _.extend({}, item)));
      }).catch(reject);
    };
    return sql;
  }
  /**
   * 汇总符合条件的记录
   *
   * @param {any} conditions
   * @memberof Table
   */
  count(conditions) {
    const sql = this.sql;
    if (conditions) {
      sql.where(conditions);
    }
    sql.mode = 'count';
    const originalThen = sql.then.bind(sql);
    sql.then = (resolve, reject) => {
      this.runHook('count', sql);
      return originalThen((result) => {
        if (sql.rawMode) {
          return resolve(result);
        }
        const count = Number.parseInt(_.get(result, 'rows[0].count'), 10);
        if (_.isNaN(count)) {
          return resolve(0);
        }
        return resolve(count);
      }).catch(reject);
    };
    return sql;
  }
  /**
   * 更新符合条件的记录
   *
   * @param {any} conditions
   * @param {any} data
   * @returns
   * @memberof Table
   */
  update(conditions, data) {
    const sql = this.sql;
    sql.mode = 'update';
    sql.where(conditions);
    sql.update(data);
    const originalThen = sql.then.bind(sql);
    sql.then = (resolve, reject) => {
      this.runHook('update', sql);
      return originalThen((result) => {
        if (sql.rawMode) {
          return resolve(result);
        }
        const count = _.get(result, 'rowCount');
        return resolve(count);
      }).catch(reject);
    };
    return sql;
  }
  /**
   * 通过ID更新
   *
   * @param {any} id
   * @param {any} data
   * @returns
   * @memberof Table
   */
  findByIdAndUpdate(id, data) {
    return this.update({
      id,
    }, data);
  }
  /**
   * 创建索引
   *
   * @param {any} indexes
   * @returns
   * @memberof Table
   */
  createIndex(indexes) {
    const sql = this.sql;
    sql.mode = 'index';
    sql.addIndex(...indexes);
    return sql;
  }
}

module.exports = Table;
