const _ = require('lodash');

const debug = require('./debug');

const clientSym = Symbol('client');
const tableSym = Symbol('table');


/**
 * 判断是否post gres 的函数
 *
 * @param {any} str
 */
function isOpFunction(str) {
  const reg = /\S+\(\S+\)/;
  return reg.test(str);
}

/**
 * 由于postgres 默认大小写不区分，因此需要将fields字段加"
 *
 * @param {any} arr
 */
function convertFields(arr) {
  return _.map(arr, (item) => {
    if (item === '*' || isOpFunction(item)) {
      return item;
    }
    if (_.kebabCase(item) === item) {
      return item;
    }
    return `"${item}"`;
  });
}

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

/**
 * 获取 where 查询条件
 *
 * @param {any} conditions
 * @param {number} [index=0]
 * @returns
 */
function getWhere(conditions, index = 0) {
  if (!conditions || _.isEmpty(conditions)) {
    return null;
  }
  let paramIndex = index;
  const params = [];
  const arr = [];
  _.forEach(conditions, (v, k) => {
    // 如果是数组，表示in查询
    if (_.isArray(v)) {
      const indexArr = _.map(v, (tmp) => {
        params.push(tmp);
        paramIndex += 1;
        return `$${paramIndex}`;
      });
      arr.push(`"${k}" in (${indexArr.join(',')})`);
    } else {
      paramIndex += 1;
      arr.push(`"${k}" = $${paramIndex}`);
      params.push(v);
    }
  });
  return {
    text: `where ${arr.join(' and ')}`,
    params,
    index: paramIndex,
  };
}

/**
 * 获取查询的的参数
 *
 * @param {any} table
 * @param {any} conditions
 * @param {any} fields
 * @param {any} options
 * @returns
 */
function getQueryParams(table, conditions, fields, options) {
  let sql = `SELECT ${convertFields(fields.split(' ')).join(',')} 
  FROM ${table}`;
  let params = null;
  const result = getWhere(conditions);
  if (result) {
    sql += ` ${result.text}`;
    params = result.params;
  }
  if (options && options.limit) {
    sql += ` limit ${options.limit}`;
  }
  return {
    text: sql,
    values: params,
  };
}

/**
 * 获取要插入数据参数
 *
 * @param {any} data
 * @param {any} keys
 * @param {number} [index=0]
 * @returns
 */
function getInsertValues(data, keys, index = 0) {
  let paramIndex = index;
  const params = [];
  const insertDatas = _.isArray(data) ? data : [data];
  const values = _.map(insertDatas, (insertData) => {
    const items = _.map(keys, (key) => {
      const v = insertData[key];
      params.push(v);
      paramIndex += 1;
      return `$${paramIndex}`;
    });
    return `(${items.join(',')})`;
  });
  return {
    values,
    params,
    index: paramIndex,
  };
}


class Table {
  constructor(client, table) {
    if (!client || !table) {
      throw new Error('client and table can not be null');
    }
    this[clientSym] = client;
    this[tableSym] = table;
  }
  get client() {
    return this[clientSym];
  }
  get table() {
    return this[tableSym];
  }
  /**
   * create table
   *
   * @param {Object} schema The schema of table
   * @returns {Promise}
   * @memberof Table
   */
  create(schema) {
    const {
      table,
      client,
    } = this;
    if (!schema) {
      throw new Error('The table schema can not be null');
    }
    const arr = _.map(schema, (v, k) => `"${k}" ${v}`);
    const sql = `CREATE TABLE ${table} (
      ${arr.join(',')}
    )`;
    return client.query(sql);
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
    const {
      table,
      client,
    } = this;
    const insertOne = !_.isArray(data);
    const insertDatas = _.map(
      insertOne ? [data] : data,
      item => _.extend({
        createdAt: new Date().toISOString(),
      }, item));
    const keys = _.keys(insertDatas[0]);
    const {
      values,
      params,
    } = getInsertValues(insertDatas, keys);
    let sql = `INSERT INTO ${table} (${convertFields(keys).join(',')}) 
    VALUES ${values.join(',')}`;
    if (fields) {
      sql += ` RETURNING ${convertFields(fields.split(' ')).join(',')}`;
    }
    debug('insert sql:%s, params:%j', sql, params);
    return client.query(`${sql};`, params).then((result) => {
      const items = _.map(_.get(result, 'rows'), item => _.extend({}, item));
      if (insertOne) {
        return items[0] || null;
      }
      return items;
    });
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
    const {
      table,
      client,
    } = this;
    const fields = getParam(args, _.isString, '*');
    const params = getQueryParams(table, conditions, fields, {
      limit: 1,
    });
    debug('findOne fields:%s params:%j', fields, params);
    return client.query(params).then((result) => {
      const item = _.get(result, 'rows[0]');
      if (!item) {
        return null;
      }
      return _.extend({}, item);
    });
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
    const {
      table,
      client,
    } = this;
    const fields = getParam(args, _.isString, '*');
    const params = getQueryParams(table, conditions, fields);
    debug('find fields:%s params:%j', fields, params);
    return client.query(params).then((result) => {
      const rows = _.get(result, 'rows');
      if (!rows || !rows.length) {
        return [];
      }
      return _.map(rows, item => _.extend({}, item));
    });
  }
  /**
   * 汇总符合条件的记录
   *
   * @param {any} conditions
   * @memberof Table
   */
  count(conditions) {
    const {
      table,
      client,
    } = this;
    const params = getQueryParams(table, conditions, 'count(*)');
    debug('count params:%j', params);
    return client.query(params).then((result) => {
      const count = Number.parseInt(_.get(result, 'rows[0].count'), 10);
      if (_.isNaN(count)) {
        return 0;
      }
      return count;
    });
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
    const {
      table,
      client,
    } = this;
    const params = [];
    let paramIndex = 0;
    const sets = _.map(data, (v, k) => {
      params.push(v);
      paramIndex += 1;
      return `${k} = $${paramIndex}`;
    });
    let sql = `UPDATE ${table} SET ${sets.join(',')}`;
    const where = getWhere(conditions, paramIndex);
    if (where) {
      sql += ` ${where.text}`;
      params.push(...where.params);
    }
    debug('update sql:%s params:%j', sql, params);
    return client.query(sql, params).then((result) => {
      const count = _.get(result, 'rowCount');
      return count;
    });
  }
}

module.exports = Table;
