const _ = require('lodash');

const debug = require('./debug');

const optionsSym = Symbol('options');
const tableSym = Symbol('table');
const schemaSym = Symbol('schema');
const clientSym = Symbol('client');

function convertColumnName(name) {
  return `"${name}"`;
}

function convertColumnsName(arr, schema) {
  return _.map(arr, (item) => {
    if (schema && schema[item]) {
      return convertColumnName(item);
    }
    return item;
  });
}


/**
 * 获取要插入数据参数
 *
 * @param {any} data
 * @param {any} keys
 * @param {number} [index=0]
 * @returns
 */
function getInsertValues(insertDatas, keys, index = 0) {
  let paramIndex = index;
  const params = [];
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


/**
 * 获取 where 查询条件
 *
 * @param {any} conditions
 * @param {any} rawConditions
 * @param {number} [index=0]
 * @returns
 */
function getWhere(conditions, rawConditions, index = 0) {
  if ((!conditions || _.isEmpty(conditions)) && !rawConditions.length) {
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
      arr.push(`${convertColumnName(k)} in (${indexArr.join(',')})`);
    } else {
      paramIndex += 1;
      arr.push(`${convertColumnName(k)} = $${paramIndex}`);
      params.push(v);
    }
  });
  arr.push(...rawConditions);
  return {
    text: `WHERE ${arr.join(' AND ')}`,
    params,
    index: paramIndex,
  };
}

function uniqueJoin(dest, source) {
  _.forEach(source, (item) => {
    if (!_.includes(dest, item)) {
      dest.push(item);
    }
  });
}

class SQL {
  constructor(table, schema, client) {
    if (!table || !schema) {
      throw new Error('table and schema can not be null');
    }
    this[optionsSym] = {
      // 保存查询的条件
      conditions: {},
      rawConditions: [],
      // 需要返回的字段
      fields: [],
      // 查询限制条件
      limit: 0,
      // 查询时跳过多少条
      offset: 0,
      // 插入的数据
      inserts: [],
      // count的方式
      count: 'count(*)',
      // 更新的数据
      updates: {},
      // 排序字段
      orders: [],
      // groupby字段
      groups: [],
      havings: [],
      constraints: [],
    };
    this[clientSym] = client;
    this[tableSym] = table;
    this[schemaSym] = schema;
    this.mode = 'select';
    // 是否返回原始数据
    this.rawMode = false;
  }
  set client(v) {
    this[clientSym] = v;
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
  get options() {
    return this[optionsSym];
  }
  addConstraint(...args) {
    const constraints = this.options.constraints;
    uniqueJoin(constraints, args);
    return this;
  }
  /**
   * 设置是否使用raw（返回原始数据)）
   *
   * @param {Boolean} raw
   * @returns
   * @memberof SQL
   */
  raw(raw) {
    this.rawMode = raw;
    return this;
  }
  /**
   * 增加查询条件
   *
   * @param {any} key
   * @param {any} value
   * @returns
   * @memberof SQL
   */
  where(key, value) {
    const {
      conditions,
      rawConditions,
    } = this.options;
    if (_.isObject(key)) {
      _.extend(conditions, key);
    } else if (key && value) {
      conditions[key] = value;
    } else if (_.isString(key)) {
      rawConditions.push(key);
    }
    return this;
  }
  /**
   * 增加要返回的字段
   *
   * @param {any} args
   * @returns
   * @memberof SQL
   */
  addField(...args) {
    const fields = this.options.fields;
    uniqueJoin(fields, args);
    return this;
  }
  /**
   * 增加排序字段
   *
   * @param {any} args
   * @memberof SQL
   */
  addOrderBy(...args) {
    const orders = this.options.orders;
    uniqueJoin(orders, args);
    return this;
  }
  /**
   * 增加group by字段
   *
   * @param {any} args
   * @memberof SQL
   */
  addGroupBy(...args) {
    const groups = this.options.groups;
    uniqueJoin(groups, args);
    return this;
  }
  /**
   * 增加having条件
   *
   * @param {any} args
   * @returns
   * @memberof SQL
   */
  addHaving(...args) {
    const havings = this.options.havings;
    uniqueJoin(havings, args);
    return this;
  }
  /**
   * 设定limit的值
   *
   * @param {any} count
   * @returns
   * @memberof SQL
   */
  limit(count) {
    const value = Number.parseInt(count, 10);
    if (!_.isNaN(value)) {
      this[optionsSym].limit = value;
    }
    return this;
  }
  /**
   * 设定offset的值
   *
   * @param {any} count
   * @memberof SQL
   */
  offset(count) {
    const value = Number.parseInt(count, 10);
    if (!_.isNaN(value)) {
      this[optionsSym].offset = value;
    }
    return this;
  }
  /**
   * 插入数据
   *
   * @param {any} args
   * @memberof SQL
   */
  insert(...args) {
    const {
      inserts,
    } = this[optionsSym];
    inserts.push(...args);
  }
  /**
   * 需要更新的数据
   *
   * @param {any} key
   * @param {any} value
   * @memberof SQL
   */
  update(key, value) {
    if (!key) {
      return this;
    }
    const updates = this.options.updates;
    if (_.isObject(key)) {
      _.extend(updates, key);
    } else if (key && value) {
      updates[key] = value;
    }
    return this;
  }
  /**
   * 获取 select 的 sql
   *
   * @returns
   * @memberof SQL
   */
  getSelect() {
    const {
      table,
      options,
      schema,
    } = this;
    const {
      conditions,
      rawConditions,
      fields,
      limit,
      offset,
      orders,
      groups,
      havings,
    } = options;
    const fieldsStr = convertColumnsName(fields, schema).join(',') || '*';

    let sql = `SELECT ${fieldsStr} FROM ${table}`;
    let params = null;
    const result = getWhere(conditions, rawConditions);
    if (result) {
      sql += ` ${result.text}`;
      params = result.params;
    }
    if (orders && orders.length) {
      // 如果 order 前面有 - ，表示 desc
      const orderDesc = _.map(orders, (order) => {
        let sortOrder = '';
        let name = order;
        if (name.charAt(0) === '-') {
          sortOrder = ' DESC';
          name = name.substring(1);
        }
        if (schema[name]) {
          name = convertColumnName(name);
        }
        return `${name}${sortOrder}`;
      }).join(',');
      sql += ` ORDER BY ${orderDesc}`;
    }
    if (groups && groups.length) {
      const groupByDesc = _.map(groups, (name) => {
        if (schema[name]) {
          return convertColumnName(name);
        }
        return name;
      }).join(',');
      sql += ` GROUP BY ${groupByDesc}`;
      if (havings && havings.length) {
        sql += ` HAVING ${havings.join(' AND ')}`;
      }
    }
    if (limit) {
      sql += ` LIMIT ${limit}`;
    }
    if (offset) {
      sql += ` OFFSET ${offset}`;
    }
    const sqlParams = [sql];
    if (params) {
      sqlParams.push(params);
    }
    return sqlParams;
  }
  /**
   * 获取插入数据的参数列表
   *
   * @returns
   * @memberof SQL
   */
  getInsert() {
    const {
      table,
      options,
    } = this;
    const {
      inserts,
      fields,
    } = options;
    const keys = _.keys(_.first(inserts));
    const {
      values,
      params,
    } = getInsertValues(inserts, keys);
    const insertKeysStr = _.map(keys, convertColumnName).join(',');
    let sql = `INSERT INTO ${table} (${insertKeysStr}) VALUES ${values.join(',')}`;
    if (fields && fields.length) {
      const fieldsStr = _.map(fields, convertColumnName).join(',');
      sql += ` RETURNING ${fieldsStr}`;
    }
    return [
      sql,
      params,
    ];
  }
  /**
   * 获取 create table 的参数列表
   *
   * @returns
   * @memberof SQL
   */
  getCreate() {
    const {
      table,
      schema,
      options,
    } = this;
    const constraints = options.constraints;
    const arr = _.map(schema, (v, k) => `${convertColumnName(k)} ${v}`);
    if (constraints && constraints.length) {
      arr.push(...constraints);
    }
    return [
      `CREATE TABLE ${table} (${arr.join(',')})`,
    ];
  }
  /**
   * 获取 count 的参数列表
   *
   * @memberof SQL
   */
  getCount() {
    const {
      options,
      table,
    } = this;
    const {
      count,
      conditions,
      rawConditions,
    } = options;
    let sql = `SELECT ${count} FROM ${table}`;
    let params = null;
    const result = getWhere(conditions, rawConditions);
    if (result) {
      sql += ` ${result.text}`;
      params = result.params;
    }
    const sqlParams = [sql];
    if (params) {
      sqlParams.push(params);
    }
    return sqlParams;
  }
  /**
   * 获取 update 的参数列表
   *
   * @returns
   * @memberof SQL
   */
  getUpdate() {
    const {
      options,
      table,
    } = this;
    const {
      updates,
      conditions,
      rawConditions,
    } = options;
    let paramIndex = 0;
    const params = [];
    const updateDataDesc = _.map(updates, (v, k) => {
      params.push(v);
      paramIndex += 1;
      return `${convertColumnName(k)} = $${paramIndex}`;
    }).join(',');
    let sql = `UPDATE ${table} SET ${updateDataDesc}`;
    const whereParams = getWhere(conditions, rawConditions, paramIndex);
    if (whereParams) {
      sql += ` ${whereParams.text}`;
      params.push(...whereParams.params);
    }
    return [
      sql,
      params,
    ];
  }
  /**
   * promise thenable
   *
   * @param {any} resolve 
   * @param {any} reject 
   * @returns
   * @memberof SQL
   */
  then(resolve, reject) {
    if (!this.fullfilledPromise) {
      this.fullfilledPromise = new Promise((innerResolve, innerReject) => {
        const {
          client,
          mode,
        } = this;
        let params;
        switch (mode) {
          case 'create':
            params = this.getCreate();
            break;
          case 'insert':
            params = this.getInsert();
            break;
          case 'count':
            params = this.getCount();
            break;
          case 'update':
            params = this.getUpdate();
            break;
          default:
            params = this.getSelect();
            break;
        }
        debug('mode:%s sql:%j', mode, params);
        if (!params || !params.length) {
          innerResolve();
        }
        client.query(...params).then(innerResolve, innerReject);
      });
    }
    return this.fullfilledPromise.then(resolve, reject);
  }
  /**
   * promise catch
   *
   * @param {any} cb
   * @returns
   * @memberof SQL
   */
  catch(cb) {
    return this.then(undefined, cb);
  }
}

module.exports = SQL;
