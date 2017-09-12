const assert = require('assert');
const PG = require('..');

describe('PG', () => {
  const table = 'users';
  const client = new PG('postgres://test:123456@127.0.0.1:5432/test?max=20&idleTimeout=5000');
  const userSchema = {
    account: 'varchar(80) unique',
    createdAt: 'varchar(24) NOT NULL',
    email: 'varchar(160)',
    age: 'smallint CHECK (age > 0)',
  };
  client.addSchema('users', userSchema);
  const users = client.getTable(table);

  it('check options', () => {
    const {
      host,
      port,
      user,
      password,
      database,
      max,
      idleTimeoutMillis,
    } = client.pool.options;
    assert.equal(host, '127.0.0.1');
    assert.equal(port, '5432');
    assert.equal(user, 'test');
    assert.equal(password, '123456');
    assert.equal(database, 'test');
    assert.equal(max, 20);
    assert.equal(idleTimeoutMillis, 5000);
  });

  it('drop table', (done) => {
    client.query(`DROP TABLE ${table}`).then(() => {
      done();
    }).catch(() => {
      done();
    });
  });

  it('create table', () => users.create());

  it('insert data', async () => {
    const user = await users.insert({
      account: 'vicanso',
      email: 'vicansocanbico@gmail.com',
      age: 30,
    }, 'account createdAt');
    assert.equal(user.account, 'vicanso');
    assert.equal(user.createdAt.length, 24);
    const found = await users.findOne({
      account: 'vicanso',
    }, 'account email age createdAt');
    assert.equal(found.account, 'vicanso');
    assert.equal(found.email, 'vicansocanbico@gmail.com');
    assert.equal(found.age, 30);
    assert.equal(found.createdAt.length, 24);
  });

  it('insert multi data', async () => {
    const arr = await users.insert([
      {
        account: 'a',
      },
      {
        account: 'b',
      },
    ], 'account');
    assert.equal(arr.length, 2);
    const result = await users.find()
      .where('account', ['a', 'b'])
      .addField('account', 'createdAt');
    assert.equal(result[1].account, 'b');
    assert.equal(result[1].createdAt.length, 24);
  });

  it('count', async () => {
    const total = await users.count();
    assert.equal(total, 3);
    const one = await users.count({
      account: 'vicanso',
    });
    assert.equal(one, 1);
    const zero = await users.count({
      account: 'reoojfeoajoe',
    });
    assert.equal(zero, 0);
  });

  it('find return raw data', async () => {
    const result = await users.find({}).raw(true);
    assert.equal(result.command, 'SELECT');
    assert.equal(result.rowCount, 3);
  });

  it('find by order', async () => {
    let result = await users.find({})
      .addOrderBy('account');
    assert.equal(result.length, 3);
    assert.equal(result[0].account, 'a');

    result = await users.find({})
      .addOrderBy('-account');
    assert.equal(result.length, 3);
    assert.equal(result[2].account, 'a');
  });

  it('update', async () => {
    const count = await users.update({}, {
      age: 10,
    });
    assert.equal(count, 3);
    const result = await users.find({
      age: 30,
    });
    assert.equal(result.length, 0);
    const one = await users.update({
      account: 'vicanso',
    }, {
      age: 30,
    });
    assert.equal(one, 1);
  });

  it('transaction rollback', async () => {
    const transaction = await client.transaction();
    const transactionUsers = transaction.getTable('users');
    try {
      await transaction.begin();
      await transactionUsers.update({
        account: 'a',
      }, {
        age: 22,
      });
      throw new Error('custom error');
    } catch (err) {
      await transaction.rollback();
      if (err.message !== 'custom error') {
        throw err;
      }
      const result = await users.findOne({
        account: 'a',
      }, 'account age');
      assert(result.account, 'a');
      assert(result.age, 10);
    } finally {
      transaction.release();
    }
  });

  it('transaction commit', async () => {
    const transaction = await client.transaction();
    const transactionUsers = transaction.getTable('users');
    try {
      await transaction.begin();
      await transactionUsers.update({
        account: 'a',
      }, {
        age: 22,
      });
      await transactionUsers.update({
        account: 'b',
        age: 10,
      }, {
        age: 22,
      });
      // the update will be block
      users.update({
        account: 'b',
      }, {
        age: 20,
      }).then(() => console.info('update account "b" to age 20 success'));
      await new Promise(resolve => setTimeout(resolve, 1000));

      await transaction.commit();
    } catch (err) {
      await transaction.rollback();
      throw err;
    } finally {
      const result = await users.find({
        age: 22,
      });
      assert.equal(result.length, 2);
      assert.equal(result[0].account, 'a');
      assert.equal(result[1].account, 'b');
      transaction.release();
    }
  });
});
