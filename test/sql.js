const assert = require('assert');
const SQL = require('../lib/sql');

describe('SQL', () => {
  const table = 'users';
  const userSchema = {
    account: 'varchar(80) unique',
    createdAt: 'varchar(24) NOT NULL',
    email: 'varchar(160)',
    age: 'smallint CHECK (age > 0)',
  };
  it('create table', () => {
    const sql = new SQL(table, userSchema);
    assert.equal(sql.getCreate()[0], 'CREATE TABLE users ("account" varchar(80) unique,"createdAt" varchar(24) NOT NULL,"email" varchar(160),"age" smallint CHECK (age > 0))');
  });
});
