# pg-do

[![Build Status](https://travis-ci.org/vicanso/pg-do.svg?branch=master)](https://travis-ci.org/vicanso/pg-do)
[![Coverage Status](https://img.shields.io/coveralls/vicanso/pg-do/master.svg?style=flat)](https://coveralls.io/r/vicanso/pg-do?branch=master)
[![npm](http://img.shields.io/npm/v/pg-do.svg?style=flat-square)](https://www.npmjs.org/package/pg-do)
[![Github Releases](https://img.shields.io/npm/dm/pg-do.svg?style=flat-square)](https://github.com/vicanso/pg-do)

## API

### Client

```js
const PG = require('pg-do');
const client = new PG('postgres://test:123456@127.0.0.1:5432/test?max=20&idleTimeout=5000');
client.on('acquire', () => console.info('acquire'));
client.on('remove', () => console.info('remove'));
client.on('error', () => console.info('error'));
client.on('connect', () => console.info('connect'));
```

### getTable

Get the table instance

```js
const userSchema = {
  account: 'varchar(80) unique',
  createdAt: 'varchar(24) NOT NULL',
  email: 'varchar(160)',
  age: 'smallint CHECK (age > 0)',
};
client.addSchema('users', userSchema);
const users = client.getTable('users');
```

#### Table.insert

Insert the data to table


```js
const user = await users.insert({
  account: 'vicanso',
  email: 'vicansocanbico@gmail.com',
  age: 30,
}, 'account createdAt');


const arr = await users.insert([
  {
    account: 'a',
  },
  {
    account: 'b',
  },
], 'account');
```

#### Table.count

Count the data

```js
const count = await users.count({
  account: 'vicanso',
});

const total = await users.count({});
```

#### Table.findOne

Find one record

```js
const user = await users.findOne({});

const user = await user.findOne({
  age: 20,
});
```

#### Table.find

Find records

```js
const data = await users.find({});
```

## LICENSE

ISC