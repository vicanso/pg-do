# pg-do


## API

### Client

```js
const PG = require('pg-do');
const client = new PG('postgres://user:password@host:port/database');
client.createTable('users', {
  account: 'varchar(80) unique',
  date: 'date',
  email: 'varchar(160)',
  age: 'smallint',
}).catch(console.error);
```