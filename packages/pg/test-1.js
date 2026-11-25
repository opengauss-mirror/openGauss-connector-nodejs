/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

const { Pool, Client } = require('pg')
// const isMd5 = false
// const client = true

const ogConfig = {
  user: 'sm3test',
  database: 'postgres',
  password: 'Aa123456',
  host: '10.211.55.22',
  port: 5432,
}
const pool = new Pool(ogConfig)
const client = new Client(ogConfig)

let now = Date.now();
client.connect();

client.query(`CREATE TABLE test
(
    c_customer_sk             integer
);`)
client.query('insert into test values(1111)', (err, res) => {
  console.log(res)
})

client.query('update test set c_customer_sk = 2222', (err, res) => {
  console.log(res)
})
client.query('SELECT * FROM test;', (err, res) => {
  console.log(res)
})
client.query('delete from test where c_customer_sk = 2222', (err, res) => {
  console.log(res)
})
client.query('SELECT * FROM test;', (err, res) => {
  console.log(res)
})
client.query('DROP TABLE test;')
.then(() => {
	client.end()
  console.log("\n\x1b[32mAll queries & client ended process complete in \x1b[1m" + (Date.now() - now) + " ms");
})


function rollback(client) {
  //terminating a client connection will
  //automatically rollback any uncommitted transactions
  //so while it's not technically mandatory to call
  //ROLLBACK it is cleaner and more correct
  return client
    .query('ROLLBACK')
    .then(() => client.end())
    .catch(() => client.end())
}
